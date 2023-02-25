using Spectre.Console;
using Swarmr.Base.Api;
using Swarmr.Base.Tasks;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public class Swarm : ISwarm
{
    public static readonly TimeSpan HOUSEKEEPING_INTERVAL = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan NODE_TIMEOUT = TimeSpan.FromSeconds(15);
    public static readonly TimeSpan HANDLER_WARN_DURATION = TimeSpan.FromSeconds(0.1);
    public const string DEFAULT_WORKDIR = ".swarmr";

    private readonly SwarmTaskQueue _localTaskQueue = new();

    private FileInfo SwarmSecretsFile { get; }
    internal Task<SwarmSecrets> LoadSwarmSecretsAsync() => SwarmSecrets.CreateAsync(SwarmSecretsFile);
   
    [JsonIgnore]
    public DirectoryInfo Workdir { get; }
    public string SelfId { get; }
    public string? PrimaryId { get; private set; }
    public LocalSwarmFiles LocalSwarmFiles { get; }
    public bool Verbose { get; }

    public bool IAmPrimary => SelfId != null && SelfId == PrimaryId;

    /// <summary>
    /// All nodes (including self).
    /// </summary>
    public IReadOnlyList<Node> Nodes
    {
        get
        {
            lock (_nodes) return _nodes.Values.ToArray();
        }
    }

    /// <summary>
    /// All nodes (except self).
    /// </summary>
    [JsonIgnore]
    public IReadOnlyList<Node> Others => Nodes.Where(n => n.Id != SelfId).ToArray();

    [JsonIgnore]
    public Node Self { get { lock (_nodes) return _nodes[SelfId]; } }

    /// <summary>
    /// Returns primary node.
    /// Throws if no primary.
    /// </summary>
    [JsonIgnore]
    public Node Primary
    {
        get
        {
            lock (_nodes)
            {
                if (PrimaryId != null && _nodes.TryGetValue(PrimaryId, out var primary))
                {
                    return primary;
                }
                else
                {
                    throw new Exception("No primary node. Error c6034aa5-cff3-483c-8d5c-5a804746231f.");
                }
            }
        }
    }

    public bool TryGetPrimaryNode([NotNullWhen(true)] out Node? primary)
    {
        var primaryId = PrimaryId;
        if (primaryId == null) { primary = null; return false; }
        lock (_nodes) return _nodes.TryGetValue(primaryId, out primary);
    }

    public void PrintNice()
    {
        var table = new Table()
            .AddColumns("Swarm", "Id", "Hostname", "Port", "Status", "Mode", "LastSeen")
            ;
        foreach (var n in Nodes.OrderBy(x => x.Id))
        {
            var who = (n.Id == PrimaryId, n.Id == Self.Id) switch
            {
                (false, false) => "",
                (false, true ) => "[green]SELF[/]",
                (true , false) => "PRIMARY",
                (true , true ) => "[green]I AM PRIMARY[/]"
            };
            table.AddRow(
                new Markup(who),
                new Markup((n.Id == SelfId) ? $"[green]{n.Id}[/]" : n.Id),
                new Markup(n.Hostname),
                new Markup(n.Port.ToString()),
                new Markup(n.Status.ToString().ToLower()),
                new Markup(n.Type.ToString().ToLower()),
                new Markup($"{n.Ago.TotalSeconds:0.0} [[s]]").Justify(Justify.Right)
                );
        }

        var rows = new Rows(
            new Markup($"\nSelfId: {SelfId}{(IAmPrimary ? "\n[lime]I AM PRIMARY[/]" : "")}"),
            table
            );

        var swarmPanel = new Panel(rows).Header("Swarm");

        AnsiConsole.Write(table);
    }

    public async void EnqueueDelayedAsync(ISwarmTask task, TimeSpan delay)
    {
        AnsiConsole.MarkupLine($"[red][[DEBUG]] EnqueueDelayedAsync {delay}[/]");
        try
        {
            var sw = Stopwatch.StartNew();
            await Task.Delay(delay);
            sw.Stop(); if (Math.Abs(sw.Elapsed.TotalSeconds - delay.TotalSeconds) > 0.1) Debugger.Break();
            await _localTaskQueue.Enqueue(task);
        }
        catch (Exception e)
        {
            AnsiConsole.MarkupLine($"[red][[ERROR]]{e.ToString().EscapeMarkup()}[/]");
        }
    }

    #region Connect (=construct)

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="url">
    /// URL of an active swarm node. 
    /// Joins this node's swarm.
    /// If null, then swarm of one will be created.</param>
    /// <param name="self"></param>
    public static async Task<Swarm> ConnectAsync(
        string? url,
        Node self,
        DirectoryInfo workdir,
        bool verbose,
        CancellationToken ct = default
        )
    {
        Swarm? swarm = null;
        if (url != null)
        {
            // clone the swarm from the node at given URL
            if (verbose) AnsiConsole.WriteLine($"connecting to {url} ...");
            var swarmNode = new NodeHttpClient(url, self);

            // swarm = await swarmNode.JoinSwarmAsync(self, workdir: workdir, verbose: verbose);
            var r = await swarmNode.SendAsync<JoinSwarmRequest, JoinSwarmResponse>(new(Node: self));
            swarm = r.Swarm.ToSwarm(self: self, workdir: workdir, verbose: verbose);
            await swarm.UpdateSecretsAsync(r.Swarm.Secrets);

            if (verbose) AnsiConsole.WriteLine($"connecting to {url} ... done");
        }
        else
        {
            // create a new swarm of one, with myself as primary
            swarm ??= new Swarm(self, workdir: workdir, verbose: verbose);
        }

        // init/sync local files
        //if (!swarm.IAmPrimary) await swarm.Primary.Client.UpdateNodeAsync(swarm.Self);
        foreach (var n in swarm.Others)
        {
            var task = SyncSwarmFilesTask.Create(n);
            await swarm._localTaskQueue.Enqueue(task);
        }

        // start housekeeping (background)
        swarm.StartHouseKeepingAsync(ct);

        // start swarm task queue processing (background)
        swarm.StartSwarmTasksProcessingAsync(ct);

        return swarm;
    }

    public static async Task<Swarm> ConnectAsync(
        NodeType type,
        string? customRemoteHost,
        int? listenPort,
        DirectoryInfo? customWorkDir,
        bool verbose
        )
    {
        // (0) Arguments.
        var (remoteHost, remotePort) = SwarmUtils.ParseHost(customRemoteHost);
        if (customWorkDir == null)
        {
            var config = await LocalConfig.LoadAsync();
            if (config.Workdir != null)
            {
                customWorkDir = new DirectoryInfo(config.Workdir);
            }
            else
            {
                throw new Exception("No work dir specified. Error 440022ad-bc87-4256-bbd5-c6430f9bd386.");
            }
        }

        // (1) Construct the 'remoteUrl', which is
        // - the URL of a running node, which we will use to join the swarm.
        // - or null, which will create a new swarm (with ourselve as only node).
        ProbeResult? probe = null;
        if (remotePort == null)
        {
            // We don't care about a specific port!

            // Let's try to auto-detect a live port.
            // If no specific host is specified either,
            // then we try localhost by default.
            var probeHost = remoteHost ?? "localhost";

            probe = await SwarmUtils.ProbeHostAsync(probeHost);
            if (probe.TryGetLivePort(out remotePort)) remoteHost = probe.Hostname;
        }

        var remoteUrl = (remoteHost, remotePort) switch
        {
            (null, null) => null,
            (string host, int port) => $"http://{host}:{port}",
            _ => throw new Exception($"Error f61e449b-dac9-40b7-b55a-cd7cd235e758.")
        };

        // (2) Determine the port, which our node will listen on.
        if (!listenPort.HasValue)
        {
            // No specific port has been specified by the user.
            // Let's choose an available port from the default port range.
            if (probe?.Hostname != "localhost")
                probe = await SwarmUtils.ProbeHostAsync("localhost");

            if (!probe.TryGetFreePort(out listenPort))
            {
                throw new Exception("No port available.");
            }
        }

        // (3) Create our node.
        var hostname = Environment.MachineName.ToLowerInvariant();
        var myself = new Node(
            Id: Guid.NewGuid().ToString(),
            Created: DateTimeOffset.UtcNow,
            LastSeen: DateTimeOffset.UtcNow,
            Hostname: hostname,
            Port: listenPort.Value,
            Files: ImmutableDictionary<string, SwarmFile>.Empty,
            Status: NodeStatus.Idle,
            Type: type
            );

        // (4) Connect to the swarm.
        var swarm = await ConnectAsync(
            url: remoteUrl,
            self: myself,
            workdir: customWorkDir,
            verbose: verbose
            );

        return swarm;
    }

    #endregion

    #region ISwarm message handlers (will be auto-detected)

    // message handlers should execute very fast
    // (if necessary enqueue a swarm task and send asynchronous reply)

    public async Task<JoinSwarmResponse> JoinSwarmAsync(JoinSwarmRequest request)
    {
        if (IAmPrimary)
        {
            // accept
            // (FUTURE: validation, e.g. black/white listing, etc.)
            UpsertNode(request.Node);

            Others
                .Where(n => n.Type != NodeType.Ephemeral)
                .SendEach(n => n.UpdateNodeAsync(request.Node))
                ;
        }
        else
        {
            if (TryGetPrimaryNode(out var primary))
            {
                // forward join request to primary node,
                // which will notify all others if join request is accepted
                await primary.Client.UpdateNodeAsync(request.Node);
            }
            else
            {
                Console.WriteLine($"[JoinSwarmAsync] there is no primary");
                throw new Exception("There is no primary node.");
            }
        }

        if (Verbose) _ = Task.Run(PrintNice);

        return new JoinSwarmResponse(Swarm: await Dto.FromSwarmAsync(this));
    }

    public async Task<LeaveSwarmResponse> LeaveSwarmAsync(LeaveSwarmRequest request)
    {
        if (ExistsNode(request.Node))
        {
            RemoveNode(request.Node.Id);
            if (Verbose) PrintNice();

            if (IAmPrimary)
            {
                // notify all nodes of newly joined node ...
                Others
                    .Except(request.Node)
                    .Where(n => n.Type != NodeType.Ephemeral)
                    .SendEach(n => n.LeaveSwarmAsync(request.Node))
                    ;
            }
            else
            {
                if (TryGetPrimaryNode(out var primary))
                {
                    // notify primary node about candidate
                    await primary.Client.LeaveSwarmAsync(request.Node);
                }
                else
                {
                    Console.WriteLine($"[LeaveSwarmAsync] there is no primary");
                    throw new Exception("There is no primary node.");
                }
            }
        }

        return new();
    }

    public Task<HeartbeatResponse> HeartbeatAsync(HeartbeatRequest request)
    {
        lock (_nodes)
        {
            if (_nodes.TryGetValue(request.NodeId, out var node))
            {
                UpsertNode(node with { LastSeen = DateTimeOffset.UtcNow });
            }
            else
            {
                Console.WriteLine($"[WARNING] heartbeat from unknown node id \"{request.NodeId}\".");
            }
        }
        var response = new HeartbeatResponse();
        return Task.FromResult(response);
    }

    public Task<PingResponse> PingAsync(PingRequest request)
    {
        if (request.Sender != null) UpsertNode(request.Sender);
        var newSelf = UpsertNode(Self with { LastSeen = DateTimeOffset.UtcNow });
        var response = new PingResponse(Node: newSelf);
        return Task.FromResult(response);
    }

    public async Task<UpdateNodeResponse> UpdateNodeAsync(UpdateNodeRequest request)
    {
        // ignore external updates for Self
        //if (request.Node.Id == SelfId) return new();

        var stopwatch = Stopwatch.StartNew();
        
        //void printElapsed(string checkpoint)
        //{
        //    stopwatch.Stop();
        //    var x = stopwatch.Elapsed;
        //    _ = Task.Run(() => AnsiConsole.WriteLine($"[DEBUG][UpdateNodeAsync][{checkpoint}] elapsed {x}"));
        //    stopwatch.Start();
        //}

        if (IAmPrimary)
        {
            // I am the primary node, so it is my duty to
            // inform all others about this new member
            Others
                .Except(request.Node)
                .Where(n => n.Type != NodeType.Ephemeral)
                .SendEach(n => n.UpdateNodeAsync(request.Node))
                ;

            //printElapsed("i am primary");
        }


        UpsertNode(request.Node);

        //printElapsed("upsert node");

        if (Verbose) _ = Task.Run(PrintNice);

        //printElapsed("print nice");

        // sync swarm files
        if (request.Node.Id != Self.Id && request.Node.Hostname != Self.Hostname)
        {
            var task = new SyncSwarmFilesTask(
                Id: Guid.NewGuid().ToString(),
                Other: request.Node
                );
            await _localTaskQueue.Enqueue(task);

            //printElapsed("enqueue SyncSwarmFilesTask");
        }

        //printElapsed("TOTAL");

        return new();
    }

    public Task<RemoveNodesResponse> RemoveNodesAsync(RemoveNodesRequest request)
    {
        foreach (var id in request.NodeIds)
        {
            RemoveNode(id);
        }

        if (request.NodeIds.Count > 0) PrintNice();

        var response = new RemoveNodesResponse();
        return Task.FromResult(response);
    }

    public async Task<GetFailoverNomineeResponse> GetFailoverNomineeAsync(GetFailoverNomineeRequest request)
    {
        // there seems to be an ongoing failover election

        // (0) update my node list with the sender
        UpsertNode(request.Sender);

        //// (1) let's see if I am the primary
        //if (IAmPrimary)
        //{
        //    // mmmh, I am obviously alive - let's nominate myself
        //    return new(Nominee: Self);
        //}

        // (2) choose nominee from my node list
        var nominee = await ChooseNomineeForPrimary();
        return new(Nominee: nominee);
    }

    public async Task<IngestFileResponse> IngestFileAsync(IngestFileRequest request)
    {
        var t = IngestFileTask.Create(request);
        await _localTaskQueue.Enqueue(t);
        return new(Task: t);
    }

    public async Task<SubmitJobResponse> SubmitJobAsync(SubmitJobRequest request)
    {
        if (IAmPrimary)
        {
            var swarmSecrets = await LoadSwarmSecretsAsync();
            var jobConfig = Jobs.Parse(request.Job, swarmSecrets);
            var task = ScheduleJobTask.Create(jobConfig);
            await _localTaskQueue.Enqueue(task);
            return new(JobId: jobConfig.Id);
        }
        else
        {
            throw new Exception(
                $"Only primary node accepts job submissions " +
                $"(SelfId={SelfId}, PrimaryId={PrimaryId}. " +
                $"Error 2a6a3e81-1004-4349-ba3d-bc8419da0350."
                );
        }
    }

    public async Task<RunJobResponse> RunJobAsync(RunJobRequest request)
    {
        if (Self.Type == NodeType.Worker && Self.Status == NodeStatus.Idle)
        {
            await _localTaskQueue.Enqueue(request.Job);
            return new(Accepted: true);
        }
        else
        {
            return new(Accepted: false);
        }
    }

    public async Task<SubmitTaskResponse> SubmitTaskAsync(SubmitTaskRequest request)
    {
        var t = SwarmTask.Deserialize(request.Task);
        await t.RunAsync(context: this);
        return new();
    }

    public async Task<SetSecretResponse> SetSecretAsync(SetSecretRequest request) 
    {
        var secrets = await LoadSwarmSecretsAsync();
        secrets = await secrets.Set(key: request.Key, value: request.Value).SaveAsync();

        Others
            .Where(n => n.Type != NodeType.Ephemeral)
            .SendEach(n => n.UpdateSecretsAsync(secrets))
            ;

        return new();
    }

    public async Task<RemoveSecretResponse> RemoveSecretAsync(RemoveSecretRequest request) 
    {
        var secrets = await LoadSwarmSecretsAsync();
        secrets = await secrets.Remove(key: request.Key).SaveAsync();

        Others
            .Where(n => n.Type != NodeType.Ephemeral)
            .SendEach(n => n.UpdateSecretsAsync(secrets))
            ;

        return new();
    }

    public async Task<ListSecretsResponse> ListSecretsAsync(ListSecretsRequest request) 
    {
        var secrets = await LoadSwarmSecretsAsync();
        return new(secrets.Map.Keys.ToList());
    }

    public async Task<UpdateSecretsResponse> UpdateSecretsAsync(UpdateSecretsRequest request) 
    {
        await UpdateSecretsAsync(request.Secrets);
        return new();
    }

    #endregion

    #region ISwarm

    private static readonly Dictionary<string, Func<object, Task<SwarmResponse>>> _handlerCache = new();
    public async Task<SwarmResponse> SendAsync(SwarmRequest request)
    {
        // (0) AUTO-DETECT handler
        var requestType =
            Type.GetType(request.Type)
            ?? Type.GetType($"Swarmr.Base.Api.{request.Type}")
            ?? throw new Exception(
                $"Failed to find type \"{request.Type}\". " +
                $"Error 51d5d721-e588-4785-acf1-b0dab351119e."
                );

        if (!_handlerCache.TryGetValue(request.Type, out var handler))
        {

            var methods = typeof(Swarm).GetMethods();
            var method = methods.SingleOrDefault(info =>
                {
                    var ps = info.GetParameters();
                    if (ps.Length != 1) return false;
                    return ps[0].ParameterType == requestType;
                }) 
                ?? throw new Exception(
                    $"Failed to find handler for request type \"{requestType}\". " +
                    $"Error e3baf1d6-a2f4-4058-8a12-8a7976d57bc9."
                    );

            handler = async x =>
            {
                var arg = SwarmUtils.Deserialize(x, requestType);
                var o = method.Invoke(this, new[] { arg });
                var t = o as Task ?? throw new Exception("Not a task. Error 7cf6d3fc-dc42-47f6-8ef9-dc267bdb0c29.");
                await t.WaitAsync(TimeSpan.FromSeconds(10));
                var result = (object)((dynamic)t).Result;
                var responseType = result.GetType().AssemblyQualifiedName ?? throw new Exception(
                    $"Failed to get AssemblyQualifiedName for type \"{result.GetType()}\". " +
                    $"Error 15125585-a168-467e-8e1e-e9c620b3dca3."
                    );
                var response = new SwarmResponse(Type: responseType, Response: result);
                return response;
            };

            _handlerCache[request.Type] = handler;
            if (Verbose) AnsiConsole.MarkupLine($"[fuchsia][[SendAsync]] cached handler for {request.Type}[/]");
        }

        // (1) run handler
        var stopwatch = Stopwatch.StartNew();
        var response = await handler(request.Request);
        stopwatch.Stop();

        if (stopwatch.Elapsed > HANDLER_WARN_DURATION)
        {
            AnsiConsole.MarkupLine($"[yellow][[WARNING]]slow handler for {requestType.FullName} ({stopwatch.Elapsed})[/]");
        }

        return response;
    }

    #endregion

    #region Internal

    public record Dto(
        string? Primary,
        IReadOnlyList<Node> Nodes,
        string Secrets
        )
    {
        public static readonly Dto Empty = new(
            Primary: null,
            Nodes: ImmutableList<Node>.Empty,
            Secrets: null!
            );

        public Swarm ToSwarm(Node self, DirectoryInfo workdir, bool verbose) => new(
            self: self,
            workdir: workdir,
            primary: Primary,
            nodes: Nodes,
            verbose: verbose
            );

        public static async Task<Dto> FromSwarmAsync(Swarm swarm)
        {
            var secrets = await swarm.LoadSwarmSecretsAsync();
            return new(
                Primary: swarm.PrimaryId,
                Nodes: swarm.Nodes,
                Secrets: await secrets.EncodeAsync()
                );
        }
    }

    private readonly Dictionary<string, Node> _nodes = new();

    private Swarm(Node self, DirectoryInfo workdir, bool verbose)
        : this(self, workdir: workdir, primary: self.Id, Enumerable.Empty<Node>(), verbose: verbose)
    { }

    private Swarm(Node self, DirectoryInfo? workdir, string? primary, IEnumerable<Node> nodes, bool verbose)
    {
        Workdir = workdir ?? new DirectoryInfo(Info.DefaultWorkdir);
        Verbose = verbose;
        PrimaryId = primary;

        if (!Workdir.Exists)
        {
            AnsiConsole.MarkupLine($"[yellow]created workdir: {Workdir.FullName}[/]");
            Workdir.Create();
        }

        LocalSwarmFiles = new(
            basedir: Path.Combine(Workdir.FullName, "files"),
            nodeId: self.Id
            );
        self = self.UpsertFiles(LocalSwarmFiles.Files);


        foreach (var n in nodes) _nodes.Add(n.Id, n);

        _nodes[self.Id] = self;
        SelfId = self.Id;

        SwarmSecretsFile = new FileInfo(Path.Combine(Workdir.FullName, ".swarmsecrets"));
    }

    private async void StartHouseKeepingAsync(CancellationToken ct = default)
    {
        if (Verbose) AnsiConsole.WriteLine("[Info] starting background task: housekeeping");

        while (!ct.IsCancellationRequested)
        {
            try
            {
                //Console.WriteLine($"[HouseKeeping] {DateTimeOffset.UtcNow}");

                if (_failover.CurrentCount == 0)
                {
                    Console.WriteLine($"[HouseKeeping] {DateTimeOffset.UtcNow} failover in progress");
                    continue; // failover in progress ...
                }

                // update myself
                UpsertNode(Self with { LastSeen = DateTimeOffset.UtcNow });

                // consistency checks
                await CheckAndRepairDuplicateEntries();

                // health checks
                if (IAmPrimary)
                {
                    // I am the PRIMARY node:  let's ping all nodes to check health
                    // if there are unresponsive nodes, then notify everyone to remove them
                    var changed = await RefreshNodeListAsync(forcePingWithinTtl: false);
                    if (changed) _ = Task.Run(PrintNice, ct);
                }
                else
                {
                    // get primary node ...
                    if (TryGetPrimaryNode(out var primary))
                    {
                        // ping primary node ...
                        try
                        {
                            var updatedPrimary = await primary.Client.PingAsync();
                            UpsertNode(updatedPrimary);
                        }
                        catch (Exception e)
                        {
                            // failed to ping primary
                            AnsiConsole.WriteLine($"[HouseKeeping] failed to ping primary, because of {e.Message}");
                            FailoverAsync();
                            continue;
                        }
                    }
                    else
                    {
                        // there is no primary
                        AnsiConsole.WriteLine($"[HouseKeeping] there is no primary");
                        FailoverAsync();
                        continue;
                    }
                }
            }
            catch (Exception e)
            {
                AnsiConsole.WriteLine($"[HouseKeeping][Error] {e}");
            }
            finally
            {
                await Task.Delay(HOUSEKEEPING_INTERVAL, ct);
            }
        }
    }

    private async void StartSwarmTasksProcessingAsync(CancellationToken ct = default)
    {
        if (Verbose) AnsiConsole.WriteLine("[Info] starting background task: swarm tasks processing");

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var t = await _localTaskQueue.TryDequeue();
                if (t != null)
                {
                    await t.RunAsync(this);
                }
                else
                {
                    await Task.Delay(2000, ct);
                }
            }
            catch (Exception e)
            {
                AnsiConsole.WriteLine($"[SwarmTasksProcessing][Error] {e}");
            }
        }
    }

    /// <summary>
    /// Inserts or updates node WITHOUT notifying other nodes.
    /// </summary>
    internal Node UpsertNode(Node n)
    {
        lock (_nodes)
        {
            if (_nodes.TryGetValue(n.Id, out var existing))
            {
                if (existing.LastSeen > n.LastSeen && n.Id != SelfId)
                {
                    // we already have a newer state -> ignore update
                    AnsiConsole.MarkupLine($"[yellow][[UpsertNode]][[WARNING]] outdated node state, node {n.Id}), {n.LastSeen- existing.LastSeen}[/]");
                    return existing;
                }
            }

            _nodes[n.Id] = n;
            return n;
        }

    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RemoveNode(string id)
    {
        if (SelfId == id) return; // never remove self
        lock (_nodes) { _nodes.Remove(id); }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ExistsNode(string id)
    {
        lock (_nodes) return _nodes.ContainsKey(id);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ExistsNode(Node node) => ExistsNode(node.Id);

    private async Task SendOthers(Func<Node, Task> action)
    {
        foreach (var node in Others)
        {
            try
            {
                await action(node);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[SendOthers] action for {node.ConnectUrl} failed because of {e.Message}");
            }
        }
    }

    private async Task CheckAndRepairDuplicateEntries()
    {
        // find nodes with duplicate connection URLs
        // (this can happen if a node is restarted more than once
        // within the TTL time window (with the same URL but different ID)
        var gs = Nodes.GroupBy(n => n.ConnectUrl).Where(g => g.Count() > 1).ToArray();
        var changed = false;

        // each group
        foreach (var g in gs)
        {
            var connectionUrl = g.Key;

            if (Verbose)
            {
                Console.WriteLine($"[WARNING] found nodes with duplicate connection URL");
                Console.WriteLine($"[WARNING]   duplicate URL is: {connectionUrl}");
                Console.WriteLine($"[WARNING]   node IDs are: {string.Join(", ", g.Select(x => x.Id))}");
            }

            // (1) remove the entries with duplicate connection URL
            foreach (var node in g)
            {
                RemoveNode(node.Id);
                changed = true;
                if (Verbose) Console.WriteLine($"[WARNING]   removed node {node.Id}");
            }

            // (2) now connect to the URL and get the correct node info
            try
            {
                if (Verbose) Console.WriteLine($"[WARNING]   contacting {connectionUrl} to retrieve current node info ... ");
                var client = new NodeHttpClient(connectionUrl, self: Self);
                var n = await client.PingAsync();
                if (Verbose) Console.WriteLine($"[WARNING]   contacting {connectionUrl} to retrieve current node info ... DONE");
                //if (Verbose) Console.WriteLine($"[WARNING]   current node info is {n.ToJsonString()}");
                UpsertNode(n);
            }
            catch
            {
                if (Verbose)
                {
                    Console.WriteLine($"[WARNING]   failed to contact {connectionUrl}");
                    Console.WriteLine($"[WARNING]   done");
                }
            }
        }

        if (changed && Verbose) _ = Task.Run(PrintNice);
    }

    /// <summary>
    /// Remove all nodes that are not responsive.
    /// </summary>
    private async Task<bool> RefreshNodeListAsync(bool forcePingWithinTtl)
    {
        bool changed = false;
        var removedNodeIds = new List<string>();

        foreach (var node in Others)
        {
            if (node.Ago < NODE_TIMEOUT && !forcePingWithinTtl) continue;

            try
            {
                var updatedNode = await node.Client.PingAsync();
                UpsertNode(updatedNode);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[RefreshNodeListAsync] ping node \"{node.Id}\" failed with {e.Message}");
                RemoveNode(node.Id);
                removedNodeIds.Add(node.Id);
                Console.WriteLine($"[RefreshNodeListAsync] removed node \"{node.Id}\"");

                changed = true;
            }
        }

        if (removedNodeIds.Count > 0)
        {
            foreach (var node in Others)
            {
                try
                {
                    Console.WriteLine($"[RefreshNodeListAsync] notify {node.ConnectUrl} about removed node(s)");
                    await node.Client.RemoveNodesAsync(removedNodeIds);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[RefreshNodeListAsync] notify {node.ConnectUrl} failed, because of {e.Message}");
                }
            }
        }

        return changed;
    }

    private async Task UpdateSecretsAsync(string secrets) {
        var localSecrets = await LoadSwarmSecretsAsync();
        var remoteSecrets = await SwarmSecrets.DecodeAsync(secrets, SwarmSecretsFile);

        if (remoteSecrets.Revision > localSecrets.Revision) {
            // update
            if (Verbose) AnsiConsole.MarkupLine(
                $"[lime][[UpdateSecretsAsync]] update revision {localSecrets.Revision} to {remoteSecrets.Revision}[/]"
                );
            await remoteSecrets.SaveAsync();
        }
        else if (remoteSecrets.Revision < localSecrets.Revision) {
            // ignore outdated revision (warn)
            AnsiConsole.MarkupLine(
                $"[yellow][[WARNING]][[UpdateSecretsAsync]] " +
                $"Ignoring outdated revision {remoteSecrets.Revision}. " +
                $"Local revision is {localSecrets.Revision}.[/]"
                );
        }
        else {
            if (Verbose) AnsiConsole.MarkupLine(
                $"[lime][[UpdateSecretsAsync]] silently ignore same revision {remoteSecrets.Revision}[/]"
                );
            // silently ignore same revision
        }
    }

    #endregion

    #region Failover

    private long _failoverCount = 0L;
    private readonly SemaphoreSlim _failover = new(initialCount: 1);

    /// <summary>
    /// Identifies primary node and resynchronizes list of swarm nodes.
    /// </summary>
    private async void FailoverAsync()
    {
        // init
        var id = Interlocked.Increment(ref _failoverCount);
        void log(string msg) => AnsiConsole.MarkupLine(
            $"[lime][[FAILOVER]][[{id}]][[{DateTimeOffset.UtcNow}]] {msg.EscapeMarkup()}[/]")
            ;

        // failover
        if (!_failover.Wait(0))
        {
            log("failover already in progress");
            throw new Exception("Failover in progress.");
        }

        try
        {
            while (true)
            {
                log($"=========================================================");
                log($"starting failover");
                log($"my node id is {SelfId}");
                log($"local time is {DateTimeOffset.Now}");

                // (1) let's forget our current primary,
                //     as we will elect a new primary
                PrimaryId = null;

                // (2) remove all nodes that are not responsive
                log($"remove all nodes that are not responsive ...");
                var changed = await RefreshNodeListAsync(forcePingWithinTtl: true);
                if (changed) _ = Task.Run(PrintNice);

                // (3) who should be the primary?
                log($"choose nominee for primary ...");
                var myNominee = await ChooseNomineeForPrimary();
                log($"my nominee is {myNominee.Id}");
                if (myNominee.Id == SelfId)
                {
                    // I think, I'm the nominee ...
                    log($"I AM THE NOMINEE :-)");

                    // ... let's ask everyone else, what they think
                    log($"ask everyone else for their nominee");
                    var rivalNominees = new List<Node>();
                    foreach (var node in Nodes)
                    {
                        if (node.Id == SelfId) continue;
                        try
                        {
                            var nodesNominee = await node.Client.GetFailoverNomineeAsync(Self);
                            log($"   {node.Id} nominates {nodesNominee.Id}");

                            if (nodesNominee.Id != myNominee.Id)
                            {
                                rivalNominees.Add(nodesNominee);
                            }
                        }
                        catch (Exception e)
                        {
                            log($"    {node.Id} failed to answer ({e.Message})");
                        }
                    }

                    // are there any rival nominees?
                    if (rivalNominees.Count > 0)
                    {
                        // obviously there are nodes we do not know -> update our node list
                        log($"there are rival nominations -> update node list");
                        foreach (var rival in rivalNominees) UpsertNode(rival);

                        // ... and give up for now (and retry in a few moments)
                        log($"give up for now (and retry in a few moments)");
                        await Task.Delay(1234);
                        continue;
                    }
                    else
                    {
                        // if no rivals, then I am the new primary
                        PrimaryId = SelfId;
                        log($"no rival nominees -> I WON :-)");
                        if (Verbose) PrintNice();
                        return; // new primary: Self
                    }
                }
                else
                {
                    // ask my nominee who he would choose ...
                    log($"ask my nominee who he would choose");
                    try
                    {
                        var nomineesNominee = await myNominee.Client.GetFailoverNomineeAsync(Self);
                        log($"nominee's nominee is {nomineesNominee.Id}");
                        // ... and make this my new primary
                        log($"make {nomineesNominee.Id} my new primary");
                        UpsertNode(nomineesNominee);
                        PrimaryId = nomineesNominee.Id;
                        if (Verbose) PrintNice();
                        return; // new primary: nomineesNominee;
                    }
                    catch (Exception e)
                    {
                        log($"my nominee did not answer ({e.Message})");
                        log($"give up for now and retry in a few moments");
                        await Task.Delay(1234);
                        continue;
                    }
                }

                throw new Exception(
                    "Never reached. Final last words. " +
                    "Error a7f84010-1dd0-4492-94d1-f50b24e327df."
                    );
            }
        }
        catch (Exception e)
        {
            log($"oh no, failover failed with {e}");
        }
        finally
        {
            log($"=========================================================");
            _failover.Release();
        }

        throw new Exception(
            "Never reached. Final last words. " +
            "Error a097b58a-9174-4fb1-92c0-d72cc7c817a7."
            );
    }

    /// <summary>
    /// Chooses a nominee for primary from current node list.
    /// </summary>
    private async Task<Node> ChooseNomineeForPrimary()
    {
        while (true)
        {
            // (1) choose node with smallest id as nominee
            var nominee = Nodes.MinBy(x => x.Id);
            if (nominee == null)
            {
                Console.WriteLine($"[ChooseNomineeForPrimary] there are no nodes, although I am alive - something got completely wrong");
                Environment.Exit(1);
            }

            // (2)
            try
            {
                await nominee.Client.PingAsync();
                return nominee;
            }
            catch (Exception e)
            {
                Console.WriteLine($"[ChooseNomineeForPrimary] failed to ping my nominee {nominee.Id}.\n{e.Message}");
                Console.WriteLine($"[ChooseNomineeForPrimary] remove node from my list and get next nominee");
                RemoveNode(nominee.Id);
            }
        }
    }

    #endregion
}
