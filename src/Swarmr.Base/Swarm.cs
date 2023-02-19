using Spectre.Console;
using Swarmr.Base.Api;
using Swarmr.Base.Tasks;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public class Swarm : ISwarm
{
    public static readonly TimeSpan HOUSEKEEPING_INTERVAL = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan NODE_TIMEOUT = TimeSpan.FromSeconds(15);
    public const string DEFAULT_WORKDIR = ".swarmr";

    private readonly SwarmTaskQueue _swarmTaskQueue = new();

    public string SelfId { get; }
    public string? PrimaryId { get; private set; }
    public string Workdir { get; }
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
        string workdir,
        bool verbose,
        CancellationToken ct = default
        )
    {
        workdir = Path.GetFullPath(workdir);

        Swarm? swarm = null;
        if (url != null)
        {
            // clone the swarm from the node at given URL
            if (verbose) AnsiConsole.WriteLine($"connecting to {url} ...");
            var swarmNode = new NodeHttpClient(url);
            swarm = await swarmNode.JoinSwarmAsync(self, workdir: workdir, verbose: verbose);
            if (verbose) AnsiConsole.WriteLine($"connecting to {url} ... done");
        }
        else
        {
            // create a new swarm of one, with myself as primary
            swarm ??= new Swarm(self, workdir: workdir, verbose: verbose);
        }

        // start housekeeping (background)
        swarm.StartHouseKeepingAsync(ct);

        // start swarm task queue processing (background)
        swarm.StartSwarmTasksProcessingAsync(ct);

        return swarm;
    }

    public static async Task<Swarm> ConnectAsync(
        string? customRemoteHost,
        int? listenPort,
        string? customWorkDir,
        bool verbose
        )
    {
        // (0) Arguments.
        var (remoteHost, remotePort) = SwarmUtils.ParseHost(customRemoteHost);
        if (string.IsNullOrWhiteSpace(customWorkDir))
        {
            var config = await LocalConfig.LoadAsync();
            if (config.Workdir != null)
            {
                customWorkDir = config.Workdir;
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
            Files: ImmutableDictionary<string, SwarmFile>.Empty
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

    public void PrintNice()
    {
        var swarmPanel = new Panel(this.ToJsonString().EscapeMarkup()).Header("Swarm");
        AnsiConsole.Write(swarmPanel);
    }

    #region ISwarm message handlers (will be auto-detected)

    public async Task<JoinSwarmResponse> JoinSwarmAsync(JoinSwarmRequest request)
    {
        var c = request.Node with { LastSeen = DateTimeOffset.UtcNow };

        if (IAmPrimary)
        {
            // notify all nodes of newly joined node ...
            await Others.Except(request.Node).SendEachAsync(n => n.UpdateNodeAsync(request.Node));
            UpsertNode(c);
        }
        else
        {
            if (TryGetPrimaryNode(out var primary))
            {
                // notify primary node about joining node
                await primary.Client.UpdateNodeAsync(c); 
            }
            else
            {
                Console.WriteLine($"[JoinSwarmAsync] there is no primary");
                throw new Exception("There is no primary node.");
            }
        }

        if (Verbose) PrintNice();
        return new JoinSwarmResponse(Swarm: Dto.FromSwarm(this));
    }

    public async Task<LeaveSwarmResponse> LeaveSwarmAsync(LeaveSwarmRequest request)
    {
        if (IAmPrimary)
        {
            // notify all nodes of newly joined node ...
            await Others.Except(request.Node).SendEachAsync(n => n.LeaveSwarmAsync(request.Node));
            RemoveNode(request.Node.Id);
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

        if (Verbose) PrintNice();
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
        var newSelf = UpsertNode(Self with { LastSeen = DateTimeOffset.UtcNow });
        var response = new PingResponse(Node: newSelf);
        return Task.FromResult(response);
    }

    public async Task<UpdateNodeResponse> UpdateNodeAsync(UpdateNodeRequest request)
    {
        if (IAmPrimary)
        {
            // I am the primary node, so it is my duty
            // to inform all others about this new member
            await Others.Except(request.Node).SendEachAsync(n => n.UpdateNodeAsync(request.Node));
        }

        UpsertNode(request.Node);
        PrintNice();

        // sync swarm files
        if (request.Node.Id != Self.Id)
        {
            await _swarmTaskQueue.Enqueue(new SyncSwarmFilesTask(request.Node));
        }

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

        // (1) let's see if I am the primary
        if (IAmPrimary)
        {
            // mmmh, I am obviously alive - let's nominate myself
            return new(Nominee: Self);
        }

        // (2) choose nominee from my node list
        var nominee = await ChooseNomineeForPrimary();
        return new(Nominee: nominee);
    }

    public async Task<IngestFileResponse> IngestFileAsync(IngestFileRequest request)
    {
        var t = IngestFileTask.Create(request);
        await _swarmTaskQueue.Enqueue(t);
        return new(Task: t);
    }

    public async Task<RunJobResponse> RunJobAsync(RunJobRequest request)
    {
        var t = RunJobTask.Create(request);
        await _swarmTaskQueue.Enqueue(t);
        return new(Task: t);
    }

    public async Task<SubmitTaskResponse> SubmitTaskAsync(SubmitTaskRequest request)
    {
        var t = SwarmTask.Deserialize(request.Task);
        await t.RunAsync(context: this);
        return new();
    }

    #endregion

    #region ISwarm

    private static Dictionary<string, Func<object, Task<SwarmResponse>>> _handlerCache = new();
    public Task<SwarmResponse> SendAsync(SwarmRequest request)
    {
        if (!_handlerCache.TryGetValue(request.Type, out var handler))
        {
            var requestType =
                Type.GetType(request.Type)
                ?? Type.GetType($"Swarmr.Base.Api.{request.Type}")
                ?? throw new Exception(
                    $"Failed to find type \"{request.Type}\". " +
                    $"Error 51d5d721-e588-4785-acf1-b0dab351119e."
                    );

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

        return handler(request.Request);
    }

    #endregion

    #region Internal

    public record Dto(
        string? Primary,
        IReadOnlyList<Node> Nodes
        )
    {
        public Swarm ToSwarm(Node self, string workdir, bool verbose) => new(
            self: self,
            workdir: workdir,
            primary: Primary,
            nodes: Nodes,
            verbose: verbose
            );

        public static Dto FromSwarm(Swarm swarm) => new(
            Primary: swarm.PrimaryId,
            Nodes: swarm.Nodes
            );
    }

    private readonly Dictionary<string, Node> _nodes = new();

    private Swarm(Node self, string workdir, bool verbose)
        : this(self, workdir: workdir, primary: self.Id, Enumerable.Empty<Node>(), verbose: verbose)
    { }

    private Swarm(Node self, string? workdir, string? primary, IEnumerable<Node> nodes, bool verbose)
    {
        Workdir = Path.GetFullPath(workdir ?? Info.DefaultWorkdir);
        Verbose = verbose;
        PrimaryId = primary;

        var d = new DirectoryInfo(Workdir);
        if (!d.Exists)
        {
            AnsiConsole.MarkupLine($"[yellow]created workdir: {d.FullName}[/]");
            d.Create();
        }

        LocalSwarmFiles = new(Path.Combine(d.FullName, "files"));
        self = self.Upsert(LocalSwarmFiles.Files);

        foreach (var n in nodes) _nodes.Add(n.Id, n);

        _nodes[self.Id] = self;
        SelfId = self.Id;
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
                    //if (changed) PrintNice();
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
                var t = await _swarmTaskQueue.TryDequeue();
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

    internal Node UpsertNode(Node n)
    {
        lock (_nodes)
        {
            if (_nodes.TryGetValue(n.Id, out var existing))
            {
                if (existing.LastSeen > n.LastSeen)
                {
                    // we already have a newer state -> ignore update
                    AnsiConsole.MarkupLine($"[yellow][[UpsertNode]][[WARNING]] outdated node state ({n.LastSeen- existing.LastSeen})[/]");
                    return existing;
                }
            }

            _nodes[n.Id] = n;
            return n;
        }
    }

    private void RemoveNode(string id)
    {
        if (SelfId == id) return; // never remove self
        lock (_nodes) { _nodes.Remove(id); }
    }

    private bool ExistsNode(string id)
    {
        lock (_nodes) return _nodes.ContainsKey(id);
    }

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
                if (Verbose) Console.WriteLine($"[WARNING]   removed node {node.Id}");
            }

            // (2) now connect to the URL and get the correct node info
            try
            {
                if (Verbose) Console.WriteLine($"[WARNING]   contacting {connectionUrl} to retrieve current node info");
                var client = new NodeHttpClient(connectionUrl);
                var n = await client.PingAsync();
                if (Verbose) Console.WriteLine($"[WARNING]   current node info is {n.ToJsonString()}");
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
                await RefreshNodeListAsync(forcePingWithinTtl: true);

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
                            var nodesNominee = await node.Client.GetFailoverNomineeAsync();
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
                        var nomineesNominee = await myNominee.Client.GetFailoverNomineeAsync();
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
