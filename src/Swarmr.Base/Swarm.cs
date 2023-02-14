﻿using Microsoft.VisualBasic;
using Spectre.Console;
using Swarmr.Base.Api;
using System.Security.Cryptography;

namespace Swarmr.Base;

public class Swarm : ISwarm
{
    public static readonly TimeSpan HOUSEKEEPING_INTERVAL = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan NODE_TIMEOUT = TimeSpan.FromSeconds(15);
    public const string DEFAULT_WORKDIR = ".swarmr";

    public string SelfId { get; }
    public string WorkDir { get; }
    public string? PrimaryId { get; private set; }

    public bool IAmPrimary => SelfId == PrimaryId;
    public IReadOnlyList<Node> Nodes
    {
        get
        {
            lock (_nodes) return _nodes.Values.ToArray();
        }
    }
    public Node? TryGetPrimaryNode()
    {
        var primaryId = PrimaryId;
        lock (_nodes)
        {
            if (primaryId == null) return null;
            _nodes.TryGetValue(primaryId, out var primary);
            return primary;
        }
    }

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="self"></param>
    /// <param name="url">URL of an active swarm node. Joins this node's swarm.</param>
    /// <returns></returns>
    public static async Task<Swarm> ConnectAsync(
        Node self,
        string? url,
        string workdir
        )
    {
        if (url != null && url.EndsWith('/')) url = url[..^1];

        Swarm swarm;
        if (url != null)
        {
            // clone the swarm from the node at given URL
            var swarmNode = new NodeHttpClient(url);
            swarm = await swarmNode.JoinSwarmAsync(self, workdir: workdir);
        }
        else
        {
            // create a new swarm of one, with myself as primary
            swarm = new Swarm(self, workdir: workdir);
        }

        swarm.StartHouseKeeping();
        return swarm;
    }

    public void PrintNice()
    {
        var swarmPanel = new Panel(this.ToJsonString().EscapeMarkup()).Header("Swarm");
        AnsiConsole.Write(swarmPanel);
    }

    #region ISwarm

    public async Task<JoinSwarmResponse> JoinSwarmAsync(JoinSwarmRequest request)
    {
        UpsertNode(request.Candidate);

        if (IAmPrimary)
        {
            // notify all nodes of newly joined node ...
            await NotifyOthersAboutNewNode(request.Candidate);
        }
        else
        {
            var primary = TryGetPrimaryNode();
            if (primary != null)
            {
                // notify primary node about candidate
                try
                {
                    await primary.Client.UpdateNodeAsync(request.Candidate);
                }
                catch (Exception e)
                {
                    Console.WriteLine(
                        $"[JoinSwarmAsync] failed to notify primary node {primary.Id} " +
                        $"about candidate \"{request.Candidate.Id}\", " +
                        $"because of {e.Message}"
                        );
                }
            }
            else
            {
                Console.WriteLine($"[HouseKeeping] there is no primary");
                await Failover();
            }
        }

        PrintNice();

        return new JoinSwarmResponse(Swarm: Dto.FromSwarm(this));
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
        var response = new PingResponse(Node: Self);
        return Task.FromResult(response);
    }

    public async Task<UpdateNodeResponse> UpdateNodeAsync(UpdateNodeRequest request)
    {
        if (IAmPrimary && !ExistsNode(request.Node.Id))
        {
            // this is a new node, and since I am the primary node,
            // it is my duty to inform all others about this new member
            await NotifyOthersAboutNewNode(request.Node);
        }

        UpsertNode(request.Node);
        PrintNice();

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

        // (2) choose nominee from node list
        var nominee = await ChooseNomineeForPrimary();
        return new(Nominee: nominee);
    }
    
    public async Task<RegisterRunnerResponse> RegisterRunnerAsync(RegisterRunnerRequest request)
    {
        var sourcefile = new FileInfo(request.Source);
        if (!sourcefile.Exists) throw new Exception($"File does not exist (\"{request.Source}\").");

        var hashstream = sourcefile.OpenRead();
        var hash = Convert.ToHexString(await SHA256.Create().ComputeHashAsync(hashstream)).ToLowerInvariant();
        hashstream.Close();

        var targetdir = new DirectoryInfo(Path.Combine(WorkDir, "runners", request.Name));
        if (!targetdir.Exists) targetdir.Create();

        var targetfile = new FileInfo(Path.Combine(targetdir.FullName, hash + sourcefile.Extension));
        using var targetstream = targetfile.OpenWrite();
        using var sourcestream = sourcefile.OpenRead();
        await sourcestream.CopyToAsync(targetstream);

        var runner = new Runner(
            Created: DateTimeOffset.UtcNow,
            Name: request.Name,
            Runtime: request.Runtime,
            Hash: targetfile.Name
            );

        return new(runner);
    }

    #endregion

    #region Internal

    public record Dto(
        string? Primary,
        IReadOnlyList<Node> Nodes
        )
    {
        public Swarm ToSwarm(Node self, string workdir) => new(this, self, workdir);
        public static Dto FromSwarm(Swarm swarm) => new(
            Primary: swarm.PrimaryId,
            Nodes: swarm.Nodes
            );
    }

    private readonly Dictionary<string, Node> _nodes = new();

    private Swarm(Node self, string workdir)
        : this(self, workdir: workdir, primary: self.Id, Enumerable.Empty<Node>())
    { }

    private Swarm(Dto dto, Node self, string workdir) 
        : this(self, workdir: workdir, primary: dto.Primary, dto.Nodes)
    { }

    private Swarm(Node self, string workdir, string? primary, IEnumerable<Node> nodes)
    {
        foreach (var n in nodes) _nodes.Add(n.Id, n);
        _nodes[self.Id] = self;
        SelfId = self.Id;
        WorkDir = workdir;
        PrimaryId = primary;

        var d = new DirectoryInfo(workdir);
        if (!d.Exists)
        {
            AnsiConsole.MarkupLine($"[yellow]created workdir: {d.FullName}[/]");
            d.Create();
        }
    }

    private async void StartHouseKeeping(CancellationToken ct = default)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                //Console.WriteLine($"[HouseKeeping] {DateTimeOffset.UtcNow}");

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
                    if (changed) PrintNice();
                }
                else
                {
                    // I'm yet another node
                    var primary = TryGetPrimaryNode();
                    if (primary != null)
                    {
                        // ping primary node
                        try
                        {
                            var updatedPrimary = await primary.Client.PingAsync();
                            UpsertNode(updatedPrimary);
                        }
                        catch (Exception e)
                        {
                            // failed to ping primary
                            Console.WriteLine($"[HouseKeeping] failed to ping primary, because of {e.Message}");
                            await Failover();
                        }
                    }
                    else
                    {
                        // there is no primary
                        Console.WriteLine($"[HouseKeeping] there is no primary");
                        await Failover();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[HouseKeeping][Error] {e}");
            }
            finally
            {
                await Task.Delay(HOUSEKEEPING_INTERVAL, ct);
            }
        }
    }

    private Node Self
    {
        get
        {
            lock (_nodes)
            {
                return _nodes[SelfId];
            }
        }
    }

    private void UpsertNode(Node n)
    {
        lock (_nodes)
        {
            if (_nodes.TryGetValue(n.Id, out var existing))
            {
                if (existing.LastSeen > n.LastSeen)
                {
                    // we already have a newer state
                    AnsiConsole.MarkupLine($"[yellow][[UpsertNode]][[WARNING]] we have a newer node state -> ignoring upsert[/]");
                    return;
                }
            }

            _nodes[n.Id] = n;
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

    private async Task NotifyOthersAboutNewNode(Node newNode)
    {
        foreach (var node in Nodes)
        {
            try
            {
                if (node.Id == SelfId) continue;     // don't notify myself
                if (node.Id == newNode.Id) continue; // don't notify new node itself

                Console.WriteLine($"[NotifyOthersAboutNewNode] notify {node.ConnectUrl} about new node \"{newNode.Id}\"");
                await node.Client.UpdateNodeAsync(newNode);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[HouseKeeping] notify {node.ConnectUrl} failed because of {e.Message}");
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

            Console.WriteLine($"[WARNING] found nodes with duplicate connection URL");
            Console.WriteLine($"[WARNING]   duplicate URL is: {connectionUrl}");
            Console.WriteLine($"[WARNING]   node IDs are: {string.Join(", ", g.Select(x => x.Id))}");

            // (1) remove the entries with duplicate connection URL
            foreach (var node in g)
            {
                RemoveNode(node.Id);
                Console.WriteLine($"[WARNING]   removed node {node.Id}");
            }

            // (2) now connect to the URL and get the correct node info
            try
            {
                Console.WriteLine($"[WARNING]   contacting {connectionUrl} to retrieve current node info");
                var client = new NodeHttpClient(connectionUrl);
                var n = await client.PingAsync();
                Console.WriteLine($"[WARNING]   current node info is {n.ToJsonString()}");
                UpsertNode(n);
            }
            catch
            {
                Console.WriteLine($"[WARNING]   failed to contact {connectionUrl}");
                Console.WriteLine($"[WARNING]   done");
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

        foreach (var node in Nodes)
        {
            if (node.Id == SelfId) continue;

            if (node.Ago < NODE_TIMEOUT && !forcePingWithinTtl) continue;

            try
            {
                //Console.WriteLine($"[HouseKeeping] ping node \"{node.Id}\"");
                var updatedNode = await node.Client.PingAsync();
                UpsertNode(updatedNode);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[HouseKeeping] ping node \"{node.Id}\" failed with {e.Message}");
                RemoveNode(node.Id);
                removedNodeIds.Add(node.Id);
                Console.WriteLine($"[HouseKeeping] removed node \"{node.Id}\"");

                changed = true;
            }
        }

        if (removedNodeIds.Count > 0)
        {
            foreach (var node in Nodes)
            {
                try
                {
                    Console.WriteLine($"[HouseKeeping] notify {node.ConnectUrl} about removed node(s)");
                    await node.Client.RemoveNodesAsync(removedNodeIds);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[HouseKeeping] notify {node.ConnectUrl} failed, because of {e.Message}");
                }
            }
        }

        return changed;
    }

    /// <summary>
    /// Identifies primary node and resynchronizes list of swarm nodes.
    /// </summary>
    private async Task Failover()
    {
        Console.WriteLine($"[Failover] start");
        Console.WriteLine($"[Failover] my node id is {SelfId}");

        // (1) let's forget our current primary,
        //     as we will elect a new primary
        PrimaryId = null;

        // (2) remove all nodes that are not responsive
        await RefreshNodeListAsync(forcePingWithinTtl: true);

        // (3) who should be the primary?
        var myNominee = await ChooseNomineeForPrimary();
        Console.WriteLine($"[Failover] my nominee is {myNominee.Id}");
        if (myNominee.Id == SelfId)
        {
            // I think, I'm the nominee ...
            Console.WriteLine($"[Failover] I AM THE NOMINEE :-)");

            // ... let's ask everyone else, what they think
            Console.WriteLine($"[Failover] ask everyone else for their nominee");
            var rivalNominees = new List<Node>();
            foreach (var node in Nodes)
            {
                if (node.Id == SelfId) continue;
                try
                {
                    var nodesNominee = await node.Client.GetFailoverNomineeAsync();
                    Console.WriteLine($"[Failover]   {node.Id} nominates {nodesNominee.Id}");

                    if (nodesNominee.Id != myNominee.Id)
                    {
                        rivalNominees.Add(nodesNominee);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[Failover]   {node.Id} failed to answer ({e.Message})");
                }
            }

            // are there any rival nominees?
            if (rivalNominees.Count > 0)
            {
                // obviously there are nodes we do not know -> update our node list
                Console.WriteLine($"[Failover] there are rival nominations -> update node list");
                foreach (var rival in rivalNominees) UpsertNode(rival);

                // ... and give up for now (we will try again at next housekeeping cycle)
                Console.WriteLine($"[Failover] give up for now and wait for next housekeeping cycle");
                return;
            }
            else
            {
                // if no rivals, then I am the new primary
                Console.WriteLine($"[Failover] no rival nominees -> I WON :-)");
                PrimaryId = SelfId;
                PrintNice();
                return;
            }
        }
        else
        {
            // ask my nominee who he would choose ...
            Console.WriteLine($"[Failover] ask my nominee who he would choose");
            try
            {
                var nomineesNominee = await myNominee.Client.GetFailoverNomineeAsync();
                Console.WriteLine($"[Failover] nominee's nominee is {nomineesNominee.Id}");
                // ... and make this my new primary
                Console.WriteLine($"[Failover] make {nomineesNominee.Id} my new primary");
                UpsertNode(nomineesNominee);
                PrimaryId = nomineesNominee.Id;
                PrintNice();
            }
            catch (Exception e)
            {
                Console.WriteLine($"[Failover] my nominee did not answer ({e.Message})");
                Console.WriteLine($"[Failover] give up for now and wait for next housekeeping cycle");
            }
            return;
        }

        throw new NotImplementedException();
    }

    /// <summary>
    /// Chooses a nominee for primary from current node list.
    /// </summary>
    private async Task<Node> ChooseNomineeForPrimary()
    {
        // (1) first let's check who is REALLY still around
        await RefreshNodeListAsync(forcePingWithinTtl: true);

        // (2) choose node with smallest id as nominee
        var nominee = Nodes.MinBy(x => x.Id);
        if (nominee == null)
        {
            Console.WriteLine($"[ChooseNomineeForPrimary] there are no nodes, although I am alive - something got completely wrong");
            Environment.Exit(1);
        }
        return nominee;
    }

    #endregion
}
