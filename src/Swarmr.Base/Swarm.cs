using Swarmr.Base.Api;
using System.Net.Http.Json;

namespace Swarmr.Base;

public class Swarm : ISwarm
{
    #region DTO

    public record Dto(
        string Primary,
        IReadOnlyList<Node> Nodes
        )
    {
        public Swarm ToSwarm() => new Swarm(this);
    }

    public Dto ToDto() => new(Primary: Primary, Nodes: Nodes);

    #endregion

    public string Primary { get; private set; }

    private Dictionary<string, Node> _nodes = new();
    public IReadOnlyList<Node> Nodes
    {
        get
        {
            lock (_nodes) return _nodes.Values.ToArray();
        }
    }

    private Swarm(Node primary) 
    {
        _nodes.Add(primary.Id, primary);
        Primary = primary.Id;
    }

    private Swarm(Dto dto)
    {
        foreach (var n in dto.Nodes) _nodes.Add(n.Id, n);
        Primary = dto.Primary;
    }

    public static async Task<Swarm> ConnectAsync(string? url = null, int portToListenOn = Info.DefaultPort)
    {
        if (url != null && url.EndsWith('/')) url = url[..^1];

        var myself = new Node(
            Id: Guid.NewGuid().ToString(),
            Created: DateTimeOffset.UtcNow,
            LastSeen: DateTimeOffset.UtcNow,
            Hostname: Environment.MachineName.ToLowerInvariant(),
            Port: portToListenOn
            );


        if (url != null)
        {
            var swarmNode = new NodeHttpClient(url);
            var x = await swarmNode.JoinSwarmAsync(myself);
            return x;
        }
        else
        {
            return new Swarm(
                primary: myself
                );
        }
    }

    public void AddNode(Node n)
    {
        lock (_nodes) _nodes[n.Id] = n;
    }

    public Task<JoinSwarmResponse> JoinSwarmAsync(JoinSwarmRequest request)
    {
        AddNode(request.Self);
        var response = new JoinSwarmResponse(ToDto());
        return Task.FromResult(response);
    }
}
