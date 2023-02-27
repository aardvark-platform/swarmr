using Swarmr.Base.Api;
using System.Collections.Immutable;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public enum NodeType
{
    Worker,
    Client,
    Ephemeral
}

public enum NodeStatus
{
    Idle,
    Busy
}

public record Node(
    string Id,
    DateTimeOffset Created,
    DateTimeOffset LastSeen,
    string Hostname,
    int Port,
    ImmutableDictionary<string, SwarmFile> Files,
    NodeType Type,
    NodeStatus Status
    )
{
    public TimeSpan Uptime => DateTimeOffset.UtcNow - Created;
    public TimeSpan SeenAgo => DateTimeOffset.UtcNow - LastSeen;

    public string ConnectUrl => $"http://{Hostname}:{Port}";

    public Node UpsertFile(SwarmFile x) => this with
    {
        LastSeen = DateTimeOffset.UtcNow,
        Files = Files.SetItem(x.LogicalName, x)
    };

    public Node UpsertFile(SwarmFile x, Swarm updateSwarm) 
        => updateSwarm.UpsertNode(UpsertFile(x));

    public Node UpsertFiles(IEnumerable<SwarmFile> xs) => this with
    {
        LastSeen = DateTimeOffset.UtcNow,
        Files = Files.SetItems(xs.Select(x => KeyValuePair.Create(x.LogicalName, x)))
    };

    public Node UpsertFiles(IEnumerable<SwarmFile> xs, Swarm updateSwarm)
        => updateSwarm.UpsertNode(UpsertFiles(xs));

    [JsonIgnore]
    public ISwarm Client => new NodeHttpClient(ConnectUrl, self: this);
}
