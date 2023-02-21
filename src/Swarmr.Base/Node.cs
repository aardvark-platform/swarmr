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
    public TimeSpan Ago => DateTimeOffset.UtcNow - LastSeen;

    public string ConnectUrl => $"http://{Hostname}:{Port}";

    public (string urlContent, string urlMetadata) GetDownloadLinks(SwarmFile requestedSwarmFile)
    {
        var prefix = $"{ConnectUrl}/static/files/{requestedSwarmFile.LogicalName}";
        return (
            urlContent : $"{prefix}/{requestedSwarmFile.FileName}",
            urlMetadata: $"{prefix}/{SwarmFile.METAFILE_NAME}"
        );
    }

    public Node UpsertFile(SwarmFile x) => this with
    {
        Files = Files.SetItem(x.LogicalName, x)
    };

    public Node UpsertFiles(IEnumerable<SwarmFile> xs) => this with
    {
        Files = Files.SetItems(xs.Select(x => KeyValuePair.Create(x.LogicalName, x)))
    };

    [JsonIgnore]
    public ISwarm Client => new NodeHttpClient(ConnectUrl, self: this);
}
