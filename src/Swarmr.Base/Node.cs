using Swarmr.Base.Api;
using System.Collections.Immutable;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public record Node(
    string Id,
    DateTimeOffset Created,
    DateTimeOffset LastSeen,
    string Hostname,
    int Port,
    ImmutableDictionary<string, SwarmFile> Files
    )
{
    public TimeSpan Ago => DateTimeOffset.UtcNow - LastSeen;

    public string ConnectUrl => $"http://{Hostname}:{Port}";

    public string[] GetDownloadLinks(SwarmFile requestedSwarmFile)
    {
        var connectUrl = ConnectUrl.EndsWith('/') ? ConnectUrl[..^1] : ConnectUrl;
        var prefix = $"{connectUrl}/static/files/{requestedSwarmFile.LogicalName}";
        return new[]
        {
            $"{prefix}/{requestedSwarmFile.FileName}",
            $"{prefix}/{SwarmFile.METAFILE_NAME}"
        };
    }

    public Node Upsert(SwarmFile x) => this with
    {
        Files = Files.SetItem(x.LogicalName, x)
    };

    public Node Upsert(IEnumerable<SwarmFile> xs) => this with
    {
        Files = Files.SetItems(xs.Select(x => KeyValuePair.Create(x.LogicalName, x)))
    };

    [JsonIgnore]
    public ISwarm Client => new NodeHttpClient(ConnectUrl);
}
