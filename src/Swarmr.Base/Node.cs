using System.Collections.Immutable;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public record Node(
    string Id,
    DateTimeOffset Created,
    DateTimeOffset LastSeen,
    string Hostname,
    int Port,
    ImmutableDictionary<string, SwarmFile> SwarmFiles
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
            $"{prefix}/file.json"
        };
    }

    [JsonIgnore]
    public NodeHttpClient Client => new(ConnectUrl);
}
