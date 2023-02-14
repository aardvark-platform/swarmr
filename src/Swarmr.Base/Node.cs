using System.Text.Json.Serialization;

namespace Swarmr.Base;

public record Node(
    string Id,
    DateTimeOffset Created,
    DateTimeOffset LastSeen,
    string Hostname,
    int Port,
    string ConnectUrl
    )
{
    public TimeSpan Ago => DateTimeOffset.UtcNow - LastSeen;

    [JsonIgnore]
    public NodeHttpClient Client => new(ConnectUrl);
}
