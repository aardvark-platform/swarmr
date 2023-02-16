using System.Collections.Immutable;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public record Node(
    string Id,
    DateTimeOffset Created,
    DateTimeOffset LastSeen,
    string Hostname,
    int Port,
    ImmutableDictionary<string, Runner> AvailableRunners
    )
{
    public TimeSpan Ago => DateTimeOffset.UtcNow - LastSeen;

    public string ConnectUrl => $"http://{Hostname}:{Port}";

    public string[] GetDownloadLinks(Runner runner)
    {
        if (!HasRunnerWithHash(runner.Hash))
        {
            throw new Exception($"Runner is not available from this node: {runner.ToJsonString()}");
        }

        var connectUrl = ConnectUrl.EndsWith('/') ? ConnectUrl[..^1] : ConnectUrl;
        var prefix = $"{connectUrl}/static/runners/{runner.Name}/executable";
        return new[]
        {
            $"{prefix}/{runner.FileName}",
            $"{prefix}/runner.json"
        };
    }

    public bool HasRunnerWithHash(string hash)
        => AvailableRunners.Values.Select(x => x.Hash).Contains(hash);

    [JsonIgnore]
    public NodeHttpClient Client => new(ConnectUrl);
}
