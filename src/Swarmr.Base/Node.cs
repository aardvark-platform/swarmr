namespace Swarmr.Base;

public record Node(
    string Id,
    DateTimeOffset Created,
    DateTimeOffset LastSeen,
    string Hostname,
    int Port
    )
{
    public TimeSpan Ago => DateTimeOffset.UtcNow - LastSeen;
}
