namespace Swarmr.Base;

public record Runner(
    DateTimeOffset Created,
    string Name,
    string Runtime,
    string Hash
    );
