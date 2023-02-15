namespace Swarmr.Base;

public record Runner(
    string Name,
    DateTimeOffset Created,
    string Runtime,
    string Hash,
    string FileName
    );
