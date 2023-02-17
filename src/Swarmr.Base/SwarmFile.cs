namespace Swarmr.Base;

public record SwarmFile(
    string Name,
    DateTimeOffset Created,
    string Hash,
    string FileName
    );
