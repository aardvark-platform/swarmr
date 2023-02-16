namespace Swarmr.Base;

public record LocalConfig(
    DateTimeOffset Created,
    string? Workdir
    )
{
    private static readonly FileInfo LocalConfigPath = new(
        Path.Combine(
            Environment.GetFolderPath(
                Environment.SpecialFolder.LocalApplicationData,
                Environment.SpecialFolderOption.Create
                ),
            "swarmr",
            "config.json"
            )
        );

    public static async Task<LocalConfig> LoadAsync()
    {
        var config = LocalConfigPath.Exists
            ? SwarmUtils.Deserialize<LocalConfig>(
                await File.ReadAllTextAsync(LocalConfigPath.FullName)
                )
            : new(
                Created: DateTimeOffset.UtcNow,
                Workdir: null
                )
            ;

        return config;
    }

    public async Task SaveAsync()
    {
        var dir = LocalConfigPath.Directory!;
        if (!dir.Exists) dir.Create();

        await File.WriteAllTextAsync(
            LocalConfigPath.FullName, this.ToJsonString()
            );
    }
}
