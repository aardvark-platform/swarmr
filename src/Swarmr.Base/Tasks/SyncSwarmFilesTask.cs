using Spectre.Console;
using Swarmr.Base.Api;

namespace Swarmr.Base.Tasks;

/// <summary>
/// Syncs swarm files from specified node with our own swarm files.
/// </summary>
public record SyncSwarmFilesTask(Node Other) : ISwarmTask
{
    public async Task RunAsync(Swarm context)
    {
        foreach (var otherSwarmFile in Other.SwarmFiles.Values)
        {
            var ownSwarmFile = await context.TryReadSwarmFileAsync(otherSwarmFile.Name);
            if (ownSwarmFile != null && ownSwarmFile.Hash == otherSwarmFile.Hash) continue;

            AnsiConsole.WriteLine($"[UpdateNodeAsync] detected new swarm file {otherSwarmFile.ToJsonString()}");

            var dir = context.GetSwarmFileDir(otherSwarmFile.Name);
            if (!dir.Exists) dir.Create();

            var urls = Other.GetDownloadLinks(otherSwarmFile);
            using var http = new HttpClient();
            foreach (var url in urls)
            {
                var targetFileName = Path.Combine(dir.FullName, Path.GetFileName(url));

                AnsiConsole.WriteLine($"[UpdateNodeAsync] downloading {url} to {targetFileName} ...");
                var source = await http.GetStreamAsync(url);
                var target = File.Open(targetFileName, FileMode.Create, FileAccess.Write, FileShare.None);
                await source.CopyToAsync(target);
                target.Close();
                source.Close();
                AnsiConsole.WriteLine($"[UpdateNodeAsync] downloading {url} to {targetFileName} ... completed");
            }

            var newSelf = context.Self with
            {
                LastSeen = DateTimeOffset.UtcNow,
                SwarmFiles = context.Self.SwarmFiles.SetItem(otherSwarmFile.Name, otherSwarmFile)
            };
            context.UpsertNode(newSelf);
            if (context.TryGetPrimaryNode(out var primary))
            {
                await primary.Client.UpdateNodeAsync(newSelf);
            }
        }
    }
}
