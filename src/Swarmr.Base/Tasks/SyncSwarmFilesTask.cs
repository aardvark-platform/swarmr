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
        foreach (var otherSwarmFile in Other.Files.Values)
        {
            var ownSwarmFile = await context.LocalSwarmFiles.TryReadAsync(otherSwarmFile.LogicalName);
            if (ownSwarmFile != null && ownSwarmFile.Hash == otherSwarmFile.Hash) continue;

            AnsiConsole.WriteLine($"[UpdateNodeAsync] detected new swarm file {otherSwarmFile.ToJsonString()}");

            //var dir = context.GetSwarmFileDir(otherSwarmFile.Name);
            //if (!dir.Exists) dir.Create();

            var urls = Other.GetDownloadLinks(otherSwarmFile);
            using var http = new HttpClient();
            foreach (var url in urls)
            {
                var targetFile = context.LocalSwarmFiles.GetContentFile(otherSwarmFile);

                AnsiConsole.WriteLine($"[UpdateNodeAsync] downloading {url} to {targetFile.FullName} ...");
                var sourceStream = await http.GetStreamAsync(url);
                var targetStream = File.Open(targetFile.FullName, FileMode.Create, FileAccess.Write, FileShare.None);
                await sourceStream.CopyToAsync(targetStream);
                targetStream.Close();
                sourceStream.Close();
                AnsiConsole.WriteLine($"[UpdateNodeAsync] downloading {url} to {targetFile.FullName} ... completed");
            }

            var newSelf = context.Self with
            {
                LastSeen = DateTimeOffset.UtcNow,
                Files = context.Self.Files.SetItem(otherSwarmFile.LogicalName, otherSwarmFile)
            };
            context.UpsertNode(newSelf);
            if (context.TryGetPrimaryNode(out var primary))
            {
                await primary.Client.UpdateNodeAsync(newSelf);
            }
        }
    }
}
