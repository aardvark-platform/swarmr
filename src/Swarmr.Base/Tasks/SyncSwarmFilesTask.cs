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
        var changed = false;

        foreach (var otherSwarmFile in Other.Files.Values)
        {
            var ownSwarmFile = await context.LocalSwarmFiles.TryReadAsync(otherSwarmFile.LogicalName);
            if (ownSwarmFile != null && ownSwarmFile.Hash == otherSwarmFile.Hash) continue;

            AnsiConsole.WriteLine($"[UpdateNodeAsync] detected new swarm file {otherSwarmFile.ToJsonString()}");
            changed = true;

            var (urlContent, urlMetadata) = Other.GetDownloadLinks(otherSwarmFile);
            using var http = new HttpClient();

            var fileContent = context.LocalSwarmFiles.GetContentFile(otherSwarmFile);
            var fileMetadata = context.LocalSwarmFiles.GetMetadataFile(otherSwarmFile);

            fileMetadata.Delete();
            await http.DownloadToFile(urlContent , fileContent);
            await http.DownloadToFile(urlMetadata, fileMetadata);

            var newSelf = context.Self with
            {
                LastSeen = DateTimeOffset.UtcNow,
                Files = context.Self.Files.SetItem(otherSwarmFile.LogicalName, otherSwarmFile)
            }; 
            context.UpsertNode(newSelf);
        }

        if (changed)
        {
            await context.Primary.Client.UpdateNodeAsync(context.Self);
        }
    }
}
