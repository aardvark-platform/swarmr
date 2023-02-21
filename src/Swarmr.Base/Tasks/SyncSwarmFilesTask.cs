using Spectre.Console;
using Swarmr.Base;

namespace Swarmr.Base.Tasks;

/// <summary>
/// Syncs swarm files from specified node to our own swarm files.
/// </summary>
public record SyncSwarmFilesTask(string Id, Node Other) : ISwarmTask
{
    public async Task RunAsync(Swarm context)
    {
        if (Other.Type == NodeType.Ephemeral) return;

        foreach (var otherSwarmFile in Other.Files.Values)
        {
            try
            {
                var ownSwarmFile = await context.LocalSwarmFiles.TryReadAsync(otherSwarmFile.LogicalName);
                if (ownSwarmFile != null && ownSwarmFile.Hash == otherSwarmFile.Hash) continue;
            }
            catch (Exception e)
            {
                AnsiConsole.MarkupLine(
                    $"[red][[ERROR]][[SyncSwarmFilesTask]] delete swarm file {otherSwarmFile.LogicalName}, " +
                    $"because of\n" +
                    $"{e.Message.EscapeMarkup()}[/]"
                    );
                context.LocalSwarmFiles.Delete(otherSwarmFile);
            }

            AnsiConsole.WriteLine($"[UpdateNodeAsync] detected new swarm file {otherSwarmFile.ToJsonString()}");

            var (urlContent, urlMetadata) = otherSwarmFile.GetDownloadLinks(Other);
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
    }
}
