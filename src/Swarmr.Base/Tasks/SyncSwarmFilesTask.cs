using Spectre.Console;

namespace Swarmr.Base.Tasks;

/// <summary>
/// Syncs swarm files from specified node to our own local swarm files.
/// </summary>
public record SyncSwarmFilesTask(string Id, Node Other) : ISwarmTask
{
    public static SyncSwarmFilesTask Create(Node other) => new(
        Id: $"SyncSwarmFilesTask-{Guid.NewGuid()}",
        Other: other
        );

    public async Task RunAsync(Swarm context)
    {
        // we ignore ephemeral source nodes as they disappear quickly ...
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
                await context.LocalSwarmFiles.Delete(otherSwarmFile);
            }

            AnsiConsole.WriteLine($"[UpdateNodeAsync] detected new swarm file {otherSwarmFile.ToJsonString()}");

            await otherSwarmFile.DownloadToLocalAsync(fromNode: Other, toLocal: context.LocalSwarmFiles);

            context.Self.UpsertFile(otherSwarmFile, updateSwarm: context);
        }
    }
}
