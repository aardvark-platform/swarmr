using Spectre.Console;
using Swarmr.Base.Api;
using System.Diagnostics;

namespace Swarmr.Base.Tasks;

/// <summary>
/// Ingests a local file into the swarm.
/// </summary>
public record IngestFileTask(string Id, IngestFileRequest Request) : ISwarmTask
{
    public static IngestFileTask Create(IngestFileRequest request) => new(
        Id: Guid.NewGuid().ToString(),
        Request: request
        );

    public async Task RunAsync(Swarm context)
    {
        var sw = new Stopwatch();

        var localFile = new FileInfo(Request.LocalFilePath);
        if (!localFile.Exists) throw new Exception($"File does not exist (\"{Request.LocalFilePath}\").");

        // (1) TARGET
        var ownSwarmFile = await context.LocalSwarmFiles.TryReadAsync(logicalName: Request.Name);
        if (ownSwarmFile != null)
        {
            if (ownSwarmFile.Hash == Request.LocalFileHash)
            {
                AnsiConsole.WriteLine($"[IngestFileTask] skipping {localFile.FullName} ... already exists");
                return;
            }
            else
            {
                AnsiConsole.WriteLine($"[IngestFileTask][WARNING] replacing {localFile.FullName} ... different hash");
            }
        }
        var targetFile = context.LocalSwarmFiles.GetContentFileInfo(
            logicalName: Request.Name,
            fileName: localFile.Name
            );

        // (2) copy [SOURCE].zip to [TARGETDIR]/[HASH].zip
        var sourceStream = localFile.OpenRead();
        var targetStream = targetFile.OpenWrite();
        AnsiConsole.WriteLine($"[IngestFileTask] ingest {localFile.FullName} ...");
        sw.Restart();
        await sourceStream.CopyToAsync(targetStream);
        targetStream.Close();
        sourceStream.Close();
        sw.Stop();
        AnsiConsole.WriteLine($"[IngestFileTask] ingest {localFile.FullName} ... {sw.Elapsed}");

        // (3) write [TARGETDIR]/file.json
        var newSwarmFile = new SwarmFile(
            Created: DateTimeOffset.UtcNow,
            LogicalName: Request.Name,
            FileName: targetFile.Name,
            Hash: Request.LocalFileHash
            );
        await context.LocalSwarmFiles.WriteAsync(newSwarmFile);

        // (4) update self
        var newSelf = context.Self with
        {
            LastSeen = DateTimeOffset.UtcNow,
            Files = context.Self.Files.SetItem(newSwarmFile.LogicalName, newSwarmFile)
        };
        context.UpsertNode(newSelf);

        // (5) report available file
        if (context.TryGetPrimaryNode(out var primary))
        {
            await primary.Client.UpdateNodeAsync(newSelf);
        }

        // done
        AnsiConsole.WriteLine($"[IngestFileTask] registered swarm file {newSwarmFile.ToJsonString()}");
    }
}
