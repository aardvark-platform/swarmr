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

        var existingSwarmFile = new FileInfo(Request.LocalFilePath);
        if (!existingSwarmFile.Exists) throw new Exception($"File does not exist (\"{Request.LocalFilePath}\").");

        // (1) TARGET
        var targetDir = context.GetSwarmFileDir(Request.Name);
        var ownSwarmFile = await context.TryReadSwarmFileAsync(Request.Name);
        if (ownSwarmFile != null)
        {
            if (ownSwarmFile.Hash == Request.LocalFileHash)
            {
                AnsiConsole.WriteLine($"[IngestFileTask] skipping {existingSwarmFile.FullName} ... already exists");
                return;
            }
            else
            {
                AnsiConsole.WriteLine($"[IngestFileTask][WARNING] replacing {existingSwarmFile.FullName} ... different hash");
            }
        }
        if (!targetDir.Exists) targetDir.Create();
        var targetFileName = existingSwarmFile.Name;
        var targetFile = new FileInfo(Path.Combine(targetDir.FullName, targetFileName));

        // (2) copy [SOURCE].zip to [TARGETDIR]/[HASH].zip
        var sourceStream = existingSwarmFile.OpenRead();
        var targetStream = targetFile.OpenWrite();
        AnsiConsole.WriteLine($"[IngestFileTask] ingest {existingSwarmFile.FullName} ...");
        sw.Restart();
        await sourceStream.CopyToAsync(targetStream);
        targetStream.Close();
        sourceStream.Close();
        sw.Stop();
        AnsiConsole.WriteLine($"[IngestFileTask] ingest {existingSwarmFile.FullName} ... {sw.Elapsed}");

        // (3) write [TARGETDIR]/file.json
        var newSwarmFile = new SwarmFile(
            Created: DateTimeOffset.UtcNow,
            Name: Request.Name,
            Hash: Request.LocalFileHash,
            FileName: targetFile.Name
            );
        await context.WriteSwarmFileAsync(newSwarmFile);

        // (4) delete old version(s)
        foreach (var info in targetDir.EnumerateFileSystemInfos())
        {
            if (info.Name == "file.json") continue;
            if (info.Name == targetFile.Name) continue;
            info.Delete();
            AnsiConsole.WriteLine($"[IngestFileTask] deleted {info.FullName}");
        }

        // (5) update self
        var newSelf = context.Self with
        {
            LastSeen = DateTimeOffset.UtcNow,
            SwarmFiles = context.Self.SwarmFiles.SetItem(newSwarmFile.Name, newSwarmFile)
        };
        context.UpsertNode(newSelf);

        // (6) report available file
        if (context.TryGetPrimaryNode(out var primary))
        {
            await primary.Client.UpdateNodeAsync(newSelf);
        }

        // done
        AnsiConsole.WriteLine($"[IngestFileTask] registered swarm file {newSwarmFile.ToJsonString()}");
    }
}
