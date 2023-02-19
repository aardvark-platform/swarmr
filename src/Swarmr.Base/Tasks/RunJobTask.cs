using Spectre.Console;
using Swarmr.Base.Api;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace Swarmr.Base.Tasks;

/// <summary>
/// Runs a job.
/// </summary>
public record RunJobTask(string Id, RunJobRequest Request) : ISwarmTask
{
    public static RunJobTask Create(RunJobRequest request) => new(
        Id: Guid.NewGuid().ToString(),
        Request: request
        );

    public async Task RunAsync(Swarm context)
    {
        AnsiConsole.WriteLine($"[RunJobTask] starting job {Id}");
        AnsiConsole.WriteLine($"[RunJobTask] {DateTimeOffset.Now}");
        AnsiConsole.WriteLine($"[RunJobTask] {Request.ToJsonString()}");

        // (0) create temporary job directory
        var prefix = $"runs/{Id}";
        var logicalExeDir = $"{prefix}/exe";
        var logicalLogDir = $"{prefix}/log";

        var exeDir = context.LocalSwarmFiles.GetOrCreateDir(logicalLogDir);
        exeDir.Create();
        AnsiConsole.MarkupLine($"[[RunJobTask]][[Setup]] [green]created dir[/] {exeDir}");

        var logDir = context.LocalSwarmFiles.GetOrCreateDir(logicalExeDir);
        logDir.Create();
        AnsiConsole.MarkupLine($"[[RunJobTask]][[Setup]] [green]created dir[/] {logDir}");

        try
        {
            // (1) setup (extract swarm files int o job dir)
            {
                var setupFileNames = Request.Job.Setup ?? Array.Empty<string>();
                foreach (var ifn in setupFileNames)
                {
                    AnsiConsole.WriteLine($"[RunJobTask][Setup] {ifn}");
                    var swarmFile = await context.LocalSwarmFiles.TryReadAsync(name: ifn);
                    if (swarmFile != null)
                    {
                        AnsiConsole.WriteLine($"    {swarmFile.ToJsonString()}");
                        var source = context.LocalSwarmFiles.GetContentFile(swarmFile);

                        AnsiConsole.WriteLine($"    extracting {swarmFile.LogicalName} ...");
                        ZipFile.ExtractToDirectory(source.FullName, exeDir.FullName, overwriteFiles: true);
                        AnsiConsole.WriteLine($"    extracting {swarmFile.LogicalName} ... done");
                    }
                    else
                    {
                        throw new Exception(
                            $"Failed to find swarm file \"{ifn}\". " +
                            $"Error 54de4c9c-73c0-4ba1-9679-8b000087b2ac."
                            );
                    }
                }
            }

            // (2) execute command lines
            {
                var commandLines = Request.Job.Execute ?? Array.Empty<JobConfig.ExecuteItem>();
                var imax = commandLines.Count;
                for (var i = 0; i < imax; i++)
                {
                    var (exeRelPath, args) = commandLines[i];

                    var exe = new FileInfo(Path.Combine(exeDir.FullName, exeRelPath));
                    AnsiConsole.WriteLine($"[RunJobTask][Execute][{i+1}/{imax}] {exe.FullName} {args}");
                    try
                    {
                        var stdoutFile = context.LocalSwarmFiles.Create(logicalName: $"log/{Id}/log{i}.txt");
                        var stdoutStream = context.LocalSwarmFiles.GetContentFile(stdoutFile).Open(FileMode.Create, FileAccess.Write, FileShare.Read);

                        await Execute(exe, args, exeDir, stdoutStream);

                        stdoutStream.Close();
                        await context.LocalSwarmFiles.SetHashFromContentFile(stdoutFile);
                    }
                    catch (Exception e)
                    {
                        AnsiConsole.WriteLine($"[RunJobTask][Execute][{i + 1}/{imax}] ERROR: {e.Message}");
                    }
                }
            }

            // (3) collect result files
            {
                var collectPaths = Request.Job.Collect ?? Array.Empty<string>();
                var resultSwarmFile = new SwarmFile(
                    Created: DateTimeOffset.UtcNow,
                    LogicalName: Request.Job.Result,
                    FileName: Path.GetFileName(Request.Job.Result) + ".zip"
,
                    Hash: "replace after zip file has been created");
                var archiveFile = context.LocalSwarmFiles.GetContentFile(resultSwarmFile);
                {
                    var dir = archiveFile.Directory ?? throw new Exception(
                        $"Missing dir path in {archiveFile.FullName}. " +
                        $"Error 78620e49-cd24-4cc2-be0e-a2f491bf35a9."
                        );
                    if (!dir.Exists) dir.Create();
                }

                var zipStream = archiveFile.Open(FileMode.Create, FileAccess.ReadWrite);
                var zipArchive = new ZipArchive(zipStream, ZipArchiveMode.Update);

                try
                {
                    var imax = collectPaths.Count;
                    for (var i = 0; i < imax; i++)
                    {
                        var pathRel = collectPaths[i];
                        var path = Path.Combine(exeDir.FullName, collectPaths[i]);

                        AnsiConsole.WriteLine($"[RunJobTask][Collect][{i + 1}/{imax}] {path}");

                        if (File.Exists(path))
                        {
                            var file = new FileInfo(path);
                            AnsiConsole.WriteLine($"    FILE {file.FullName}"); 
                            var e = zipArchive.CreateEntry(pathRel);
                            var target = e.Open();
                            var source = file.OpenRead();
                            await source.CopyToAsync(target);
                            source.Close();
                            target.Close();

                        }
                        else if (Directory.Exists(path))
                        {
                            var dir = new DirectoryInfo(path);
                            AnsiConsole.WriteLine($"    DIR  {dir.FullName}");

                            var files = dir.EnumerateFiles("*", new EnumerationOptions()
                            {
                                RecurseSubdirectories = true,
                                MatchType = MatchType.Simple
                            });

                            var prefixLength = dir.FullName.Length + 1;
                            foreach (var file in files)
                            {
                                var entryName = file.FullName[prefixLength..];
                                AnsiConsole.MarkupLine($"         [dim]{entryName.EscapeMarkup()}[/]");
                                var e = zipArchive.CreateEntry(entryName);
                                var target = e.Open();
                                var source = file.OpenRead();
                                await source.CopyToAsync(target);
                                source.Close();
                                target.Close();
                            }
                        }
                        else
                        {
                            AnsiConsole.MarkupLine($"    [red]ERROR {path.EscapeMarkup()} does not exist[/]");
                        }
                    }
                }
                finally
                {
                    zipArchive.Dispose();
                    zipStream.Close();

                    AnsiConsole.WriteLine($"    compute hash ...");
                    var hash = await SwarmFile.ComputeHashAsync(archiveFile);
                    resultSwarmFile = resultSwarmFile with { Hash = hash };
                    AnsiConsole.WriteLine($"    compute hash ... {hash}");

                    await context.LocalSwarmFiles.WriteAsync(resultSwarmFile);
                    AnsiConsole.MarkupLine($"    created result swarm file [green]{resultSwarmFile.ToJsonString().EscapeMarkup()}[/]");
                }
            }
        }
        finally
        {
            // (4) cleanup
            AnsiConsole.WriteLine($"[RunJobTask][Cleanup]");
            AnsiConsole.WriteLine($"    DELETE {exeDir.FullName} ... ");
            exeDir.Delete(recursive: true);
            AnsiConsole.WriteLine($"    DELETE {exeDir.FullName} ... done");
        }
    }

    private static async Task Execute(FileInfo exe, string args, DirectoryInfo exeWorkingDir, Stream stdoutTarget)
    {
        var processStartInfo = new ProcessStartInfo
        {
            FileName = exe.FullName,
            Arguments = args,
            CreateNoWindow = true,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            WorkingDirectory = exeWorkingDir.FullName
        };

        var process = new Process
        {
            StartInfo = processStartInfo
        };

        var newProcessStarted = process.Start();
        AnsiConsole.WriteLine($"newProcessStarted = {newProcessStarted}");

        using var stdout = process.StandardOutput;
        //await stdout.BaseStream.CopyToAsync(stdoutTarget);
        _ = Task.Run(async () =>
        {
            AnsiConsole.WriteLine($"[STDOUT] attaching");
            while (true)
            {
                var line = await stdout.ReadLineAsync();
                if (line == null)
                {
                    AnsiConsole.WriteLine("[STDOUT] EOF");
                    return;
                }
                AnsiConsole.WriteLine($"[STDOUT] {line}");
                stdoutTarget.Write(Encoding.UTF8.GetBytes(line + '\n'));
                stdoutTarget.Flush();
            }
        });

        AnsiConsole.WriteLine($"[{DateTimeOffset.UtcNow}] awaiting exit");

        await process.WaitForExitAsync();

        if (process.ExitCode == 0)
        {
            AnsiConsole.MarkupLine($"[[{DateTimeOffset.UtcNow}]] [green]exit {process.ExitCode}[/]");
        }
        else
        {
            AnsiConsole.MarkupLine($"[[{DateTimeOffset.UtcNow}]] [red]exit {process.ExitCode}[/]");
        }
    }
}
