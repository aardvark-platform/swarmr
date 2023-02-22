using Spectre.Console;
using Swarmr.Base.Api;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace Swarmr.Base.Tasks;

/// <summary>
/// Runs a job.
/// </summary>
public record RunJobTask(string Id, JobConfig Job) : ISwarmTask
{
    public static RunJobTask Create(JobConfig job) => new(
        Id: $"RunJobTask-{Guid.NewGuid()}",
        Job: job
        );

    public async Task RunAsync(Swarm context)
    {
        ////////////////////////////////
        // announce busy
        var newSelf = context.UpsertNode(context.Self with
        {
            LastSeen = DateTime.UtcNow,
            Status = NodeStatus.Busy
        });
        await context.Primary.Client.UpdateNodeAsync(newSelf);
        AnsiConsole.MarkupLine($"[lime] updated Self.Status to {context.Self.Status}[/]");

        ////////////////////////////////
        // process job
        _ = ProcessJobAsync(context)
            .ContinueWith(async t =>
            {
                ////////////////////////////////
                // announce idle
                var newSelf = context.UpsertNode(context.Self with
                {
                    LastSeen = DateTime.UtcNow,
                    Status = NodeStatus.Idle
                });
                await context.Primary.Client.UpdateNodeAsync(newSelf);
                AnsiConsole.MarkupLine($"[lime] updated Self.Status to {context.Self.Status}[/]");
            });
    }

    private async Task ProcessJobAsync(Swarm context)
    {
        AnsiConsole.WriteLine($"[RunJobTask] starting job {Id}");
        AnsiConsole.WriteLine($"[RunJobTask] {DateTimeOffset.Now}");
        AnsiConsole.WriteLine($"[RunJobTask] {Job.ToJsonString()}");

        // (0) create temporary job directory
        var jobDir = new DirectoryInfo(Path.Combine(context.Workdir, "runs", Id));
        var exeDir = new DirectoryInfo(Path.Combine(jobDir.FullName, "exe"));
        var logDir = new DirectoryInfo(Path.Combine(jobDir.FullName, "logs"));

        exeDir.Create();
        AnsiConsole.MarkupLine($"[[RunJobTask]][[Setup]] [green]created dir[/] {exeDir}");

        logDir.Create();
        AnsiConsole.MarkupLine($"[[RunJobTask]][[Setup]] [green]created dir[/] {logDir}");

        try
        {
            // (1) setup (extract swarm files into job dir)
            {
                var setupFileNames = Job.Setup ?? Array.Empty<string>();
                foreach (var ifn in setupFileNames)
                {
                    AnsiConsole.WriteLine($"[RunJobTask][Setup] {ifn}");
                    var swarmFile = await context.LocalSwarmFiles.TryReadAsync(logicalName: ifn);
                    if (swarmFile != null)
                    {
                        try
                        {
                            AnsiConsole.WriteLine($"    {swarmFile.ToJsonString()}");
                            var source = context.LocalSwarmFiles.GetContentFileInfo(swarmFile);

                            await using var swarmFileLock = await context.LocalSwarmFiles.GetLockAsync(swarmFile, "ProcessJobAsync setup");

                            AnsiConsole.WriteLine($"    extracting {swarmFile.LogicalName} ...");
                            ZipFile.ExtractToDirectory(source.FullName, exeDir.FullName, overwriteFiles: true);
                            AnsiConsole.WriteLine($"    extracting {swarmFile.LogicalName} ... done");
                        }
                        catch (Exception e)
                        {
                            AnsiConsole.MarkupLine($"[red][[ERROR]] {e.ToString().EscapeMarkup()}[/]");
                        }
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
            var commandLines = Job.Execute ?? Array.Empty<JobConfig.ExecuteItem>();
            var stdoutFiles = new FileInfo[commandLines.Count];
            {
                var imax = commandLines.Count;
                for (var i = 0; i < imax; i++)
                {
                    var (exeRelPath, args) = commandLines[i];

                    var exe = new FileInfo(Path.Combine(exeDir.FullName, exeRelPath));
                    AnsiConsole.WriteLine($"[RunJobTask][Execute][{i + 1}/{imax}] {exe.FullName} {args}");
                    try
                    {
                        stdoutFiles[i] = new FileInfo(Path.Combine(logDir.FullName, $"stdout{i}.txt"));
                        var stdoutStream = stdoutFiles[i].Open(FileMode.Create, FileAccess.Write, FileShare.Read);

                        await ExecuteAsync(exe, args, exeDir, stdoutStream);

                        stdoutStream.Close();
                    }
                    catch (Exception e)
                    {
                        AnsiConsole.WriteLine($"[RunJobTask][Execute][{i + 1}/{imax}] ERROR: {e.Message}");
                    }
                }
            }

            // (3) collect result files
            {
                var collectPaths = Job.Collect ?? Array.Empty<string>();

                var resultZip = SwarmFile.Create(
                    logicalName: Job.Result,
                    fileName: Path.GetFileName(Job.Result) + ".zip"
                    );

                await using var resultZipLock = await context.LocalSwarmFiles.GetLockAsync(resultZip, label: "ProcessJobAsync");

                var resultZipFileInfo = context.LocalSwarmFiles.GetContentFileInfo(resultZip);
                {
                    var dir = resultZipFileInfo.Directory ?? throw new Exception(
                        $"Missing dir path in {resultZipFileInfo.FullName}. " +
                        $"Error 78620e49-cd24-4cc2-be0e-a2f491bf35a9."
                        );
                    if (!dir.Exists) dir.Create();
                }

                var zipStream = resultZipFileInfo.Open(FileMode.Create, FileAccess.ReadWrite);
                var zipArchive = new ZipArchive(zipStream, ZipArchiveMode.Update);

                async Task addToZip(FileInfo source, string zipEntryName)
                {
                    var e = zipArchive.CreateEntry(zipEntryName);
                    var toStream = e.Open();
                    var fromStream = source.OpenRead();
                    await fromStream.CopyToAsync(toStream);
                    fromStream.Close();
                    toStream.Close();
                }

                try
                {
                    // stdout log(s)
                    foreach (var stdoutFile in stdoutFiles)
                    {
                        await addToZip(stdoutFile, $"___job-{Id}/{stdoutFile.Name}");
                    }

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
                            await addToZip(file, pathRel);
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
                                await addToZip(file, entryName);
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
                    try
                    {
                        zipArchive.Dispose();
                        zipStream.Close();

                        AnsiConsole.WriteLine($"    closed {resultZipFileInfo}");

                        AnsiConsole.WriteLine($"    compute hash ...");
                        resultZip = await context.LocalSwarmFiles.SetHashFromContentFile(resultZip);
                        AnsiConsole.WriteLine($"    compute hash ... {resultZip.Hash}");

                        await context.LocalSwarmFiles.WriteAsync(resultZip);
                        AnsiConsole.MarkupLine($"    created result swarm file [green]{resultZip.ToJsonString().EscapeMarkup()}[/]");

                        context.UpsertNode(
                            context.Self.UpsertFile(resultZip)
                            );
                        AnsiConsole.MarkupLine($"    announced new node file [green]{resultZip.LogicalName}[/]");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Environment.Exit(1);
                    }
                }
            }
        }
        finally
        {
            // (4) cleanup
            AnsiConsole.WriteLine($"[RunJobTask][Cleanup]");
            AnsiConsole.WriteLine($"    DELETE {jobDir} ... ");
            //jobDir.Delete(recursive: true);
            AnsiConsole.WriteLine($"    DELETE {jobDir} ... done");
        }
    }

    private static async Task ExecuteAsync(FileInfo exe, string args, DirectoryInfo exeWorkingDir, Stream stdoutTarget)
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
