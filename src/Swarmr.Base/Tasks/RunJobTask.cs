using Spectre.Console;
using Swarmr.Base.Api;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace Swarmr.Base.Tasks;

/// <summary>
/// Runs a job.
/// </summary>
public record RunJobTask(string Id, Job Job) : ISwarmTask
{
    public static RunJobTask Create(Job job) => new(
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

        context.JobPool.UpsertJob(
            Job.WithStart(context.SelfId)
            );


        ///////////////////////////////////////
        // announce started job to primary
        var startedJob = Job.WithStart(startedOnNodeId: context.SelfId);
        AnsiConsole.MarkupLine($"[lime] announce started job to primary ({context.PrimaryId}) {startedJob.ToJsonString().EscapeMarkup()}[/]");
        await context.Primary.Client.UpsertJobAsync(startedJob);
        AnsiConsole.MarkupLine($"[lime] announce started job to primary ({context.PrimaryId}) ... DONE[/]");

        ///////////////////////////////////////
        // process job
        _ = ProcessJobAsync(context)
            .ContinueWith(async t => {
                try {
                    AnsiConsole.MarkupLine($"[lime] ContinueWith({t.Status})[/]");

                    ///////////////////////////////////////
                    // announce finished job to primary
                    var finishedJob = t.Status switch {
                        TaskStatus.RanToCompletion => startedJob.WithSucceeded(),
                        TaskStatus.Faulted => startedJob.WithFailed(t.Exception!),
                        TaskStatus.Canceled => startedJob.WithFailed(new Exception("Canceled. Error be36d5d3-7af1-45b1-8629-b4a2471858b2.")),
                        _ => throw new Exception($"Unexpected {t.Status}. Error ab86370e-ae2b-4979-aed0-8b471d4a269e.")
                    };
                    AnsiConsole.MarkupLine($"[lime] announce finished job to primary ({context.PrimaryId}) {finishedJob.ToJsonString().EscapeMarkup()}[/]");
                    await context.Primary.Client.UpsertJobAsync(finishedJob);
                    AnsiConsole.MarkupLine($"[lime] announce finished job to primary ({context.PrimaryId}) ... DONE[/]");

                    ////////////////////////
                    // announce idle
                    var newSelf = context.UpsertNode(context.Self with {
                        LastSeen = DateTime.UtcNow,
                        Status = NodeStatus.Idle
                    });
                    await context.Primary.Client.UpdateNodeAsync(newSelf);
                    AnsiConsole.MarkupLine($"[lime] updated Self.Status to {context.Self.Status}[/]");
                }
                catch (Exception e) {
                    AnsiConsole.MarkupLine($"[red] {e.ToString().EscapeMarkup()}[/]");
                }
            });
    }

    private async Task ProcessJobAsync(Swarm context)
    {
        AnsiConsole.WriteLine($"[RunJobTask] starting job {Id}");
        AnsiConsole.WriteLine($"[RunJobTask] {DateTimeOffset.Now}");
        AnsiConsole.WriteLine($"[RunJobTask] {Job.ToJsonString()}");

        // (0) create temporary job directory
        var jobDir = new DirectoryInfo(Path.Combine(context.Workdir.FullName, "runs", Id));
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
                var setupFileNames = Job.Config.Setup ?? Array.Empty<string>();
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
            var commandLines = Job.Config.Execute ?? Array.Empty<JobDescription.ExecuteItem>();
            var stdoutFiles = new FileInfo[commandLines.Count];
            var stderrFiles = new FileInfo[commandLines.Count];
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
                        stderrFiles[i] = new FileInfo(Path.Combine(logDir.FullName, $"stderr{i}.txt"));
                        var stdoutStream = stdoutFiles[i].Open(FileMode.Create, FileAccess.Write, FileShare.Read);
                        var stderrStream = stderrFiles[i].Open(FileMode.Create, FileAccess.Write, FileShare.Read);
                        await ExecuteAsync(exe, args, exeDir, stdoutStream, stderrStream);

                        stdoutStream.Close();
                        stderrStream.Close();
                    }
                    catch (Exception e)
                    {
                        AnsiConsole.WriteLine($"[RunJobTask][Execute][{i + 1}/{imax}] ERROR: {e.Message}");
                    }
                }
            }

            // (3) collect result files
            {
                var collectPaths = Job.Config.Collect ?? Array.Empty<string>();

                var resultZip = SwarmFile.Create(
                    logicalName: Job.Config.Result,
                    fileName: Path.GetFileName(Job.Config.Result) + ".zip"
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

                    // stderr log(s)
                    foreach (var stderrFile in stderrFiles)
                    {
                        await addToZip(stderrFile, $"___job-{Id}/{stderrFile.Name}");
                    }

                    // collect files/directories
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

                            var prefixLength = exeDir.FullName.Length + 1;
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

                        var newSelf = context.Self.UpsertFile(resultZip);
                        context.UpsertNode(newSelf);
                        await context.Primary.Client.UpdateNodeAsync(newSelf);

                        AnsiConsole.MarkupLine($"    announced new swarm file [green]{resultZip.LogicalName}[/]");
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

    private static async Task ExecuteAsync(FileInfo exe, string args, DirectoryInfo exeWorkingDir, Stream stdoutTarget, Stream stderrTarget)
    {
        var processStartInfo = new ProcessStartInfo
        {
            FileName = exe.FullName,
            Arguments = args,
            CreateNoWindow = true,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = exeWorkingDir.FullName
        };

        var process = new Process
        {
            StartInfo = processStartInfo
        };

        var newProcessStarted = process.Start();
        AnsiConsole.WriteLine($"newProcessStarted = {newProcessStarted}");

        void outputDataReceived(object sender, DataReceivedEventArgs args) {
            try {
                var line = args.Data;
                if (line == null) {
                    AnsiConsole.WriteLine("[STDOUT] EOF");
                    return;
                }
                AnsiConsole.WriteLine($"[STDOUT] {line}");
                stdoutTarget.Write(Encoding.UTF8.GetBytes(line + '\n'));
                stdoutTarget.Flush();
            }
            catch (Exception e) {
                AnsiConsole.MarkupLine($"[[STDOUT]] [red]{e.Message}[/]");
            }
        }

        AnsiConsole.WriteLine($"[STDOUT] attaching");
        process.OutputDataReceived += outputDataReceived;
        process.BeginOutputReadLine();

        void errorDataReceived(object sender, DataReceivedEventArgs args)
        {
            try {
                var line = args.Data;
                if (line == null) {
                    AnsiConsole.WriteLine("[STDERR] EOF");
                    return;
                }
                AnsiConsole.WriteLine($"[STDERR] {line}");
                stderrTarget.Write(Encoding.UTF8.GetBytes(line + '\n'));
                stderrTarget.Flush();
            }
            catch (Exception e) {
                AnsiConsole.MarkupLine($"[[STDERR]] [red]{e.Message}[/]");
            }
        }

        AnsiConsole.WriteLine($"[STDERR] attaching");
        process.ErrorDataReceived += errorDataReceived;
        process.BeginErrorReadLine();

        AnsiConsole.WriteLine($"[{DateTimeOffset.UtcNow}] awaiting exit");

        await process.WaitForExitAsync();

        process.CancelOutputRead();
        process.CancelErrorRead();

        //process.OutputDataReceived -= outputDataReceived;
        //process.ErrorDataReceived -= errorDataReceived;

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
