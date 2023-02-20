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
        var jobDir = new DirectoryInfo(Path.Combine(context.Workdir, "runs", Id));
        var exeDir = new DirectoryInfo(Path.Combine(jobDir.FullName, "exe"));
        var logDir = new DirectoryInfo(Path.Combine(jobDir.FullName, "logs"));

        exeDir.Create();
        AnsiConsole.MarkupLine($"[[RunJobTask]][[Setup]] [green]created dir[/] {exeDir}");

        logDir.Create();
        AnsiConsole.MarkupLine($"[[RunJobTask]][[Setup]] [green]created dir[/] {logDir}");

        var resultZipArchive = context.LocalSwarmFiles.Create(
            logicalName: Request.Job.Result,
            fileName: Path.GetFileName(Request.Job.Result) + ".zip",
            force: true
            );

        try
        {
            // (1) setup (extract swarm files into job dir)
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
            var commandLines = Request.Job.Execute ?? Array.Empty<JobConfig.ExecuteItem>();
            var stdoutFiles = new FileInfo[commandLines.Count];
            {
                var imax = commandLines.Count;
                for (var i = 0; i < imax; i++)
                {
                    var (exeRelPath, args) = commandLines[i];

                    var exe = new FileInfo(Path.Combine(exeDir.FullName, exeRelPath));
                    AnsiConsole.WriteLine($"[RunJobTask][Execute][{i+1}/{imax}] {exe.FullName} {args}");
                    try
                    {
                        stdoutFiles[i] = new FileInfo(Path.Combine(logDir.FullName, $"stdout{i}.txt"));
                        var stdoutStream = stdoutFiles[i].Open(FileMode.Create, FileAccess.Write, FileShare.Read);

                        await Execute(exe, args, exeDir, stdoutStream);

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
                var collectPaths = Request.Job.Collect ?? Array.Empty<string>();

                var archiveFile = context.LocalSwarmFiles.GetContentFile(resultZipArchive);
                {
                    var dir = archiveFile.Directory ?? throw new Exception(
                        $"Missing dir path in {archiveFile.FullName}. " +
                        $"Error 78620e49-cd24-4cc2-be0e-a2f491bf35a9."
                        );
                    if (!dir.Exists) dir.Create();
                }

                var zipStream = archiveFile.Open(FileMode.Create, FileAccess.ReadWrite);
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
                    zipArchive.Dispose();
                    zipStream.Close();

                    AnsiConsole.WriteLine($"    compute hash ...");
                    var hash = await SwarmFile.ComputeHashAsync(archiveFile);
                    resultZipArchive = resultZipArchive with { Hash = hash };
                    AnsiConsole.WriteLine($"    compute hash ... {hash}");

                    await context.LocalSwarmFiles.WriteAsync(resultZipArchive);
                    AnsiConsole.MarkupLine($"    created result swarm file [green]{resultZipArchive.ToJsonString().EscapeMarkup()}[/]");
                }
            }
        }
        finally
        {
            // (4) cleanup
            AnsiConsole.WriteLine($"[RunJobTask][Cleanup]");
            AnsiConsole.WriteLine($"    DELETE {jobDir} ... ");
            jobDir.Delete(recursive: true);
            AnsiConsole.WriteLine($"    DELETE {jobDir} ... done");
        }

        if (resultZipArchive != null)
        {
            AnsiConsole.WriteLine($"[RunJobTask][Result] announce result {resultZipArchive.LogicalName} ...");
            context.UpsertNode(
                context.Self.UpsertFile(resultZipArchive)
                );

            await context.Primary.Client.UpdateNodeAsync(context.Self);
            AnsiConsole.WriteLine($"[RunJobTask][Result] announce result {resultZipArchive.LogicalName} ... DONE");
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
