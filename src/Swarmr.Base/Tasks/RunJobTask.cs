using Spectre.Console;
using Swarmr.Base.Api;
using System.Diagnostics;
using System.IO.Compression;

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
        var jobDir = new DirectoryInfo(Path.Combine(context.Workdir, "tmp", Id));

        if (jobDir.Exists)
        {
            AnsiConsole.MarkupLine($"[[RunJobTask]][[Setup]] [yellow]job dir already exists[/] {jobDir}");
        }
        else
        {
            jobDir.Create();
            AnsiConsole.MarkupLine($"[[RunJobTask]][[Setup]] [green]created job dir[/] {jobDir}");
        }

        try
        {
            // (1) setup (extract swarm files int o job dir)
            {
                var setupFileNames = Request.Job.Setup ?? Array.Empty<string>();
                foreach (var ifn in setupFileNames)
                {
                    AnsiConsole.WriteLine($"[RunJobTask][Setup] {ifn}");
                    var swarmFile = await context.TryReadSwarmFileAsync(ifn);
                    if (swarmFile != null)
                    {
                        AnsiConsole.WriteLine($"    {swarmFile.ToJsonString()}");
                        var source = context.GetSwarmFilePath(swarmFile);

                        AnsiConsole.WriteLine($"    extracting {swarmFile.Name} ...");
                        ZipFile.ExtractToDirectory(source.FullName, jobDir.FullName, overwriteFiles: true);
                        AnsiConsole.WriteLine($"    extracting {swarmFile.Name} ... done");
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

                    var exe = new FileInfo(Path.Combine(jobDir.FullName, exeRelPath));
                    AnsiConsole.WriteLine($"[RunJobTask][Execute][{i+1}/{imax}] {exe.FullName} {args}");
                    try
                    {
                        await Execute(exe, args, jobDir);
                    }
                    catch (Exception e)
                    {
                        AnsiConsole.WriteLine($"[RunJobTask][Execute][{i + 1}/{imax}] ERROR: {e.Message}");
                    }
                }
            }

            // (3) collect result files
            {
                //var z = new ZipArchive(null, ZipArchiveMode.Update);
                //var e = z.CreateEntry("foo");
                //var writestream = e.Open();

                var collectPaths = Request.Job.Collect ?? Array.Empty<JobConfig.CollectItem>();
                var imax = collectPaths.Count;
                for (var i = 0; i < imax; i++)
                {
                    var (path, swarmfile) = collectPaths[i];
                    path = Path.Combine(jobDir.FullName, path);

                    AnsiConsole.WriteLine($"[RunJobTask][Collect][{i + 1}/{imax}] {path} {swarmfile}");

                    if (File.Exists(path))
                    {
                        AnsiConsole.WriteLine($"    FILE {path}");
                    }
                    else if (Directory.Exists(path))
                    {
                        AnsiConsole.WriteLine($"    DIR  {path}");
                    }
                    else
                    {
                        AnsiConsole.MarkupLine($"    [red]ERROR {path.EscapeMarkup()} does not exist[/]");
                    }
                }
            }
        }
        finally
        {
            // (4) cleanup
            AnsiConsole.WriteLine($"[RunJobTask][CLEANUP]");
            AnsiConsole.WriteLine($"    DELETE {jobDir.FullName} ... ");
            jobDir.Delete(recursive: true);
            AnsiConsole.WriteLine($"    DELETE {jobDir.FullName} ... done");
        }
    }

    private static async Task Execute(FileInfo exe, string args, DirectoryInfo dir)
    {
        var processStartInfo = new ProcessStartInfo
        {
            FileName = exe.FullName,
            Arguments = args,
            CreateNoWindow = true,
            UseShellExecute = false,
            RedirectStandardOutput = true, 
            WorkingDirectory = dir.FullName
        };

        var process = new Process
        {
            StartInfo = processStartInfo
        };
        var newProcessStarted = process.Start();
        AnsiConsole.WriteLine($"newProcessStarted = {newProcessStarted}");

        using var stdout = process.StandardOutput;
        using var consoleout = Console.OpenStandardOutput();
        await stdout.BaseStream.CopyToAsync(consoleout);
        AnsiConsole.WriteLine($"attached stdout");

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
