using Spectre.Console;
using Swarmr.Base.Api;
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
        AnsiConsole.WriteLine($"[RunJobTask] {Request.ToJsonString()}");

        // create tmp directory for job execution
        var jobDir = new DirectoryInfo(Path.Combine(context.Workdir, "tmp", Id));

        if (jobDir.Exists)
        {
            AnsiConsole.WriteLine($"[RunJobTask] job dir already exists: {jobDir}");
        }
        else
        {
            jobDir.Create();
            AnsiConsole.WriteLine($"[RunJobTask] created job dir {jobDir}");
        }

        try
        {

            if (Request.InputFileNames != null)
            {
                foreach (var ifn in Request.InputFileNames)
                {
                    AnsiConsole.WriteLine($"[RunJobTask][InputFileName] {ifn}");
                    var swarmFile = await context.TryReadSwarmFileAsync(ifn);
                    if (swarmFile != null)
                    {
                        AnsiConsole.WriteLine($"[RunJobTask][InputFileName] {ifn}: {swarmFile.ToJsonString()}");
                        var source = context.GetSwarmFilePath(swarmFile);

                        AnsiConsole.WriteLine($"[RunJobTask][InputFileName] unpacking {swarmFile.Name} ...");
                        ZipFile.ExtractToDirectory(source.FullName, jobDir.FullName, overwriteFiles: true);
                        AnsiConsole.WriteLine($"[RunJobTask][InputFileName] unpacking {swarmFile.Name} ... done");
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

            AnsiConsole.WriteLine($"[RunJobTask] NOT IMPLEMENTED");
        }
        finally
        {
            AnsiConsole.WriteLine($"[RunJobTask][CLEANUP] delete {jobDir.FullName} ... ");
            //jobDir.Delete(recursive: true);
            AnsiConsole.WriteLine($"[RunJobTask][CLEANUP] delete {jobDir.FullName} ... done");
        }
    }
}
