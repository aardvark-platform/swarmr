using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class JobsSubmitCommand : AsyncCommand<JobsSubmitCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("Job description file. ")]
        [CommandArgument(0, "[FILE]")]
        public string JobConfigFile { get; init; } = null!;

        [Description("Print additional information. ")]
        [CommandOption("-v|--verbose")]
        public bool Verbose { get; init; }
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        // (1) parse job description file
        var jobConfig = Jobs.Parse(File.ReadAllText(settings.JobConfigFile));

        // (2) submit job to primary node
        var swarm = await SwarmUtils.GetClientSwarm(settings.Verbose);
        var response = await swarm.Primary.Client.SubmitJobAsync(jobConfig);

        // (3) tear down
        AnsiConsole.WriteLine(response.ToJsonString());
        await swarm.LeaveSwarmAsync(swarm.Self);
        return 0;
    }
}
