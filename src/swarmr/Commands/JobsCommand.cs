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
        var jobConfig = Jobs.Parse(File.ReadAllText(settings.JobConfigFile));

        var swarm = await Swarm.ConnectAsync(
            customRemoteHost: null,
            listenPort: null,
            customWorkDir: null,
            verbose: settings.Verbose
            );

        var response = await swarm.Primary.Client.SubmitJobAsync(jobConfig);
        AnsiConsole.WriteLine(response.JobId.ToJsonString());

        await swarm.LeaveSwarmAsync(swarm.Self);

        return 0;
    }
}
