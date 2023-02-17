using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class JobsSubmitCommand : AsyncCommand<JobsSubmitCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description(
            "Job description file. Optional.\n" +
            "Use this instead of specifying everything with command line arguments.\n" +
            "Any additional command line arguments will replace (-r, -c, --priority)\n" +
            "or extend (-i, -o) the values from the file. "
            )]
        [CommandArgument(0, "[FILE]")]
        public string? JobConfigFile { get; init; }

        [Description("Specifies the runner for this job. ")]
        [CommandOption("-r|--runner <RUNNER>")]
        public string? RunnerName { get; init; }

        [Description(
            "Specifies the command line to be executed.\n" +
            "Don't forget to properly escape quotes, e.g.:\n" +
            "  -c \"start.exe \\\"path/with whitespace/data.txt\\\"\"" +
            "As an alternative you can provide a .json file instead of arguments, see . "
            )]
        [CommandOption("-c|--command <COMMANDLINE>")]
        public string? CommandLine { get; init; }

        [Description(
            "Specifies data to provide for this job.\n" +
            "The name refers to data that has been added with \"data add <RUNNER> <FILE>\".\n" +
            "Multiple allowed. "
            )]
        [CommandOption("-i|--input <NAME>")]
        public string[]? InputData { get; init; }

        [Description("Specifies a file or directory to collect after the job has finished.\nMultiple allowed. ")]
        [CommandOption("-o|--output <PATH>")]
        public string[]? OutputData { get; init; }

        [Description("Specifies worker process priority (low, normal). ")]
        [CommandOption("--priority <PRIORITY>")]
        [DefaultValue("low")]
        public string? Priority { get; init; }
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        await Task.Delay(0);
        AnsiConsole.WriteLine(settings.ToJsonString());
        throw new NotImplementedException();
    }
}
