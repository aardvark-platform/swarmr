using Spectre.Console.Cli;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class JobDataAddCommand : AsyncCommand<JobDataAddCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("Data is intended for this runner. ")]
        [CommandArgument(0, "<RUNNER>")]
        public string RunnerName { get; init; } = null!;

        [Description("Data file to add. ")]
        [CommandArgument(1, "<FILE>")]
        public string DataFilePath { get; init; } = null!;

        [Description("Specifies custom data name. By default the filename is used. ")]
        [CommandOption("-n|--name <NAME>")]
        public string? Name { get; init; }
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        await Task.Delay(0);
        throw new NotImplementedException();
    }
}
