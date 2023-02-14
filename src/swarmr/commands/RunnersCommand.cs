using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class RunnersRegisterCommand : AsyncCommand<RunnersRegisterCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("Registers <FILE> as a runner. ")]
        [CommandArgument(0, "<FILE>")]
        public string Path { get; init; } = null!;

        [Description("Specifies custom runner name. By default the filename is used. ")]
        [CommandOption("-n|--name <NAME>")]
        public string? Name { get; init; }

        [Description("Specifies custom runner name. By default the filename is used. ")]
        [CommandOption("-r|--runtime <RUNTIME_IDENTIFIER>")]
        [DefaultValue("win-x64")]
        public string Runtime { get; init; } = null!;

        [Description($"Register with node at <URL>. ")]
        [CommandOption("--url <URL>")]
        [DefaultValue(Info.DefaultLocalNodeUrl)]
        public string Url { get; init; } = null!;
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        if (Path.GetExtension(settings.Path).ToLower() != ".zip")
        {
            AnsiConsole.WriteLine($"Expected .zip file (instead of \"{settings.Path}\").");
        }

        var name = 
            settings.Name 
            ?? Path.GetFileNameWithoutExtension(settings.Path).ToLower()
            ;

        var client = new NodeHttpClient(settings.Url);

        var runner = await client.RegisterRunnerAsync(
            source: settings.Path,
            name: name,
            runtime: settings.Runtime
            );

        AnsiConsole.WriteLine(runner.ToJsonString());

        return 0;
    }
}
