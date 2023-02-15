using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;

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

        [Description("Specifies the runner's runtime, e.g. win-x64, or linux-x64. ")]
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
            return 1;
        }

        var name = 
            settings.Name 
            ?? Path.GetFileNameWithoutExtension(settings.Path).ToLower()
        ;

        var file = new FileInfo(settings.Path);
        string hash = null!;
        Runner runner = null!;

        await AnsiConsole.Progress()
            .StartAsync(async ctx =>
            {
                var maxLength = Math.Min(file.Length, 256 * 1024 * 1024);
                var hashTask = ctx.AddTask("[green]computing hash[/]", maxValue: maxLength);
                var hashstream = new TruncateStream(file.OpenRead(), maxLength: maxLength, n => hashTask.Value = n);
                var sha256 = await SHA256.Create().ComputeHashAsync(hashstream);
                hash = Convert.ToHexString(sha256).ToLowerInvariant();
                hashstream.Close();
            });

        await AnsiConsole.Status()
            .StartAsync($"swarm is ingesting {file.Name} ...", async ctx =>
            {
                var client = new NodeHttpClient(settings.Url);
                runner = await client.RegisterRunnerAsync(
                    sourceFile: settings.Path,
                    sourceFileHash: hash,
                    name: name,
                    runtime: settings.Runtime
                    );
            });

        AnsiConsole.WriteLine(runner.ToJsonString());

        return 0;
    }
}
