using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using Swarmr.Base.Api;
using Swarmr.Base.Tasks;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Security.Cryptography;

namespace swarmr.Commands;

public class IngestCommand : AsyncCommand<IngestCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("Ingests <FILE>. ")]
        [CommandArgument(0, "<FILE>")]
        public string Path { get; init; } = null!;

        [Description("Specifies custom name. By default the filename is used. ")]
        [CommandOption("-n|--name <NAME>")]
        public string? Name { get; init; }
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        var file = new FileInfo(settings.Path);

        if (file.Extension.ToLower() != ".zip")
        {
            AnsiConsole.WriteLine($"Expected .zip file (instead of \"{file.FullName}\").");
            return 1;
        }

        var swarm = await SwarmUtils.TryGetLocalNodeAsync();
        if (swarm == null)
        {
            AnsiConsole.WriteLine("Error. Failed to connect to local swarm node.");
            AnsiConsole.WriteLine("Please start a swarmr node on localhost!");
            return 1;
        }

        var name = 
            settings.Name 
            ?? Path.GetFileNameWithoutExtension(file.FullName).ToLower()
        ;

        string hash = null!;
       
        await AnsiConsole.Progress()
            .StartAsync(async ctx =>
            {
                var hashTask = ctx.AddTask("[green]computing hash[/]", maxValue: 1.0);
                var s = 1.0 / file.Length;
                await SwarmFile.ComputeHashAsync(file, n => hashTask.Value = n * s);
            });

        var result = await swarm.IngestFileAsync(
            localFilePath: file.FullName,
            localFileHash: hash,
            name: name
            );

        AnsiConsole.WriteLine(result.ToJsonString());

        return 0;
    }
}
