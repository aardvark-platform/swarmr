using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class JoinCommand : AsyncCommand<JoinCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("Hostname[dim]:PORT[/] of a swarm node you want to join. ")]
        [CommandArgument(0, "[HOST]")]
        public string? RemoteHost { get; init; }

        [Description(
            "Specifies the port to listen on.\n" +
            "If not specified, then a free port from the default port range is chosen. "
            )]
        [CommandOption("-p|--port <PORT>")]
        public int? ListenPort { get; init; }

        [Description("Local working directory. ")]
        [CommandOption("-w|--workdir <LOCALDIR>")]
        public string? Workdir { get; internal set; }

        [Description("Print additional information. ")]
        [CommandOption("-v|--verbose")]
        public bool Verbose { get; init; }

        [Description("Automatic yes to prompts. ")]
        [CommandOption("-y|--yes")]
        public bool Yes { get; init; }
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        if (settings.Workdir == null)
        {
            var config = await LocalConfig.LoadAsync();
            if (config.Workdir == null)
            {
                var wd = new DirectoryInfo(Info.DefaultWorkdir);
                if (!settings.Yes)
                {
                    var answer = AnsiConsole.Ask("Please specify a local workdir", wd.FullName);
                    wd = new DirectoryInfo(answer);
                }

                if (!wd.Exists) wd.Create();
                config = config with { Workdir = wd.FullName };
                await config.SaveAsync();
            }
        }

        var swarm = await Swarm.ConnectAsync(
            customRemoteHost: settings.RemoteHost,
            listenPort: settings.ListenPort,
            customWorkDir: settings.Workdir,
            verbose: settings.Verbose
            );


        var updatedSelf = swarm.Self.Upsert(swarm.LocalSwarmFiles.Files);
        await swarm.Primary.Client.UpdateNodeAsync(updatedSelf);

        var app = await Server.RunAsync(swarm: swarm);
        if (app == null) return 1;

        if (settings.Verbose)
        {
            AnsiConsole.WriteLine($"[Info] local config : {LocalConfig.LocalConfigPath}");
            AnsiConsole.WriteLine($"[Info] local workdir: {swarm.Workdir}");

            swarm.PrintNice();
        }
        else
        {
            AnsiConsole.MarkupLine("[green]connected[/]");
        }

        await app.WaitForShutdownAsync();

        return 0;
    }
}
