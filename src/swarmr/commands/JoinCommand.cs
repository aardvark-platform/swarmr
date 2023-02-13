using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace swarmr.Commands;

public class JoinCommand : AsyncCommand<JoinCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("URL of a node in the swarm you want to join. ")]
        [CommandArgument(0, "[URL]")]
        public string? Url { get; init; }

        [Description("Specifies the port to listen on.")]
        [CommandOption("-p|--port <PORT>")]
        [DefaultValue(Info.DefaultPort)]
        public int Port { get; init; }
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        var swarm = await Swarm.ConnectAsync(
            url: settings.Url,
            portToListenOn: settings.Port
            );

        var swarmPanel = new Panel(swarm.ToJsonString().EscapeMarkup()).Header("Swarm");
        AnsiConsole.Write(swarmPanel);

        await Server.RunAsync(swarm, port: settings.Port);

        return 0;
    }
}
