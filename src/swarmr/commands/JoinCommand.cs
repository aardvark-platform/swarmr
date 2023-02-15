using Spectre.Console.Cli;
using Swarmr.Base;
using System.Collections.Immutable;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class JoinCommand : AsyncCommand<JoinCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("URL of a node in the swarm you want to join. ")]
        [CommandArgument(0, "[URL]")]
        public string? Url { get; init; }

        [Description("Specifies the port to listen on. ")]
        [CommandOption("-p|--port <PORT>")]
        [DefaultValue(Info.DefaultPort)]
        public int Port { get; init; }

        [Description("Local working directory. ")]
        [CommandOption("-w|--workdir <LOCALDIR>")]
        [DefaultValue(".swarmr")]
        public string WorkDir { get; init; } = null!;
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        var hostname = Environment.MachineName.ToLowerInvariant();
        
        var myself = new Node(
            Id: Guid.NewGuid().ToString(),
            Created: DateTimeOffset.UtcNow,
            LastSeen: DateTimeOffset.UtcNow,
            Hostname: hostname,
            Port: settings.Port,
            ConnectUrl: $"http://{hostname}:{settings.Port}",
            AvailableRunners: ImmutableDictionary<string, Runner>.Empty
            );

        var swarm = await Swarm.ConnectAsync(
            self: myself,
            url: settings.Url,
            workdir: settings.WorkDir
            );

        swarm.PrintNice();

        await Server.RunAsync(swarm: swarm, port: settings.Port);

        return 0;
    }
}
