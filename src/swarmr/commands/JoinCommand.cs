using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.Collections.Immutable;
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
        [DefaultValue(".swarmr")]
        public string WorkDir { get; init; } = null!;
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        var (remoteHost, remotePort) = SwarmUtils.ParseHost(settings.RemoteHost);

        ProbeResult? probe = null;

        // (1) Construct the 'remoteUrl', which is
        // - the URL of a running node, which we will use to join the swarm.
        // - or null, which will create a new swarm (with ourselve as only node).
        if (remotePort == null)
        {
            // We don't care about a specific port!

            // Let's try to auto-detect a live port.
            // If no specific host is specified either,
            // then we try localhost by default.
            var probeHost = remoteHost ?? "localhost"; 

            probe = await SwarmUtils.ProbeHostAsync(probeHost);
            if (probe.TryGetLivePort(out remotePort)) remoteHost = probe.Hostname;
        }

        var remoteUrl = (remoteHost, remotePort) switch
        {
            (null, null) => null,
            (string host, int port) => $"http://{host}:{port}",
            _ => throw new Exception($"Error f61e449b-dac9-40b7-b55a-cd7cd235e758.")
        };

        // (2) Determine the port, which our node will listen on.
        var listenPort = settings.ListenPort;

        if (!listenPort.HasValue)
        {
            // No specific port has been specified by the user.
            // Let's choose an available port from the default port range.
            if (probe?.Hostname != "localhost")
                probe = await SwarmUtils.ProbeHostAsync("localhost");

            if (!probe.TryGetFreePort(out listenPort))
            {
                throw new Exception("No port available.");
            }
        }


        var hostname = Environment.MachineName.ToLowerInvariant();
        var myself = new Node(
            Id: Guid.NewGuid().ToString(),
            Created: DateTimeOffset.UtcNow,
            LastSeen: DateTimeOffset.UtcNow,
            Hostname: hostname,
            Port: listenPort.Value,
            AvailableRunners: ImmutableDictionary<string, Runner>.Empty
            );

        var swarm = await Swarm.ConnectAsync(
                url: remoteUrl,
                self: myself,
                workdir: settings.WorkDir
                );

        var app = await Server.RunAsync(swarm: swarm, port: listenPort.Value);
        if (app == null) return 1;

        swarm.PrintNice();


        await app.WaitForShutdownAsync();

        return 0;
    }
}
