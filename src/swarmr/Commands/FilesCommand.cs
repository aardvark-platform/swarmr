using Spectre.Console;
using Spectre.Console.Cli;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class FilesListCommand : AsyncCommand<FilesListCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("Print additional information. ")]
        [CommandOption("-v|--verbose")]
        public bool Verbose { get; init; }

    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        var swarm = await Swarm.ConnectAsync(
            customRemoteHost: null,
            listenPort: null,
            customWorkDir: null,
            verbose: settings.Verbose
            );

        var table = new Table()
               .AddColumn("logical name")
               .AddColumn("file name")
               .AddColumn("created")
               .AddColumn("hash")
               ;

        foreach (var x in swarm.LocalSwarmFiles.List())
        {
            table.AddRow(
                 new Text(x.LogicalName),
                 new Text(x.FileName),
                 new Text(x.Created.ToString()),
                 new Text(x.Hash ?? "")
                 );
        }

        AnsiConsole.Write(table);

        await swarm.LeaveSwarmAsync(swarm.Self);

        return 0;
    }
}
