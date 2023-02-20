using Spectre.Console;
using Spectre.Console.Cli;
using Spectre.Console.Rendering;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class FilesListCommand : AsyncCommand<FilesListCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("Tree view. ")]
        [CommandOption("-t|--tree")]
        public bool TreeView { get; init; }

        [Description("Print additional information. ")]
        [CommandOption("-v|--verbose")]
        public bool Verbose { get; init; }
    }

    private record NFile(string Name, SwarmFile File)
    {
        public IRenderable Render(Table table) => table.AddRow(
            new Text(Name),
            new Text(File.FileName),
            new Text(File.Created.ToString()),
            new Text(File.Hash ?? "")
            );

        public static NFile Create(SwarmFile file)
            => new(Name: Path.GetFileName(file.LogicalName), File: file);
    }
    private record NDir(string Name, NFile[] Files, NDir[] Dirs)
    {
        public IRenderable Render()
        {
            var tree = new Tree(Name);

            if (Files.Length > 0)
            {
                var table = new Table()
                    .AddColumn("logical name")
                    .AddColumn("file name")
                    .AddColumn("created")
                    .AddColumn("hash")
                    ;
                foreach (var x in Files) x.Render(table);
                tree.AddNode(table);
            }

            foreach (var x in Dirs)
            {
                tree.AddNode(x.Render());
            }

            return tree;
        }

        public static NDir Create(string name, (string[] parts, SwarmFile file)[] xs)
        {
            var files = xs
                .Where(x => x.parts.Length == 1)
                .Select(x => NFile.Create(x.file))
                .ToArray()
                ;
            
            var dirs = xs
                .Where(x => x.parts.Length > 1)
                .GroupBy(x => x.parts[0])
                .Select(g =>
                {
                    var ys = g.Select(x => (parts: x.parts[1..], x.file)).ToArray();
                    return Create(name: g.Key, ys);
                })
                .ToArray()
                ;

            return new NDir(
                Name: name,
                Files: files,
                Dirs: dirs
                );
        }
        public static NDir Create(string name, IEnumerable<SwarmFile> files)
            => Create(name, files
                .Select(f => (parts: f.LogicalName.Split('/'), file: f))
                .ToArray()
                );
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        var swarm = await SwarmUtils.GetClientSwarm(settings.Verbose);

        var allFiles = swarm.LocalSwarmFiles.List().OrderBy(x => x.LogicalName);

        if (settings.TreeView)
        {
            var root = NDir.Create("/", allFiles);
            AnsiConsole.Write(root.Render());
        }
        else
        {
            var table = new Table()
                    .AddColumn("logical name")
                    .AddColumn("file name")
                    .AddColumn("created")
                    .AddColumn("hash")
                    ;
            foreach (var x in allFiles)
            {
                table.AddRow(
                        new Text(x.LogicalName),
                        new Text(x.FileName),
                        new Text(x.Created.ToString()),
                        new Text(x.Hash ?? "")
                        );
            }
            AnsiConsole.Write(table);
        }

        await swarm.LeaveSwarmAsync(swarm.Self);

        return 0;
    }
}
