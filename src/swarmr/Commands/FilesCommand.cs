using Spectre.Console;
using Spectre.Console.Cli;
using Spectre.Console.Rendering;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.IO.Compression;

namespace swarmr.Commands;

public class FilesListCommand : AsyncCommand<FilesListCommand.Settings>
{
    public class Settings : CommandSettings 
    { 
        [Description("Path to list. Default is root path. ")]
        [CommandArgument(0, "[PATH]")]
        public string? Path { get; init; }

        [Description("List directories and their contents recursively. ")]
        [CommandOption("-r|--recursive")]
        public bool Recursive { get; init; }

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
        var swarm = await SwarmUtils.GetEphemeralSwarm(settings.Verbose);

        var allFiles = swarm.LocalSwarmFiles
            .List(settings.Path, recursive: settings.Recursive)
            .OrderBy(x => x.LogicalName)
            ;

        if (settings.TreeView && settings.Recursive)
        {
            var root = NDir.Create("/", allFiles.Cast<SwarmFile>());
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
            foreach (var x in allFiles) {
                switch (x) 
                {
                    case SwarmFile f: {
                            table.AddRow(
                                new Text(f.LogicalName),
                                new Text(f.FileName),
                                new Text(f.Created.ToString()),
                                new Text(f.Hash ?? "")
                                );
                            break;
                        }
                    case SwarmFileDir d: {
                            table.AddRow(
                                new Text(d.LogicalName),
                                new Text("/"),
                                new Text(d.Created.ToString()),
                                new Text("")
                                );
                            break;
                        }
                }
            }
            AnsiConsole.Write(table);
        }

        await swarm.LeaveSwarmAsync(swarm.Self);

        return 0;
    }
}

public class FilesExtractCommand : AsyncCommand<FilesExtractCommand.Settings> {
    public class Settings : CommandSettings 
    {
        [Description("Extract all swarm files in [SOURCE] path to local file system. Default is root path. ")]
        [CommandArgument(0, "[SOURCE]")]
        public string? Source { get; init; }

        [Description("Target path in local filesystem. ")]
        [CommandArgument(1, "<TARGET>")]
        public string Target { get; init; } = null!;

        [Description("Overwrite existing files. ")]
        [CommandOption("-f|--force")]
        public bool Force { get; init; }

        [Description("Print additional information. ")]
        [CommandOption("-v|--verbose")]
        public bool Verbose { get; init; }
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings) 
    {
        if (settings.Target == null) 
        { 
            AnsiConsole.WriteLine($"Missing <TARGET> path.");
            return 1;
        }

        var targetBase = new DirectoryInfo(settings.Target);
        if (!targetBase.Exists) targetBase.Create();

        var swarm = await SwarmUtils.GetEphemeralSwarm(settings.Verbose);

        var files = swarm.LocalSwarmFiles.List(settings.Source, recursive: true).OrderBy(x => x.LogicalName);
        foreach (var file in files.Cast<SwarmFile>()) 
        {
            AnsiConsole.WriteLine(file.LogicalName);

            var sourceFile = swarm.LocalSwarmFiles.GetContentFileInfo(file);

            var targetDir = new DirectoryInfo(Path.Combine(targetBase.FullName, file.LogicalName));
            if (!targetDir.Exists) targetDir.Create();

            if (sourceFile.Extension.ToLower() == ".zip") 
            {
                ZipFile.ExtractToDirectory(sourceFile.FullName, targetDir.FullName, overwriteFiles: settings.Force);
            }
            else 
            {
                sourceFile.CopyTo(Path.Combine(targetDir.FullName, file.FileName), overwrite: settings.Force);
            }
        }

        await swarm.LeaveSwarmAsync(swarm.Self);

        return 0;
    }
}

public class FilesDeleteCommand : AsyncCommand<FilesDeleteCommand.Settings> 
{
    public class Settings : CommandSettings
    {
        [Description("Delete swarm files in [PATH]. Default is root path. ")]
        [CommandArgument(0, "[PATH]")]
        public string? Path { get; init; }

        [Description("Delete directories and their contents recursively. ")]
        [CommandOption("-r|--recursive")]
        public bool Recursive { get; init; }
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        var swarm = await SwarmUtils.GetEphemeralSwarm(verbose: false);

        await swarm.Primary.Client.DeleteSwarmFilesAsync(sender: swarm.Self, path: settings.Path, recursive: settings.Recursive);

        return 0;
    }
}