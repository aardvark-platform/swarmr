using Spectre.Console.Cli;
using swarmr.Commands;
using Swarmr.Base;

var app = new CommandApp();

app.Configure(config =>
{
    config
        .SetApplicationName("swarmr")
        .SetApplicationVersion(Info.Version)
        .TrimTrailingPeriods(trimTrailingPeriods: false)
        .ValidateExamples()
        ;

    config
        .AddCommand<JoinCommand>("join")
        .WithDescription("Joins swarm.")
        .WithExample(new[] { "join", "node5.lan" })
        ;

    config
        .AddCommand<IngestCommand>("ingest")
        .WithDescription("Uploads files into the swarm.")
        .WithExample(new[] { "ingest", "C:\\data\\example.zip", "-n", "alice/example" })
        ;

    config
        .AddBranch("files", c =>
        {
            c.SetDescription(
                "Manages swarm files."
                );

            c.AddCommand<FilesListCommand>("list")
                .WithAlias("ls")
                .WithDescription("List swarm files.")
                .WithExample(new[] { "files", "list"  })
                ;

            c.AddCommand<FilesExtractCommand>("extract")
                .WithAlias("x")
                .WithDescription("Extracts swarm files to local directory.")
                .WithExample(new[] { "files", "extract", "my/swarm/path", "C:/Data" })
                ;
        })
        ;

    config
        .AddBranch("jobs", c =>
        {
            c.SetDescription(
                "Manages jobs."
                );

            c.AddCommand<JobsSubmitCommand>("submit")
                .WithDescription("Submits a new job to the swarm.")
                .WithExample(new[] {"jobs", "submit", "./compute42.job"})
                ;
        })
        ;

    config
        .AddBranch("secrets", c => {
            c.SetDescription(
                "Manages swarm secrets."
                );

            c.AddCommand<SecretsSetCommand>("set")
                .WithAlias("add")
                .WithDescription("Sets a key/value pair.")
                .WithExample(new[] { "secrets", "set", "KEY1", "0dabc939-4520-4960-b602-03625944a1c3" })
                ;

            c.AddCommand<SecretsRemoveCommand>("remove")
                .WithAlias("rm")
                .WithAlias("delete")
                .WithAlias("del")
                .WithDescription("Deletes a secret.")
                .WithExample(new[] { "secrets", "remove", "KEY1" })
                ;

            c.AddCommand<SecretsListCommand>("list")
                .WithAlias("ls")
                .WithDescription("List all secrets by name.")
                .WithExample(new[] { "secrets", "list" })
                ;
        })
        ;
});

return app.Run(args);
