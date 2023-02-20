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
                  .WithDescription("List swarm files.")
                  .WithExample(new[] { "files", "list"  })
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
});

return app.Run(args);
