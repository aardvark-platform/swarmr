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
        .WithExample(new[] { "ingest", "example.zip" })
        ;

    config
        .AddBranch("data", c =>
        {
            c.SetDescription(
                "Manages job data."
                );

            c.AddCommand<JobDataAddCommand>("add")
                  .WithDescription("Prepares input data for jobs.")
                  .WithExample(new[] { "data", "add", "\"my test runner\"", "\"./data1.zip\"",  })
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
                  .WithExample(new[] {"jobs", "submit", "./job.json"})
                  .WithExample(new[] {
                      "jobs", "submit", 
                      "-r", "\"runner1\"",
                      "-c", "\"compute.exe data1.txt data2.txt > result.txt\"",
                      "-i", "data1.txt", "-i", "data2.txt",
                      "-o", "result.txt", "-o", "log.txt"
                  })
                  ;
        })
        ;
});

return app.Run(args);
