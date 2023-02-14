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
        .WithExample(new[] { "join", "http://node5.lan" })
        ;

    config
        .AddBranch("runners", c =>
        {
            c.SetDescription(
                "Manages runners."
                );

            c.AddCommand<RunnersRegisterCommand>("register")
                  .WithDescription("Registers a new runner.")
                  .WithExample(new[] { "runners", "register", "\"./myrunner.zip\"" })
                  .WithExample(new[] { "runners", "register", "\"./myrunner.zip\"", "--name", "\"my test runner\"", "-r", "\"linux-x64\"" })
                  ;
        })
        ;
});

return app.Run(args);
