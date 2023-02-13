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
});

return app.Run(args);
