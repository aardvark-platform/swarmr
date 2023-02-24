using Spectre.Console;
using Spectre.Console.Cli;
using Spectre.Console.Rendering;
using Swarmr.Base;
using Swarmr.Base.Api;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace swarmr.Commands;

public class SecretsSetCommand : AsyncCommand<SecretsSetCommand.Settings>
{
    public class Settings : CommandSettings
    {
        [Description("Name of secret. ")]
        [CommandArgument(0, "<KEY>")]
        public string Key { get; init; } = null!;

        [Description("Value of secret. ")]
        [CommandArgument(1, "<VALUE>")]
        public string Value { get; init; } = null!;
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Key)) {
            AnsiConsole.WriteLine("<KEY> must not be empty.");
            return 1;
        }

        var swarm = await SwarmUtils.GetEphemeralSwarm(verbose: false);

        await swarm.SetSecretAsync(settings.Key, settings.Value ?? "");
       
        await swarm.LeaveSwarmAsync(swarm.Self);

        return 0;
    }
}

public class SecretsRemoveCommand : AsyncCommand<SecretsRemoveCommand.Settings> {
    public class Settings : CommandSettings {
        [Description("Name of secret. ")]
        [CommandArgument(0, "<KEY>")]
        public string Key { get; init; } = null!;
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings) {
        if (string.IsNullOrWhiteSpace(settings.Key)) {
            AnsiConsole.WriteLine("<KEY> must not be empty.");
            return 1;
        }

        var swarm = await SwarmUtils.GetEphemeralSwarm(verbose: false);

        await swarm.RemoveSecretAsync(settings.Key);

        await swarm.LeaveSwarmAsync(swarm.Self);

        return 0;
    }
}

public class SecretsListCommand : AsyncCommand<SecretsListCommand.Settings> {
    public class Settings : CommandSettings {
    }

    public override async Task<int> ExecuteAsync([NotNull] CommandContext context, [NotNull] Settings settings) {
      
        var swarm = await SwarmUtils.GetEphemeralSwarm(verbose: false);

        var xs = await swarm.ListSecretsAsync();
        foreach (var s in xs) AnsiConsole.WriteLine(s);

        await swarm.LeaveSwarmAsync(swarm.Self);

        return 0;
    }
}