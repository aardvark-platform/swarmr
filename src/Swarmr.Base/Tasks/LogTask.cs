using Spectre.Console;

namespace Swarmr.Base.Tasks;

/// <summary>
/// Mostly for testing and debugging purposes.
/// </summary>
public record LogTask(string Id, string Message) : ISwarmTask
{
    public Task RunAsync(Swarm context)
    {
        AnsiConsole.WriteLine(Message);
        return Task.CompletedTask;
    }
}
