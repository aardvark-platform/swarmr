using Spectre.Console;

namespace Swarmr.Base.Tasks;

public class SwarmTaskQueue
{
    private static readonly Queue<ISwarmTask> _queue = new();

    public Task Enqueue(ISwarmTask task)
    {
        lock (_queue) _queue.Enqueue(task);
        AnsiConsole.MarkupLine($"[aqua][[SwarmTaskQueue.Enqueue]]{task.ToJsonString().EscapeMarkup()}[/]");
        return Task.CompletedTask;
    }

    public Task<ISwarmTask?> TryDequeue()
    {
        ISwarmTask? result;
        lock (_queue) _queue.TryDequeue(out result);
        if (result != null)
        {
            AnsiConsole.MarkupLine($"[teal][[SwarmTaskQueue.Dequeue]]{result.ToJsonString().EscapeMarkup()}[/]");
        }
        return Task.FromResult(result);
    }
}
