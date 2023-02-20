using Spectre.Console;

namespace Swarmr.Base.Tasks;

public class SwarmTaskQueue
{
    private static readonly Queue<ISwarmTask> _queue = new();

    public Task Enqueue(ISwarmTask task)
    {
        lock (_queue) _queue.Enqueue(task);
        _ = Task.Run(() => AnsiConsole.MarkupLine($"[aqua][[SwarmTaskQueue]][[ENQUEUE]]{task.GetType().FullName.EscapeMarkup()}[/]"));
        return Task.CompletedTask;
    }

    public Task<ISwarmTask?> TryDequeue()
    {
        ISwarmTask? result;
        lock (_queue) _queue.TryDequeue(out result);
        if (result != null)
        {
            _ = Task.Run(() => AnsiConsole.MarkupLine($"[teal][[SwarmTaskQueue]][[DEQUEUE]]{result.GetType().FullName.EscapeMarkup()}[/]"));
        }
        return Task.FromResult(result);
    }
}
