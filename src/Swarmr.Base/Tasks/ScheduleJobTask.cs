using Spectre.Console;
using Swarmr.Base.Api;
using System.Xml.Linq;

namespace Swarmr.Base.Tasks;

public record ScheduleJobTask(string Id, JobConfig Job) : ISwarmTask
{
    public static ScheduleJobTask Create(JobConfig job) => new(
        Id: $"ScheduleJobTask-{Guid.NewGuid()}",
        Job: job
        );

    public async Task RunAsync(Swarm context)
    {
        var runJobTask = RunJobTask.Create(Job);

        var idleNodes = context.Nodes.Where(n => n.Status == NodeStatus.Idle);
        foreach (var n in idleNodes) AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] idle node {n.Id}[/]");
        foreach (var node in idleNodes)
        {
            // send task to selected node
            AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] sending job {Id} to node {node.Id} ... [/]");
            var runJobResponse = await node.Client.RunJobAsync(runJobTask);
            if (runJobResponse.Accepted)
            {
                AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] sending job {Id} to node {node.Id} ... ACCEPTED[/]");
                return;
            }
            else
            {
                AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] sending job {Id} to node {node.Id} ... JOB REJECTED[/]");
            }
        }

        var delay = TimeSpan.FromSeconds(15);
        AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] no idle worker nodes, trying again in {delay}.[/]");
        context.EnqueueDelayedAsync(this, delay);
    }
}
