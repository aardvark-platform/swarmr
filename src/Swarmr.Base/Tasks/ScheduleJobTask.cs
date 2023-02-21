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

        // get idle nodes
        var idleNodes = context.Nodes.Where(n => n.Status == NodeStatus.Idle).ToList();

        // if possible, do not schedule to priary node ...
        if (idleNodes.Count > 1 && context.PrimaryId != null)
        {
            idleNodes = idleNodes.Except(context.PrimaryId).ToList();
            if (idleNodes.Count == 0) throw new Exception("Error 7ff23229-d11a-419c-9dce-2a6799f5264d.");
        }

        while (idleNodes.Count > 0)
        {
            // schedule random node
            var i = Random.Shared.Next(idleNodes.Count);
            var scheduledNode = idleNodes[i];
            idleNodes.RemoveAt(i);

            // send task to selected node
            AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] sending job {Id} to node {scheduledNode.Id} ... [/]");
            var runJobResponse = await scheduledNode.Client.RunJobAsync(runJobTask);
            if (runJobResponse.Accepted)
            {
                AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] sending job {Id} to node {scheduledNode.Id} ... ACCEPTED[/]");
                return;
            }
            else
            {
                AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] sending job {Id} to node {scheduledNode.Id} ... JOB REJECTED[/]");
            }
        }

        var delay = TimeSpan.FromSeconds(15);
        AnsiConsole.MarkupLine($"[lime][[ScheduleJobTask]] no idle worker nodes, trying again in {delay}.[/]");
        context.EnqueueDelayedAsync(this, delay);
    }
}
