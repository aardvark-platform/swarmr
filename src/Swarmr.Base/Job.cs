using Spectre.Console;
using Swarmr.Base.Api;
using Swarmr.Base.Tasks;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Swarmr.Base;

/// <summary>
/// </summary>
/// <param name="Id">Job ID.</param>
/// <param name="Setup">Swarm files to extract into job dir.</param>
/// <param name="Execute">Command lines to execute.</param>
/// <param name="Results">Result files/dirs to save (relative to job dir).</param>
/// <param name="ResultFile">Swarm file name for saved results.</param>
public record JobDescription(
    string Id,
    IReadOnlyList<string>? Setup,
    IReadOnlyList<JobDescription.ExecuteItem>? Execute,
    IReadOnlyList<string>? Collect,
    string Result
    )
{
    public record ExecuteItem(string Exe, string Args);
}

public enum JobStatus {
    Pending,
    Running,
    Succeeded,
    Failed
}

public record Job(
    JobDescription Config,
    JobStatus Status,
    int MaxRetries,
    int Runs,
    ImmutableDictionary<int, DateTimeOffset> Started,
    ImmutableDictionary<int, DateTimeOffset> Stopped,
    ImmutableDictionary<int, string> WorkerNodeId,
    ImmutableDictionary<int, Exception> Errors
    )
{
    public static Job Create(JobDescription config) => new (
        Config: config,
        Status: JobStatus.Pending,
        MaxRetries: 3,
        Runs: 0,
        Started: ImmutableDictionary<int, DateTimeOffset>.Empty,
        Stopped : ImmutableDictionary<int, DateTimeOffset>.Empty,
        WorkerNodeId: ImmutableDictionary<int, string>.Empty,
        Errors: ImmutableDictionary<int, Exception>.Empty
    );

    public string JobId => Config.Id;

    public bool IsCurrentlyRunningOnNode(string nodeId) 
    {
        if (Status == JobStatus.Running && WorkerNodeId.TryGetValue(Runs - 1, out var workerNodeId))
        {
            return workerNodeId == nodeId;
        }
        else 
        {
            return false;
        }
    }

    public Job WithStart(string startedOnNodeId) {
        if (Status == JobStatus.Pending) {
            var i = Runs;
            return this with {
                Status = JobStatus.Running,
                Runs = i + 1,
                Started = Started.Add(i, DateTimeOffset.UtcNow),
                WorkerNodeId = WorkerNodeId.Add(i, startedOnNodeId)
                };
        }
        else {
            throw new NotImplementedException("48a2e39c-72b8-44e1-b2c3-5c145e9e2a28");
        }
    }

    public Job WithLostNode(string lostOnNodeId) {
        if (Status == JobStatus.Running) {
            var i = Runs - 1;
            return this with {
                MaxRetries = MaxRetries + 1, // lost worker node DOES NOT count as retry
                Status = Runs < MaxRetries ? JobStatus.Pending : JobStatus.Failed,
                Stopped = Stopped.Add(i, DateTimeOffset.UtcNow),
                Errors = Errors.Add(i, new Exception($"lost on node {lostOnNodeId})"))
            };
        }
        else {
            throw new NotImplementedException("43e4926b-7482-4ac0-8f3a-ffebd941712d");
        }
    }

    public Job WithFailed(Exception e) {
        if (Status == JobStatus.Running) {
            var i = Runs - 1;
            return this with {
                Status = Runs < MaxRetries ? JobStatus.Pending : JobStatus.Failed,
                Stopped = Stopped.Add(i, DateTimeOffset.UtcNow),
                Errors = Errors.Add(i, e)
            };
        }
        else {
            throw new NotImplementedException("43e4926b-7482-4ac0-8f3a-ffebd941712d");
        }
    }

    public Job WithSucceeded() {
        if (Status == JobStatus.Running) {
            var i = Runs - 1;
            return this with {
                Status = JobStatus.Succeeded,
                Stopped = Stopped.Add(i, DateTimeOffset.UtcNow)
            };
        }
        else {
            throw new NotImplementedException("43e4926b-7482-4ac0-8f3a-ffebd941712d");
        }
    }
}

public class JobPool 
{
    public static JobPool Create() => new();
    public static JobPool Create(Dto dto) => new(dto.Jobs);

    private readonly ConcurrentDictionary<string, Job> _jobs = new();
    private JobPool() { }
    private JobPool(IEnumerable<Job> jobs) {
        foreach (var job in jobs) _jobs[job.JobId] = job;
    }

    public IReadOnlyList<Job> Jobs => _jobs.Values.ToImmutableList();

    public void UpsertJob(Job x) 
        => _jobs[x.JobId] = x;

    public void RemoveJob(string jobId)
        => _jobs.Remove(jobId, out var _);

    public bool TryRemoveJob(string jobId, [NotNullWhen(true)]out Job? job)
        => _jobs.TryRemove(jobId, out job);

    public bool TryGetJob(string jobId, [NotNullWhen(true)] out Job? job)
        => _jobs.TryGetValue(jobId, out job);
    
    public IReadOnlyList<Job> RemoveJobsForWorkerNode(string workerNodeId)
    {
        var jobs = _jobs.Values
            .Where(x => x.IsCurrentlyRunningOnNode(workerNodeId))
            .ToList()
            ;
        foreach (var job in jobs) RemoveJob(job.JobId);
        return jobs;
    }

    public async void NotifyLostWorkerNodes(Swarm swarm, IEnumerable<string> lostNodeIds)
    {
        var updatedJobs = new List<Job>();

        foreach (var lostNodeId in lostNodeIds) {

            var lostJobs = RemoveJobsForWorkerNode(lostNodeId);

            if (lostJobs.Count > 0) {
                AnsiConsole.MarkupLine($"[lime][[RefreshNodeListAsync]] lost {lostJobs.Count} active job(s) on node {lostNodeId}[/]");

                foreach (var lostJob in lostJobs) {
                    var updatedJob = lostJob.WithLostNode(lostOnNodeId: lostNodeId);
                    UpsertJob(updatedJob);
                    updatedJobs.Add(updatedJob);

                    if (swarm.IAmPrimary) 
                    {
                        // notify others about job update
                        swarm.Others
                            .Where(x => x.Type != NodeType.Ephemeral)
                            .SendEach(async n => await n.UpsertJobAsync(updatedJob))
                            ;

                        // re-schedule
                        AnsiConsole.MarkupLine($"[lime][[RefreshNodeListAsync]] re-schedule job {updatedJob.JobId} ... [/]");
                        await swarm.LocalTaskQueue.Enqueue(ScheduleJobTask.Create(updatedJob));
                        AnsiConsole.MarkupLine($"[lime][[RefreshNodeListAsync]] re-schedule job {updatedJob.JobId} ... done[/]");
                    }
                }
            }
        }
    }

    public void NotifyLostWorkerNodes(Swarm swarm, string lostNodeId)
        => NotifyLostWorkerNodes(swarm, new[] { lostNodeId });


    public record Dto(IReadOnlyList<Job> Jobs) {
        public static readonly Dto Empty = new(Array.Empty<Job>());
    }
    public Dto ToDto() => new(Jobs);
}

public static class Jobs 
{
    public static JobDescription Parse(string src, SwarmSecrets secrets) {

        var jobid = $"job-{Guid.NewGuid()}";

        var lines = src
            .Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(StripComments)
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Select(ReplaceVariables)
            .Select(TokenizeLine)
            .ToArray()
            ;

        string ReplaceVariables(string line) {
            line = line.Replace("{JOBID}", jobid, ignoreCase: true, CultureInfo.InvariantCulture);
            foreach (var (key, value) in secrets.Map) {
                line = line.Replace("{" + key + "}", value, ignoreCase: true, CultureInfo.InvariantCulture);
            }
            return line;
        }

        var setup = ImmutableList<string>.Empty;
        var execute = ImmutableList<JobDescription.ExecuteItem>.Empty;
        var collect = ImmutableList<string>.Empty;
        string? result = null;

        for (var i = 0; i < lines.Length; i++) {
            var (line, tokens) = lines[i];
            switch (tokens[0].ToLower()) {
                case "setup": {
                        if (tokens.Count != 2) throw new Exception();
                        setup = setup.Add(tokens[1]);
                        break;
                    }

                case "execute": {
                        var x = new JobDescription.ExecuteItem(
                            Exe: lines[++i].line,
                            Args: lines[++i].line
                            );
                        execute = execute.Add(x);
                        break;
                    }

                case "collect": {
                        if (tokens.Count != 2) throw new Exception();
                        collect = collect.Add(tokens[1]);
                        break;
                    }

                case "result": {
                        if (tokens.Count != 2) throw new Exception();
                        if (result != null) throw new Exception("Result already defined.");
                        result = tokens[1];
                        break;
                    }
            }
        }

        if (result == null) throw new Exception("Result is undefined.");

        var job = new JobDescription(
            Id: jobid,
            Setup: setup,
            Execute: execute,
            Collect: collect,
            Result: result
            );

        return job;

        static string StripComments(string line) {
            line = line.Trim();
            var i = line.IndexOf('#');
            if (i >= 0) line = line[..i];
            return line.Trim();
        }

        static (string line, ImmutableList<string> tokens) TokenizeLine(string line) {
            var tokens = ImmutableList<string>.Empty;
            var token = "";
            var insideQuotes = false;
            for (var i = 0; i < line.Length; i++) {
                var c = line[i];

                if (char.IsWhiteSpace(c) && token.Length == 0) {
                    continue;
                }

                if (c == '\"') {
                    insideQuotes = !insideQuotes;
                    continue;
                }

                if (insideQuotes) {
                    token += c;
                    continue;
                }

                if (char.IsWhiteSpace(c)) {
                    tokens = tokens.Add(token);
                    token = "";
                    continue;
                }

                token += c;
            }

            if (token.Length > 0) {
                tokens = tokens.Add(token);
            }

            return (line, tokens);
        }
    }
}