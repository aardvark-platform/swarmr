using Swarmr.Base.Tasks;

namespace Swarmr.Base.Api;

#region ISwarm

public record SwarmRequest(string Type, object Request);
public record SwarmResponse(string Type, object Response);

public interface ISwarm {
    Node? Self { get; }
    Task<SwarmResponse> SendAsync(SwarmRequest request);
}

#endregion

#region node messages

public record JoinSwarmRequest(Node Node);
public record JoinSwarmResponse(Swarm.Dto Swarm);
public static partial class ISwarmExtensions {
    public static async Task<Swarm> JoinSwarmAsync(this ISwarm client,
        Node self,
        DirectoryInfo workdir,
        bool verbose
        ) 
    {
        var r = await client.SendAsync<JoinSwarmRequest, JoinSwarmResponse>(new(Node: self));
        return await r.Swarm.ToSwarmAsync(self: self, workdir: workdir, verbose: verbose);
    }
}

public record LeaveSwarmRequest(Node Node);
public record LeaveSwarmResponse();
public static partial class ISwarmExtensions {
    public static async Task LeaveSwarmAsync(this ISwarm client,
        Node self
        ) {
        var r = await client.SendAsync<LeaveSwarmRequest, LeaveSwarmResponse>(new(Node: self));
    }
}

public record HeartbeatRequest(string NodeId);
public record HeartbeatResponse();
public static partial class ISwarmExtensions {
    public static async Task HeartbeatAsync(this ISwarm client,
        Node self
        ) {
        var _ = await client.SendAsync<HeartbeatRequest, HeartbeatResponse>(new(NodeId: self.Id));
    }
}

public record PingRequest(Node? Sender);
public record PingResponse(Node Node);
public static partial class ISwarmExtensions {
    public static async Task<Node> PingAsync(this ISwarm client
        ) {
        var response = await client.SendAsync<PingRequest, PingResponse>(new(client.Self));
        return response.Node;
    }
}

public record UpdateNodeRequest(Node Node);
public record UpdateNodeResponse();
public static partial class ISwarmExtensions {
    public static async Task UpdateNodeAsync(this ISwarm client,
        Node node
        ) {
        var _ = await client.SendAsync<UpdateNodeRequest, UpdateNodeResponse>(new(Node: node));
    }
}

public record RemoveNodesRequest(Node Sender, IReadOnlyList<string> NodeIds);
public record RemoveNodesResponse();
public static partial class ISwarmExtensions {
    public static async Task RemoveNodesAsync(this ISwarm client,
        Node sender,
        IReadOnlyList<string> nodeIds
        ) {
        var _ = await client.SendAsync<RemoveNodesRequest, RemoveNodesResponse>(new(Sender: sender, NodeIds: nodeIds));
    }
}

public record GetFailoverNomineeRequest(Node Sender);
public record GetFailoverNomineeResponse(Node Nominee);
public static partial class ISwarmExtensions {
    public static async Task<Node> GetFailoverNomineeAsync(this ISwarm client,
        Node sender
        ) {
        var response = await client.SendAsync<GetFailoverNomineeRequest, GetFailoverNomineeResponse>(new(Sender: sender));
        return response.Nominee;
    }
}

#endregion

#region swarm file messages

public record IngestFileRequest(string LocalFilePath, string LocalFileHash, string Name);
public record IngestFileResponse(IngestFileTask Task);
public static partial class ISwarmExtensions {
    public static async Task<IngestFileTask> IngestFileAsync(this ISwarm client,
        string localFilePath,
        string localFileHash,
        string name
        ) {
        var response = await client.SendAsync<IngestFileRequest, IngestFileResponse>(new(
            LocalFilePath: localFilePath,
            LocalFileHash: localFileHash,
            Name: name
            ));
        return response.Task;
    }
}

#endregion

#region swarm task messages

public record SubmitTaskRequest(SwarmTask.Dto Task);
public record SubmitTaskResponse();
public static partial class ISwarmExtensions {
    public static async Task SubmitTaskAsync(this ISwarm client,
        ISwarmTask task
        ) {
        var dto = SwarmTask.ToDto(task);
        await client.SendAsync<SubmitTaskRequest, SubmitTaskResponse>(new(dto));
    }
}

#endregion

#region swarm jobs messages

public record SubmitJobRequest(string Job);
public record SubmitJobResponse(string JobId);
public static partial class ISwarmExtensions {
    public static async Task<SubmitJobResponse> SubmitJobAsync(this ISwarm client,
        string job
        ) {
        var response = await client.SendAsync<SubmitJobRequest, SubmitJobResponse>(new(
            Job: job
            ));
        return response;
    }
}

public record RunJobRequest(RunJobTask Job);
public record RunJobResponse(bool Accepted);
public static partial class ISwarmExtensions {
    public static async Task<RunJobResponse> RunJobAsync(this ISwarm client,
        RunJobTask job
        ) {
        var response = await client.SendAsync<RunJobRequest, RunJobResponse>(new(
            Job: job
            ));
        return response;
    }
}

public record UpsertJobRequest(string SenderNodeId, Job Job);
public record UpsertJobResponse();
public static partial class ISwarmExtensions {
    public static async Task UpsertJobAsync(this ISwarm client,
        Job activeJob
        )
    {
        if (client.Self == null) throw new Exception("No self. Error d0017c70-a2fb-45ea-b37b-3f7a9e3514ff.");
        await client.SendAsync<UpsertJobRequest, UpsertJobResponse>(new(client.Self.Id, activeJob));
    }
}

public record RemoveJobRequest(string JobId);
public record RemoveJobResponse();
public static partial class ISwarmExtensions {
    public static async Task RemoveJobAsync(this ISwarm client,
        string jobId
        ) {
        await client.SendAsync<RemoveJobRequest, RemoveJobResponse>(new(jobId));
    }
}

public record ListJobsRequest();
public record ListJobsResponse(IReadOnlyList<Job> Jobs);
public static partial class ISwarmExtensions {
    public static async Task<IReadOnlyList<Job>> ListJobsAsync(this ISwarm client
        ) {
        var r = await client.SendAsync<ListJobsRequest, ListJobsResponse>(new());
        return r.Jobs;
    }
}

#endregion

#region swarm secrets messages

public record SetSecretRequest(string Key, string Value);
public record SetSecretResponse();
public static partial class ISwarmExtensions {
    public static async Task SetSecretAsync(this ISwarm client,
        string key, string value
        ) {
        await client.SendAsync<SetSecretRequest, SetSecretResponse>(new(key, value));
    }
}

public record RemoveSecretRequest(string Key);
public record RemoveSecretResponse();
public static partial class ISwarmExtensions {
    public static async Task RemoveSecretAsync(this ISwarm client,
        string key
        ) {
        await client.SendAsync<RemoveSecretRequest, RemoveSecretResponse>(new(key));
    }
}

public record ListSecretsRequest();
public record ListSecretsResponse(IReadOnlyList<string> Secrets);
public static partial class ISwarmExtensions {
    public static async Task<IReadOnlyList<string>> ListSecretsAsync(this ISwarm client
        ) {
        var r = await client.SendAsync<ListSecretsRequest, ListSecretsResponse>(new());
        return r.Secrets;
    }
}

public record UpdateSecretsRequest(string Secrets);
public record UpdateSecretsResponse();
public static partial class ISwarmExtensions {
    public static async Task UpdateSecretsAsync(this ISwarm client,
        SwarmSecrets secrets
        ) {
        var encoded = await secrets.EncodeAsync();
        await client.SendAsync<UpdateSecretsRequest, UpdateSecretsResponse>(new(encoded));
    }
}

#endregion

public static partial class ISwarmExtensions
{
    private static async Task<RES> SendAsync<REQ, RES>(this ISwarm client, REQ request) where REQ : notnull {
        var m = new SwarmRequest(
            Type: typeof(REQ).AssemblyQualifiedName ?? throw new Exception(
                $"Failed to get AssemblyQualifiedName of {typeof(REQ)}. " +
                $"Error 353e84d1-7495-4818-8011-077b110017d5."
                ),
            Request: request
            );

        var (responseTypeName, responseObject) = await client.SendAsync(m);
        var responseType = Type.GetType(responseTypeName) ?? throw new Exception(
                $"Failed to get type for {responseTypeName}. " +
                $"Error 81d77ca4-1823-41c7-8a29-da0b815332a0."
                );

        if (responseType != typeof(RES)) throw new Exception(
            $"Expected response type {typeof(RES)}, but found {responseType}. " +
            $"Error f3a2c92d-ab92-48b8-ba94-09c41013fa7f."
            );

        var response = SwarmUtils.Deserialize<RES>(responseObject);
        return response;
    }
}