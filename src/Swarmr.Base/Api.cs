using Swarmr.Base.Tasks;

namespace Swarmr.Base.Api;

public record JoinSwarmRequest(Node Candidate);
public record JoinSwarmResponse(Swarm.Dto Swarm);

public record HeartbeatRequest(string NodeId);
public record HeartbeatResponse();

public record PingRequest();
public record PingResponse(Node Node);

public record UpdateNodeRequest(Node Node);
public record UpdateNodeResponse();

public record RemoveNodesRequest(IReadOnlyList<string> NodeIds);
public record RemoveNodesResponse();

public record GetFailoverNomineeRequest();
public record GetFailoverNomineeResponse(Node Nominee);

public record RegisterRunnerRequest(string Source, string SourceHash, string Name, string Runtime);
public record RegisterRunnerResponse(Runner Runner);

public record SubmitTaskRequest(SwarmTask.Dto Task);
public record SubmitTaskResponse();

public interface ISwarm
{
    Task<SwarmResponse> SendAsync(SwarmRequest request);
}

public record SwarmRequest(string Type, object Request);
public record SwarmResponse(string Type, object Response);

public static class INodeClientExtensions
{
    private static async Task<RES> SendAsync<REQ, RES>(this ISwarm client, REQ request) where REQ : notnull
    {
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

    public static async Task<Swarm> JoinSwarmAsync(this ISwarm client,
        Node self,
        string workdir,
        bool verbose
        )
    {
        var r = await client.SendAsync<JoinSwarmRequest, JoinSwarmResponse>(new(Candidate: self));
        return r.Swarm.ToSwarm(self: self, workdir: workdir, verbose: verbose);
    }
   
    public static async Task HeartbeatAsync(this ISwarm client,
        Node self
        )
    {
        var _ = await client.SendAsync<HeartbeatRequest, HeartbeatResponse>(new(NodeId: self.Id));
    }

    public static async Task<Node> PingAsync(this ISwarm client
        )
    {
        var response = await client.SendAsync<PingRequest, PingResponse>(new());
        return response.Node;
    }

    public static async Task UpdateNodeAsync(this ISwarm client,
        Node node
        )
    {
        var _ = await client.SendAsync<UpdateNodeRequest, UpdateNodeResponse>(new(Node: node));
    }

    public static async Task RemoveNodesAsync(this ISwarm client,
        IReadOnlyList<string> nodeIds
        )
    {
        var _ = await client.SendAsync<RemoveNodesRequest, RemoveNodesResponse>(new(NodeIds: nodeIds));
    }

    public static async Task<Node> GetFailoverNomineeAsync(this ISwarm client
        )
    {
        var response = await client.SendAsync<GetFailoverNomineeRequest, GetFailoverNomineeResponse>(new());
        return response.Nominee;
    }

    public static async Task<Runner> RegisterRunnerAsync(this ISwarm client, 
        string sourceFile,
        string sourceFileHash,
        string name, 
        string runtime
        )
    {
        var response = await client.SendAsync<RegisterRunnerRequest, RegisterRunnerResponse>(new(
            Source: sourceFile,
            SourceHash: sourceFileHash,
            Name: name,
            Runtime: runtime
            ));
        return response.Runner;
    }

    public static async Task SubmitTaskAsync(this ISwarm client,
        ISwarmTask task
        )
    {
        var dto = SwarmTask.ToDto(task);
        await client.SendAsync<SubmitTaskRequest, SubmitTaskResponse>(new(dto));
    }
}