using System.Runtime.CompilerServices;

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

public record RegisterRunnerRequest(string Source, string Name, string Runtime);
public record RegisterRunnerResponse(Runner Runner);

public interface ISwarm
{
    Task<JoinSwarmResponse         > JoinSwarmAsync         (JoinSwarmRequest          request);
    Task<HeartbeatResponse         > HeartbeatAsync         (HeartbeatRequest          request);
    Task<PingResponse              > PingAsync              (PingRequest               request);
    Task<UpdateNodeResponse        > UpdateNodeAsync        (UpdateNodeRequest         request);
    Task<RemoveNodesResponse       > RemoveNodesAsync       (RemoveNodesRequest        request);
    Task<GetFailoverNomineeResponse> GetFailoverNomineeAsync(GetFailoverNomineeRequest request);
    Task<RegisterRunnerResponse    > RegisterRunnerAsync    (RegisterRunnerRequest     request);
}

public record SwarmRequest(string Type, object Request);
public record SwarmResponse(string Type, object Response);

public static class INodeClientExtensions
{
    public static Task<SwarmResponse> RequestAsync(this ISwarm client, SwarmRequest request)
    {
        return request.Type switch
        {
            nameof(JoinSwarmRequest         ) => call<JoinSwarmRequest         , JoinSwarmResponse         >(client.JoinSwarmAsync         ),
            nameof(HeartbeatRequest         ) => call<HeartbeatRequest         , HeartbeatResponse         >(client.HeartbeatAsync         ),
            nameof(PingRequest              ) => call<PingRequest              , PingResponse              >(client.PingAsync              ),
            nameof(UpdateNodeRequest        ) => call<UpdateNodeRequest        , UpdateNodeResponse        >(client.UpdateNodeAsync        ),
            nameof(RemoveNodesRequest       ) => call<RemoveNodesRequest       , RemoveNodesResponse       >(client.RemoveNodesAsync       ),
            nameof(GetFailoverNomineeRequest) => call<GetFailoverNomineeRequest, GetFailoverNomineeResponse>(client.GetFailoverNomineeAsync),
            nameof(RegisterRunnerRequest    ) => call<RegisterRunnerRequest    , RegisterRunnerResponse    >(client.RegisterRunnerAsync    ),

            _ => throw new Exception( 
                $"Unknown request \"{request.Type}\". " +
                $"Error 53fe2ab3-c803-445d-84b4-f35e59003986."
                )
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        async Task<SwarmResponse> call<REQ, RES>(Func<REQ, Task<RES>> f) => new SwarmResponse(
            Type: typeof(RES).Name,
            Response: await f(SwarmUtils.Deserialize<REQ>(request.Request)) ?? throw new Exception("Error 3a4c26bc-c3b8-4aa4-9248-4a7fd63df647.")
            );
    }

    public static async Task<Swarm> JoinSwarmAsync(this ISwarm client, Node self, string workdir)
    {
        var response = await client.JoinSwarmAsync(new(Candidate: self));
        return response.Swarm.ToSwarm(self: self, workdir: workdir);
    }
   
    public static async Task HeartbeatAsync(this ISwarm client, Node self)
    {
        var _ = await client.HeartbeatAsync(new(NodeId: self.Id));
    }

    public static async Task<Node> PingAsync(this ISwarm client)
    {
        var response = await client.PingAsync(new());
        return response.Node;
    }

    public static async Task UpdateNodeAsync(this ISwarm client, Node node)
    {
        var _ = await client.UpdateNodeAsync(new(Node: node));
    }

    public static async Task RemoveNodesAsync(this ISwarm client, IReadOnlyList<string> nodeIds)
    {
        var _ = await client.RemoveNodesAsync(new(NodeIds: nodeIds));
    }

    public static async Task<Node> GetFailoverNomineeAsync(this ISwarm client)
    {
        var response = await client.GetFailoverNomineeAsync(new());
        return response.Nominee;
    }

    public static async Task<Runner> RegisterRunnerAsync(this ISwarm client, string source, string name, string runtime)
    {
        var response = await client.RegisterRunnerAsync(new(
            Source: source,
            Name: name,
            Runtime: runtime
            ));
        return response.Runner;
    }
}