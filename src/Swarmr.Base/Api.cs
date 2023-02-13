using System.Runtime.CompilerServices;

namespace Swarmr.Base.Api;

public record SwarmRequest(string Type, object Request);
public record SwarmResponse(string Type, object Response);

public record JoinSwarmRequest(Node Self);
public record JoinSwarmResponse(Swarm.Dto Swarm);

public interface ISwarm
{
    Task<JoinSwarmResponse> JoinSwarmAsync(JoinSwarmRequest request);
}

public static class INodeClientExtensions
{
    public static async Task<Swarm> JoinSwarmAsync(this ISwarm client, Node self)
    {
        var response = await client.JoinSwarmAsync(new(Self: self));
        return response.Swarm.ToSwarm();
    }

    public static Task<SwarmResponse> RequestAsync(this ISwarm client, SwarmRequest request)
    {
        return request.Type switch
        {
            nameof(JoinSwarmRequest) => call<JoinSwarmRequest, JoinSwarmResponse>(client.JoinSwarmAsync),

            _ => throw new Exception(
                $"Unknown request \"{request.Type}\". " +
                $"Error 53fe2ab3-c803-445d-84b4-f35e59003986."
                )
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        async Task<SwarmResponse> call<REQ, RES>(Func<REQ, Task<RES>> f) => new SwarmResponse(
            Type: typeof(RES).Name,
            Response: await f(SwarmUtils.Deserialize<REQ>(request)) ?? throw new Exception("Error 3a4c26bc-c3b8-4aa4-9248-4a7fd63df647.")
            );
    }
}