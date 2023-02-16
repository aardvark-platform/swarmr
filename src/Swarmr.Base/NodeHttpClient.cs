using Swarmr.Base.Api;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;

namespace Swarmr.Base;

public class NodeHttpClient : ISwarm
{
    private readonly HttpClient _http;

    public NodeHttpClient(string url)
    {
        if (url == null) throw new ArgumentNullException(
            paramName: nameof(url),
            message: "Error f8332b8b-d75f-4b14-b2db-4af8691e8477."
            );

        _http = new HttpClient() { BaseAddress = new Uri(url) } ;
    }

    public NodeHttpClient(string host, int port) 
        : this($"http://{host}:{port}")
    { }

    #region ISwarm

    public Task<JoinSwarmResponse> JoinSwarmAsync(JoinSwarmRequest request)
        => Call<JoinSwarmResponse, JoinSwarmRequest>(request);

    public Task<HeartbeatResponse> HeartbeatAsync(HeartbeatRequest request)
        => Call<HeartbeatResponse, HeartbeatRequest>(request);

    public Task<PingResponse> PingAsync(PingRequest request)
        => Call<PingResponse, PingRequest>(request);

    public Task<UpdateNodeResponse> UpdateNodeAsync(UpdateNodeRequest request)
        => Call<UpdateNodeResponse, UpdateNodeRequest>(request);

    public Task<RemoveNodesResponse> RemoveNodesAsync(RemoveNodesRequest request)
        => Call<RemoveNodesResponse, RemoveNodesRequest>(request);

    public Task<GetFailoverNomineeResponse> GetFailoverNomineeAsync(GetFailoverNomineeRequest request)
        => Call<GetFailoverNomineeResponse, GetFailoverNomineeRequest>(request);

    public Task<RegisterRunnerResponse> RegisterRunnerAsync(RegisterRunnerRequest request)
        => Call<RegisterRunnerResponse, RegisterRunnerRequest>(request);

    #endregion

    #region helpers

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task<RES> Call<RES, REQ>(REQ request)
    {
        var httpResponse = await _http.PostAsJsonAsync("/api", new SwarmRequest(typeof(REQ).Name, request!));
        var r =await httpResponse.Content.ReadFromJsonAsync<SwarmResponse>();
        return SwarmUtils.Deserialize<RES>(r!.Response);
    }

    #endregion
}
