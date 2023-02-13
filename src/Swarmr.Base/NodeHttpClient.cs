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

    #region ISwarm

    public Task<JoinSwarmResponse> JoinSwarmAsync(JoinSwarmRequest request)
        => Call<JoinSwarmResponse, JoinSwarmRequest>(request);

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
