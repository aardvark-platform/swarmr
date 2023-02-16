using Swarmr.Base.Api;
using System.Net.Http.Json;

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

        _http = new HttpClient() { BaseAddress = new Uri(url), Timeout = Swarm.NODE_TIMEOUT } ;
    }

    public NodeHttpClient(string host, int port) 
        : this($"http://{host}:{port}")
    { }

    #region ISwarm

    public async Task<SwarmResponse> SendAsync(SwarmRequest request)
    {
        var httpResponse = await _http.PostAsJsonAsync("/api", request);
        var r = await httpResponse.Content.ReadFromJsonAsync<SwarmResponse>() ?? throw new Exception();
        return r;
    }

    #endregion
}
