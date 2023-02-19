using Spectre.Console;
using Swarmr.Base.Api;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public record ProbeResult(
    string Hostname,
    NodePort[] Ports
    )
{
    /// <summary>
    /// Returns first free port, or null if all ports are used. 
    /// </summary>
    public bool TryGetFreePort([NotNullWhen(true)] out int? port)
    {
        var p = Ports.FirstOrDefault(x => x.Status == NodePortStatus.Free);
        if (p != null)
        {
            port = p.Port;
            return true;
        }
        else
        {
            port = null;
            return false;
        }
    }

    /// <summary>
    /// Returns a live port, or null if no node is running.
    /// </summary>
    public bool TryGetLivePort([NotNullWhen(true)] out int? port)
    {
        var ps = Ports.Where(x => x.Status == NodePortStatus.LiveNode).ToArray();
        if (ps.Length > 0)
        {
            port = ps[Random.Shared.Next(ps.Length)].Port;
            return true;
        }
        else
        {
            port = null;
            return false;
        }
    }
}

public record NodePort(
    int Port,
    NodePortStatus Status,
    string? Version
    );

public enum NodePortStatus
{
    LiveNode,
    LiveNodeWithDifferentVersion,
    Unavailable,
    Free,
}

public static class SwarmUtils
{
    public static (string? hostname, int? port) ParseHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host)) return (null, null);

        string hostname = "localhost";
        int? port = null;

        var s = host.Trim();

        {
            var i = s.IndexOf("://");
            if (i >= 0) s = s[(i + 3)..];
        }

        {
            var i = s.IndexOf('/');
            if (i >= 0) s = s[..i];
        }

        {
            if (s.EndsWith(':')) s = s[..^1];

            switch (s.IndexOf(':'))
            {
                case 0:
                    port = int.Parse(s[1..]);
                    break;
                case int i when i > 0:
                    hostname = s[..i];
                    port = int.Parse(s[(i + 1)..]);
                    break;
                default:
                    if (s.Length > 0) hostname = s;
                    break;
            }
        }

        return (hostname, port);
    }

    public static async Task<ISwarm?> TryGetLocalNodeAsync()
    {
        var host = "localhost";
        var probe = await ProbeHostAsync(host);
        if (!probe.TryGetLivePort(out var port)) return null;
        var url = $"http://{host}:{port}";
        return new NodeHttpClient(url);
    }

    public static async Task<ProbeResult> ProbeHostAsync(string hostname)
    {
        // probe localhost for running nodes to connect to ...
        using var http = new HttpClient()
        {
            Timeout = TimeSpan.FromSeconds(1)
        };

        var ports = await Task.WhenAll(
            Enumerable
                .Range(start: Info.DefaultPort, count: Info.DefaultPortRange)
                .Select(async port =>
                {
                    try
                    {
                        var response = await http.GetAsync($"http://{hostname}:{port}/version");
                        if (response.IsSuccessStatusCode)
                        {
                            var version = await response.Content.ReadAsStringAsync();
                            var isCurrentVersion = version == Info.Version;
                            return new NodePort(
                                Port   : port,
                                Status : isCurrentVersion ? NodePortStatus.LiveNode : NodePortStatus.LiveNodeWithDifferentVersion,
                                Version: version
                                );
                        }
                        else
                        {
                            return new NodePort(port, NodePortStatus.Unavailable, Version: null);
                        }
                    }
                    catch
                    {
                        return new NodePort(port, NodePortStatus.Free, Version: null);
                    }
                })
            );

        return new ProbeResult(Hostname: hostname, Ports: ports);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToJsonString(this object self, JsonSerializerOptions? jsonSerializerOptions = null)
        => JsonSerializer.Serialize(self, jsonSerializerOptions ?? JsonOptions);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T Deserialize<T>(object self)
        => (T)Deserialize(self, typeof(T));

    public static object Deserialize(object self, Type type) => self switch
    {
        object x when x.GetType() == type => x,

        JsonElement e =>
            JsonSerializer.Deserialize(e, type, JsonOptions)
            ?? throw new Exception(
                "Error 81a683b2-8fbc-4a3f-b967-37013883b05e."
                ),

        string s =>
            JsonSerializer.Deserialize(s, type, JsonOptions)
            ?? throw new Exception(
                $"Failed to deserialize JSON \"{s}\". " +
                $"Error a6a602f7-277a-4f76-9d80-8cbd2ecd78c1."
                ),

        null => throw new Exception(
            "Missing request. " +
            "Error b96a7af3-7f30-4aa9-a713-f36568e179a7."
            ),

        _ => throw new Exception(
            $"Unknown request object \"{self.GetType().FullName}\". " +
            $"Error d6f9dfbd-ed17-4ba8-94c7-afa7e1663cd3."
            )
    };

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        AllowTrailingCommas = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals | JsonNumberHandling.AllowReadingFromString,
        Converters =
        {
            new JsonStringEnumConverter(),
        },
    };
}

public static class Extensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Node> Except(this IEnumerable<Node> xs, Node n)
        => xs.Where(x => x.Id != n.Id);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Node> Except(this IEnumerable<Node> xs, string nodeId)
        => xs.Where(x => x.Id != nodeId);

    public static async Task SendEachAsync(this IEnumerable<Node> xs, Func<ISwarm, Task> action)
    {
        foreach (var x in xs)
        {
            try
            {
                await action(x.Client);
            }
            catch (Exception e)
            {
                AnsiConsole.MarkupLine(
                    $"[yellow][[WARNING]] SendEachAsync action failed for node {x.Id}[/].\n" +
                    $"{e.Message.EscapeMarkup()}"
                    );
            }
        }
    }
}