using System.Text.Json;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public static class SwarmUtils
{
    public static string ToJsonString(this object self, JsonSerializerOptions? jsonSerializerOptions = null)
        => JsonSerializer.Serialize(self, jsonSerializerOptions ?? JsonOptions);

    public static T Deserialize<T>(object self) => self switch
    {
        T x => x,

        JsonElement e =>
            JsonSerializer.Deserialize<T>(e, JsonOptions)
            ?? throw new Exception(
                "Error 81a683b2-8fbc-4a3f-b967-37013883b05e."
                ),

        string s =>
            JsonSerializer.Deserialize<T>(s, JsonOptions)
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
