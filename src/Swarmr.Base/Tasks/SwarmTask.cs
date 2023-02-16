using Swarmr.Base.Tasks;
using System.Text.Json;

namespace Swarmr.Base;

public static class SwarmTask
{
    public record Dto(string Type, object Task);

    public static Dto ToDto(this ISwarmTask self)
        => new(Type: self.GetType().AssemblyQualifiedName!, Task: self);

    public static string ToJsonString(this ISwarmTask self)
        => self.ToDto().ToJsonString();

    public static ISwarmTask Deserialize(object o)
    {
        var dto = SwarmUtils.Deserialize<Dto>(o);

        var t = Type.GetType(dto.Type) ?? throw new Exception(
            $"Failed to get type {dto.Type}. " +
            $"Error 80b46342-a976-43d6-a03c-0814697fc64c."
            );

        return dto.Task switch
        {
            ISwarmTask x => x,
            JsonElement x => (ISwarmTask)x.Deserialize(t, SwarmUtils.JsonOptions)!,
            string x => (ISwarmTask)JsonSerializer.Deserialize(x, t, SwarmUtils.JsonOptions)!,
            _ => throw new Exception(
                $"Failed to create ISwarmTask from {dto.Task.GetType().FullName}. " +
                $"Error 5eb081f8-0bc2-43da-b16e-e52426760411."
                )
        };
    }

    public static T Deserialize<T>(object o) where T : ISwarmTask
        => (T)Deserialize(o);
}
