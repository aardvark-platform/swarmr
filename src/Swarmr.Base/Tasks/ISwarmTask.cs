namespace Swarmr.Base.Tasks;

public interface ISwarmTask
{
    string Id { get; }
    Task RunAsync(Swarm context);
}
