namespace Swarmr.Base.Tasks;

public interface ISwarmTask
{
    Task RunAsync(Swarm context);
}
