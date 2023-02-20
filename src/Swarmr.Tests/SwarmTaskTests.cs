using Swarmr.Base;
using Swarmr.Base.Tasks;

namespace Swarmr.Tests;

public class SwarmTaskTests
{
    [SetUp]
    public void Setup()
    {
    }

    private record TestTask1(string Id, string Name) : ISwarmTask
    {
        public Task RunAsync(Swarm context) => Task.CompletedTask;
    }

    [Test]
    public void Roundtrip()
    {
        var a = new TestTask1(
            Id: Guid.NewGuid().ToString(),
            Name: $"name-{Guid.NewGuid()}"
            );

        var s = SwarmTask.ToJsonString(a);
        var b = SwarmTask.Deserialize<TestTask1>(s);
        Assert.True(a.Id == b.Id);
        Assert.True(a.Name == b.Name);
    }
}