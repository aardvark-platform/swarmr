using Swarmr.Base;
using Swarmr.Base.Tasks;

namespace Swarmr.Tests;

public class SwarmTaskTests
{
    [SetUp]
    public void Setup()
    {
    }

    private record TestTask1(string Name) : ISwarmTask
    {
        public Task RunAsync(Swarm context) => Task.CompletedTask;
    }

    [Test]
    public void Roundtrip()
    {
        var a = new TestTask1(Name: Guid.NewGuid().ToString());
        var s = SwarmTask.ToJsonString(a);
        var b = SwarmTask.Deserialize<TestTask1>(s);
        Assert.True(a.Name == b.Name);
    }
}