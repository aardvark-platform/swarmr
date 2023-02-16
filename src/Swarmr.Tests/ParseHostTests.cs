using Swarmr.Base;

namespace Swarmr.Tests;

public class ParseHostTests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void Test0()
    {
        var (hostname, port) = SwarmUtils.ParseHost(null);
        Assert.True(hostname == null && port == null);
    }

    [Test]
    public void Test1()
    {
        var (hostname, port) = SwarmUtils.ParseHost("");
        Assert.True(hostname == null && port == null);
    }

    [Test]
    public void Test1a()
    {
        var (hostname, port) = SwarmUtils.ParseHost("   ");
        Assert.True(hostname == null && port == null);
    }

    [Test]
    public void Test2()
    {
        var (hostname, port) = SwarmUtils.ParseHost("myhost");
        Assert.True(hostname == "myhost" && port == null);
    }

    [Test]
    public void Test3()
    {
        var (hostname, port) = SwarmUtils.ParseHost("myhost:");
        Assert.True(hostname == "myhost" && port == null);
    }

    [Test]
    public void Test4()
    {
        var (hostname, port) = SwarmUtils.ParseHost("http://myhost");
        Assert.True(hostname == "myhost" && port == null);
    }

    [Test]
    public void Test4a()
    {
        var (hostname, port) = SwarmUtils.ParseHost("https://myhost");
        Assert.True(hostname == "myhost" && port == null);
    }

    [Test]
    public void Test4b()
    {
        var (hostname, port) = SwarmUtils.ParseHost("http://:6000");
        Assert.True(hostname == "localhost" && port == 6000);
    }

    [Test]
    public void Test5()
    {
        var (hostname, port) = SwarmUtils.ParseHost(":6000");
        Assert.True(hostname == "localhost" && port == 6000);
    }

    [Test]
    public void Test6()
    {
        var (hostname, port) = SwarmUtils.ParseHost("myhost:6000");
        Assert.True(hostname == "myhost" && port == 6000);
    }

    [Test]
    public void Test7()
    {
        var (hostname, port) = SwarmUtils.ParseHost("myhost:6000/");
        Assert.True(hostname == "myhost" && port == 6000);
    }

    [Test]
    public void Test8()
    {
        var (hostname, port) = SwarmUtils.ParseHost("myhost:6000/abc");
        Assert.True(hostname == "myhost" && port == 6000);
    }

    [Test]
    public void Test9()
    {
        var (hostname, port) = SwarmUtils.ParseHost("myhost:6000/abc/d");
        Assert.True(hostname == "myhost" && port == 6000);
    }
}