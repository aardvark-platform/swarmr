using Swarmr.Base;

namespace Swarmr.Tests;

public class JobTests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void ParseTest1()
    {
        var src = """
            SETUP sm/test/exe
            SETUP sm/test/data1

            EXECUTE 
              Sum.exe   # exe
              work 5    # args

            COLLECT .

            RESULT sm/test/work13
            """;

        var job = Jobs.Parse(src);
    }
}