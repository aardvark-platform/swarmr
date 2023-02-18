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
        //var src = """
        //    RUNNER helloworld

        //    HOSTDIR "T:\tmp\swarmr\helloworld"
        //    DATADIR data

        //    # specify data for runner
        //    # COPY <source> <target>
        //    # - <source> is file or directory <HOSTDIR>/<source>
        //    # - <target> is directory <DATADIR>/<target>
        //    # - where <DATADIR> will be made available at runtime relative to Environment.CurrentDirectory
        //    COPY input1.txt  .       # ... at path ./[DATADIR]/input1.txt
        //    COPY input2.txt  .       # ... at path ./[DATADIR]/input2.txt
        //    COPY "more data" .       # ... at path ./[DATADIR]/input1.txt

        //    RUN helloworld.exe 
        //    """;

        //Jobs.Parse(src);
    }
}