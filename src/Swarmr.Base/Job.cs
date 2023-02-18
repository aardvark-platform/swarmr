using static Swarmr.Base.JobConfig;

namespace Swarmr.Base;

/// <summary>
/// </summary>
/// <param name="Setup">Swarm files to extract into job dir.</param>
/// <param name="Execute">Command lines to execute.</param>
/// <param name="Results">Result files/dirs to save (relative to job dir).</param>
/// <param name="ResultFile">Swarm file name for saved results.</param>
public record JobConfig(
    IReadOnlyList<string>? Setup,
    IReadOnlyList<ExeConfig>? Execute,
    IReadOnlyList<string>? Collect,
    string ResultFile
    )
{
    public record ExeConfig(string Exe, string Args);
}

public static class Jobs
{
    /*
    
    RUNNER helloworld

    HOSTDIR "T:\tmp\swarmr\helloworld"
    DATADIR data

    # specify data for runner
    # COPY <source> <target>
    # - <source> is file or directory <HOSTDIR>/<source>
    # - <target> is directory <DATADIR>/<target>
    # - where <DATADIR> will be made available at runtime relative to Environment.CurrentDirectory
    COPY input1.txt  .       # ... at path ./[DATADIR]/input1.txt
    COPY input2.txt  .       # ... at path ./[DATADIR]/input2.txt
    COPY "more data" .       # ... at path ./[DATADIR]/input1.txt

    RUN helloworld.exe 

    */

    //public static void Parse(string src)
    //{
    //    var lines = src
    //        .Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
    //        .Select(StripComments)
    //        .Where(line => !string.IsNullOrWhiteSpace(line))
    //        .Select(TokenizeLine)
    //        .ToArray()
    //        ;

    //    static string StripComments(string line)
    //    {
    //        line = line.Trim();
    //        var i = line.IndexOf('#');
    //        if (i >= 0) line = line[..i];
    //        return line;
    //    }

    //    static ImmutableList<string> TokenizeLine(string line)
    //    {
    //        var tokens = ImmutableList<string>.Empty;
    //        var token = "";
    //        var insideQuotes = false;
    //        for (var i = 0; i < line.Length; i++)
    //        {
    //            var c = line[i];

    //            if (char.IsWhiteSpace(c) && token.Length == 0)
    //            {
    //                continue;
    //            }

    //            if (c == '\"')
    //            {
    //                insideQuotes = !insideQuotes;
    //                continue;
    //            }

    //            if (insideQuotes)
    //            {
    //                token += c;
    //                continue;
    //            }

    //            if (char.IsWhiteSpace(c))
    //            {
    //                tokens = tokens.Add(token);
    //                token = "";
    //                continue;
    //            }

    //            token += c;
    //        }

    //        if (token.Length > 0)
    //        {
    //            tokens = tokens.Add(token);
    //        }

    //        return tokens;
    //    }
    //}
}