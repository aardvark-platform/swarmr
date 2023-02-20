using System.Collections.Immutable;
using static Swarmr.Base.JobConfig;

namespace Swarmr.Base;

/// <summary>
/// </summary>
/// <param name="Id">Job ID.</param>
/// <param name="Setup">Swarm files to extract into job dir.</param>
/// <param name="Execute">Command lines to execute.</param>
/// <param name="Results">Result files/dirs to save (relative to job dir).</param>
/// <param name="ResultFile">Swarm file name for saved results.</param>
public record JobConfig(
    string Id,
    IReadOnlyList<string>? Setup,
    IReadOnlyList<ExecuteItem>? Execute,
    IReadOnlyList<string>? Collect,
    string Result
    )
{
    public record ExecuteItem(string Exe, string Args);
}

public static class Jobs
{
    /*
    SETUP sm/test/exe
    SETUP sm/test/data1

    EXECUTE 
      Sum.exe   # exe
      work 5    # args

    COLLECT .

    RESULT sm/test/work13
    */

    public static JobConfig Parse(string src)
    {
        var lines = src
            .Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(StripComments)
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Select(TokenizeLine)
            .ToArray()
            ;

        var setup = ImmutableList<string>.Empty;
        var execute = ImmutableList<ExecuteItem>.Empty;
        var collect = ImmutableList<string>.Empty;
        string? result = null;

        for (var i = 0; i < lines.Length; i++)
        {
            var (line, tokens) = lines[i];
            switch (tokens[0].ToLower())
            {
                case "setup":
                    {
                        if (tokens.Count != 2) throw new Exception();
                        setup = setup.Add(tokens[1]);
                        break;
                    }

                case "execute":
                    {
                        var x = new ExecuteItem(
                            Exe: lines[++i].line,
                            Args: lines[++i].line
                            );
                        execute = execute.Add(x);
                        break;
                    }

                case "collect":
                    {
                        if (tokens.Count != 2) throw new Exception();
                        collect = collect.Add(tokens[1]);
                        break;
                    }

                case "result":
                    {
                        if (tokens.Count != 2) throw new Exception();
                        if (result != null) throw new Exception("Result already defined.");
                        result = tokens[1];
                        break;
                    }
            }
        }

        if (result == null) throw new Exception("Result is undefined.");

        var job = new JobConfig(
            Id: $"job-{Guid.NewGuid()}",
            Setup: setup,
            Execute: execute,
            Collect: collect,
            Result: result
            );

        return job;

        static string StripComments(string line)
        {
            line = line.Trim();
            var i = line.IndexOf('#');
            if (i >= 0) line = line[..i];
            return line;
        }

        static (string line, ImmutableList<string> tokens) TokenizeLine(string line)
        {
            var tokens = ImmutableList<string>.Empty;
            var token = "";
            var insideQuotes = false;
            for (var i = 0; i < line.Length; i++)
            {
                var c = line[i];

                if (char.IsWhiteSpace(c) && token.Length == 0)
                {
                    continue;
                }

                if (c == '\"')
                {
                    insideQuotes = !insideQuotes;
                    continue;
                }

                if (insideQuotes)
                {
                    token += c;
                    continue;
                }

                if (char.IsWhiteSpace(c))
                {
                    tokens = tokens.Add(token);
                    token = "";
                    continue;
                }

                token += c;
            }

            if (token.Length > 0)
            {
                tokens = tokens.Add(token);
            }

            return (line, tokens);
        }
    }
}