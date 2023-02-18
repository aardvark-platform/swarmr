using System.Globalization;

CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;

Console.WriteLine($"stdout log");
Console.WriteLine($"start at {DateTimeOffset.Now}");
Console.WriteLine($"args: {string.Join(' ', args)}");

var delay = TimeSpan.FromSeconds(7 + Random.Shared.Next(10));
Console.WriteLine($"[{DateTimeOffset.Now}] simulating work for {delay}");
await Task.Delay(delay);
Console.WriteLine($"[{DateTimeOffset.Now}] simulated work done");

switch (args[0])
{
    case "create":
        {
            var n = int.Parse(args[1]);
            for (var i = 0; i < n; i++)
            {
                var x = Math.Round(Random.Shared.NextDouble() * 999.999, 3);
                Console.WriteLine(x);
            }
            break;
        }

    default:
        {
            var xs = File
                .ReadAllLines(args[0])
                .Select(line => decimal.Parse(line))
                .ToArray()
                ;

            if (args.Length > 1)
            {
                using var stdout = new StreamWriter(File.OpenWrite(args[1]));
                stdout.WriteLine($"parsed {xs.Length} values");
                stdout.WriteLine($"sum is {xs.Sum()}");
            }
            else
            {
                Console.WriteLine($"parsed {xs.Length} values");
                Console.WriteLine($"sum is {xs.Sum()}");
            }
            break;
        }
}

{
    var logdir = new DirectoryInfo("logs");
    logdir.Create();
    using var logfile = new StreamWriter(File.OpenWrite(Path.Combine(logdir.FullName, "log.txt")));
    logfile.WriteLine($"created {DateTimeOffset.Now}");
    logfile.WriteLine($"some log file entry");
    logfile.WriteLine($"another entry");
    logfile.WriteLine($"hello world");
}