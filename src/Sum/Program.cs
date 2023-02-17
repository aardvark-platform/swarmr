using System.Globalization;

CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;

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
            Console.WriteLine($"parsed {xs.Length} values");
            Console.WriteLine($"sum is {xs.Sum()}");
            break;
        }
}

