using Swarmr.Base;
using Swarmr.Base.Api;

namespace swarmr;

public class Server
{
    public static Task RunAsync(Swarm swarm, int port)
    {
        var args = Environment.GetCommandLineArgs();
        var builder = WebApplication.CreateBuilder(args);

        //builder.Services.AddSingleton<SwarmrHub>(_ => new SwarmrHub(server, configLogRequests));

        builder.Services.AddSignalR();
        builder.Services.AddCors(options =>
        {
            options.AddDefaultPolicy(
                policy =>
                {
                    policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
                });
        });

        var app = builder.Build();

        {
            builder.WebHost.UseUrls($"http://*:{port}");
            for (var i = 0; i < args.Length; i++)
            {
                if (args[i] == "--urls" && ++i < args.Length)
                {
                    builder.WebHost.UseUrls(args[i]);
                }
            }
        }

        app.UseCors();

        app.MapGet("/", () => $"Swarmr {Info.Version}");

        app.MapPost("/api", async (HttpContext context) =>
        {
            var request =
                await context.Request.ReadFromJsonAsync<SwarmRequest>() 
                ?? throw new Exception("Error df347207-7548-452e-a3f9-8224c3e6ef01.");

            var response = await swarm.RequestAsync(request);

            return Results.Json(response);
        });

        //app.MapHub<SwarmrHub>("/swarmrhub");

        //{
        //    var hubContext =
        //        app.Services.GetService<IHubContext<SwarmrHub, ISwarmrHubClient>>()
        //        ?? throw new Exception("Error 1afb2552-8875-4352-881f-750ef33961a8.")
        //    ;

        //    await server.OnUpdate(async p => await hubContext.Clients.All.OnUpdated(p));
        //}

        return app.RunAsync();
    }
}
    