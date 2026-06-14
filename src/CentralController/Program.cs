using Microsoft.EntityFrameworkCore;

namespace CentralController
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            builder.Services.AddHostedService<EmqxBridgeConfigurator>();
            builder.Services.AddHostedService<DeviceFeedbackWorker>();

            var app = builder.Build();

            app.UseStaticFiles();

            app.MapGet("/ota/{fileName}", async (string fileName, HttpContext context) =>
            {
                var webRoot = app.Environment.WebRootPath ?? Path.Combine(Directory.GetCurrentDirectory(), "wwwroot");
                var filePath = Path.Combine(webRoot, "ota", fileName);
                if (!File.Exists(filePath))
                {
                    return Results.NotFound($"Firmware file {fileName} not found.");
                }
                return Results.File(filePath, "application/octet-stream", fileName);
            });

            app.MapGet("/devices", async () =>
            {
                using var db = new ControllerContext();
                var devices = await db.DeviceStatuses.ToListAsync();
                return Results.Ok(devices);
            });

            app.MapGet("/feedbacks", async () =>
            {
                using var db = new ControllerContext();
                var feedbacks = await db.CommandFeedbacks.OrderByDescending(x => x.Timestamp).Take(100).ToListAsync();
                return Results.Ok(feedbacks);
            });

            app.MapGet("/", () => "CentralController API is running.");

            app.Run();
        }
    }
}
