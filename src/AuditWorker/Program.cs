using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AuditWorker
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);
            builder.Services.AddHostedService<AuditLogWorker>();
            builder.Services.AddHostedService<PeripheralStatusWorker>();
            
            var app = builder.Build();
            app.Run();
        }
    }
}
