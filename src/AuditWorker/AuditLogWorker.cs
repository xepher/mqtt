using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace AuditWorker
{
    public class AuditLogWorker : BackgroundService
    {
        private readonly ILogger<AuditLogWorker> _logger;
        private readonly string _workerName;
        private readonly string _rabbitmqHost;

        public AuditLogWorker(ILogger<AuditLogWorker> logger)
        {
            _logger = logger;
            _workerName = Environment.GetEnvironmentVariable("HOSTNAME") ?? $"worker-{Guid.NewGuid().ToString().Substring(0, 6)}";
            _rabbitmqHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"AuditLogWorker {_workerName} starting...");

            // Ensure SQLite database directory exists and configure database in WAL mode
            try
            {
                Directory.CreateDirectory("data");
                using (var db = new AuditContext())
                {
                    try
                    {
                        await db.Database.EnsureCreatedAsync(stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"Database creation skipped or failed (likely due to concurrent worker startup): {ex.Message}");
                    }
                    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;", stoppingToken);
                    _logger.LogInformation("SQLite database initialized in WAL mode successfully.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to configure SQLite database.");
                throw;
            }

            IConnection connection = null;
            IChannel channel = null;
            var factory = new ConnectionFactory { HostName = _rabbitmqHost };

            int retryCount = 0;
            while (connection == null && !stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation($"Connecting to RabbitMQ at {_rabbitmqHost}...");
                    connection = await factory.CreateConnectionAsync(stoppingToken);
                    channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
                    _logger.LogInformation("Connected to RabbitMQ successfully.");
                }
                catch (Exception ex)
                {
                    retryCount++;
                    _logger.LogWarning($"RabbitMQ connection attempt {retryCount} failed: {ex.Message}. Retrying in 5 seconds...");
                    try
                    {
                        await Task.Delay(5000, stoppingToken);
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                }
            }

            if (stoppingToken.IsCancellationRequested)
            {
                if (channel != null) await channel.CloseAsync(cancellationToken: CancellationToken.None);
                if (connection != null) await connection.CloseAsync(cancellationToken: CancellationToken.None);
                return;
            }

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var jsonStr = Encoding.UTF8.GetString(body);
                    _logger.LogInformation($"Received audit log message: {jsonStr}");

                    using var document = JsonDocument.Parse(jsonStr);
                    var root = document.RootElement;

                    string deviceId = null;
                    string user = null;
                    string action = null;
                    string result = null;
                    DateTime timestamp = DateTime.UtcNow;

                    if (root.TryGetProperty("payload", out var payloadProp))
                    {
                        try
                        {
                            string rawPayload = null;
                            if (payloadProp.ValueKind == JsonValueKind.String)
                            {
                                rawPayload = payloadProp.GetString();
                            }
                            else if (payloadProp.ValueKind == JsonValueKind.Object)
                            {
                                rawPayload = payloadProp.GetRawText();
                            }

                            if (!string.IsNullOrEmpty(rawPayload))
                            {
                                using var innerDoc = JsonDocument.Parse(rawPayload);
                                var innerRoot = innerDoc.RootElement;

                                deviceId = innerRoot.TryGetProperty("device_id", out var devProp) ? devProp.GetString() : null;
                                user = innerRoot.TryGetProperty("user", out var usrProp) ? usrProp.GetString() : null;
                                action = innerRoot.TryGetProperty("action", out var actProp) ? actProp.GetString() : null;
                                result = innerRoot.TryGetProperty("result", out var resProp) ? resProp.GetString() : null;
                                
                                if (innerRoot.TryGetProperty("timestamp", out var tsProp))
                                {
                                    if (tsProp.ValueKind == JsonValueKind.String && DateTime.TryParse(tsProp.GetString(), out var ts))
                                    {
                                        timestamp = ts;
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Failed to parse inner payload: {ex.Message}");
                        }
                    }

                    // Fallback to root properties if payload parsing didn't work or if properties are at root
                    if (string.IsNullOrEmpty(deviceId))
                        deviceId = root.TryGetProperty("device_id", out var rootDevProp) && rootDevProp.ValueKind == JsonValueKind.String ? rootDevProp.GetString() : null;
                    if (string.IsNullOrEmpty(user))
                        user = root.TryGetProperty("user", out var rootUsrProp) && rootUsrProp.ValueKind == JsonValueKind.String ? rootUsrProp.GetString() : null;
                    if (string.IsNullOrEmpty(action))
                        action = root.TryGetProperty("action", out var rootActProp) && rootActProp.ValueKind == JsonValueKind.String ? rootActProp.GetString() : null;
                    if (string.IsNullOrEmpty(result))
                        result = root.TryGetProperty("result", out var rootResProp) && rootResProp.ValueKind == JsonValueKind.String ? rootResProp.GetString() : null;

                    var logEntry = new AuditLog
                    {
                        DeviceId = deviceId,
                        User = user,
                        Action = action,
                        Result = result,
                        Timestamp = timestamp,
                        WorkerName = _workerName
                    };

                    using (var db = new AuditContext())
                    {
                        db.AuditLogs.Add(logEntry);
                        int saved = await db.SaveChangesAsync(stoppingToken);
                        _logger.LogInformation($"db.SaveChangesAsync returned: {saved}");
                        
                        var count = await db.AuditLogs.CountAsync(stoppingToken);
                        _logger.LogInformation($"Current count inside worker context: {count}");
                    }

                    _logger.LogInformation($"Audit log persisted to SQLite by {_workerName}.");

                    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: CancellationToken.None);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing or persisting audit log. Re-queuing...");
                    try
                    {
                        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true, cancellationToken: CancellationToken.None);
                    }
                    catch (Exception nackEx)
                    {
                        _logger.LogError(nackEx, "Failed to nack message.");
                    }
                }
            };

            await channel.BasicConsumeAsync(queue: "audit_queue", autoAck: false, consumer: consumer, cancellationToken: stoppingToken);
            _logger.LogInformation("Started consuming from 'audit_queue'.");

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                _logger.LogInformation("AuditLogWorker is shutting down...");
            }
            finally
            {
                if (channel != null) await channel.CloseAsync(cancellationToken: CancellationToken.None);
                if (connection != null) await connection.CloseAsync(cancellationToken: CancellationToken.None);
            }
        }
    }
}
