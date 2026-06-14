using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace AuditWorker
{
    public class PeripheralStatusWorker : BackgroundService
    {
        private readonly ILogger<PeripheralStatusWorker> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _workerName;
        private readonly string _rabbitmqHost;

        public PeripheralStatusWorker(ILogger<PeripheralStatusWorker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            var hostname = _configuration["HOSTNAME"];
            _workerName = !string.IsNullOrEmpty(hostname) ? hostname : $"worker-{Guid.NewGuid().ToString().Substring(0, 6)}";
            _rabbitmqHost = _configuration["RABBITMQ_HOST"] ?? "localhost";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"PeripheralStatusWorker {_workerName} starting...");

            // Ensure SQLite database directory exists and configure database in WAL mode
            try
            {
                Directory.CreateDirectory("data");
                using (var db = new PeripheralContext())
                {
                    try
                    {
                        await db.Database.EnsureCreatedAsync(stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"Peripheral Database creation skipped or failed (likely due to concurrent worker startup): {ex.Message}");
                    }
                    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;", stoppingToken);
                    _logger.LogInformation("Peripheral SQLite database initialized in WAL mode successfully.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to configure Peripheral SQLite database.");
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
                    _logger.LogInformation($"Received peripheral status message: {jsonStr}");

                    using var document = JsonDocument.Parse(jsonStr);
                    var root = document.RootElement;

                    string? deviceId = null;
                    int batteryLevel = 0;
                    bool sensorsOk = true;
                    DateTime timestamp = DateTime.UtcNow;

                    void ParseElement(JsonElement element)
                    {
                        if (element.TryGetProperty("device_id", out var devProp) && devProp.ValueKind == JsonValueKind.String)
                            deviceId = devProp.GetString();
                        if (element.TryGetProperty("battery_level", out var batProp))
                        {
                            if (batProp.ValueKind == JsonValueKind.Number)
                                batteryLevel = batProp.GetInt32();
                            else if (batProp.ValueKind == JsonValueKind.String && int.TryParse(batProp.GetString(), out var batVal))
                                batteryLevel = batVal;
                        }
                        if (element.TryGetProperty("sensors_ok", out var sensProp))
                        {
                            if (sensProp.ValueKind == JsonValueKind.True || sensProp.ValueKind == JsonValueKind.False)
                                sensorsOk = sensProp.GetBoolean();
                            else if (sensProp.ValueKind == JsonValueKind.String && bool.TryParse(sensProp.GetString(), out var sensVal))
                                sensorsOk = sensVal;
                        }
                        if (element.TryGetProperty("timestamp", out var tsProp))
                        {
                            if (tsProp.ValueKind == JsonValueKind.String && DateTime.TryParse(tsProp.GetString(), out var ts))
                                timestamp = ts;
                        }
                    }

                    // Try inner payload first
                    if (root.TryGetProperty("payload", out var payloadProp))
                    {
                        try
                        {
                            string? rawPayload = null;
                            if (payloadProp.ValueKind == JsonValueKind.String)
                                rawPayload = payloadProp.GetString();
                            else if (payloadProp.ValueKind == JsonValueKind.Object)
                                rawPayload = payloadProp.GetRawText();

                            if (!string.IsNullOrEmpty(rawPayload))
                            {
                                using var innerDoc = JsonDocument.Parse(rawPayload);
                                ParseElement(innerDoc.RootElement);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Failed to parse inner peripheral payload: {ex.Message}");
                        }
                    }

                    // Fallback/override with root fields if missing
                    if (string.IsNullOrEmpty(deviceId))
                    {
                        ParseElement(root);
                    }

                    var statusEntry = new PeripheralStatus
                    {
                        DeviceId = deviceId,
                        BatteryLevel = batteryLevel,
                        SensorsOk = sensorsOk,
                        Timestamp = timestamp,
                        WorkerName = _workerName
                    };

                    using (var db = new PeripheralContext())
                    {
                        db.PeripheralStatuses.Add(statusEntry);
                        await db.SaveChangesAsync(stoppingToken);
                        var count = await db.PeripheralStatuses.CountAsync(stoppingToken);
                        _logger.LogInformation($"Peripheral status persisted to SQLite (peripheral.db) by {_workerName}. Total count: {count}");
                    }

                    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: CancellationToken.None);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing or persisting peripheral status. Re-queuing...");
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

            await channel.BasicConsumeAsync(queue: "peripheral_queue", autoAck: false, consumer: consumer, cancellationToken: stoppingToken);
            _logger.LogInformation("Started consuming from 'peripheral_queue'.");

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                _logger.LogInformation("PeripheralStatusWorker is shutting down...");
            }
            finally
            {
                if (channel != null) await channel.CloseAsync(cancellationToken: CancellationToken.None);
                if (connection != null) await connection.CloseAsync(cancellationToken: CancellationToken.None);
            }
        }
    }
}
