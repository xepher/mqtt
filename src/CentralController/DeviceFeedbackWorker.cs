using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace CentralController
{
    public class DeviceFeedbackWorker : BackgroundService
    {
        private readonly ILogger<DeviceFeedbackWorker> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _rabbitmqHost;

        public DeviceFeedbackWorker(ILogger<DeviceFeedbackWorker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _rabbitmqHost = _configuration["RABBITMQ_HOST"] ?? "localhost";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("DeviceFeedbackWorker starting...");

            // Ensure SQLite database directory exists and configure database in WAL mode
            try
            {
                Directory.CreateDirectory("data");
                using (var db = new ControllerContext())
                {
                    try
                    {
                        await db.Database.EnsureCreatedAsync(stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"Controller Database creation skipped or failed: {ex.Message}");
                    }
                    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;", stoppingToken);
                    _logger.LogInformation("Controller SQLite database initialized in WAL mode successfully.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to configure Controller SQLite database.");
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
                    
                    bool isStatus = ea.RoutingKey.EndsWith("status.up");
                    var parts = ea.RoutingKey.Split('.');
                    string deviceId = parts.Length >= 3 ? parts[2] : "unknown";

                    if (isStatus)
                    {
                        string statusValue = jsonStr.Trim('\"', ' ', '\n', '\r');
                        bool isOnline = statusValue.Equals("online", StringComparison.OrdinalIgnoreCase);
                        _logger.LogInformation($"Device status update: DeviceId={deviceId}, Online={isOnline}");

                        using (var db = new ControllerContext())
                        {
                            var device = await db.DeviceStatuses.FirstOrDefaultAsync(x => x.DeviceId == deviceId, stoppingToken);
                            if (device == null)
                            {
                                device = new DeviceStatus
                                {
                                    DeviceId = deviceId,
                                    IsOnline = isOnline,
                                    LastSeen = DateTime.UtcNow
                                };
                                db.DeviceStatuses.Add(device);
                            }
                            else
                            {
                                device.IsOnline = isOnline;
                                device.LastSeen = DateTime.UtcNow;
                            }
                            await db.SaveChangesAsync(stoppingToken);
                        }
                    }
                    else
                    {
                        _logger.LogInformation($"Received device command feedback: Topic={ea.RoutingKey}, Payload={jsonStr}");
                        
                        var feedbackEntry = new CommandFeedback
                        {
                            DeviceId = deviceId,
                            Topic = ea.RoutingKey,
                            Payload = jsonStr,
                            Timestamp = DateTime.UtcNow
                        };

                        using (var db = new ControllerContext())
                        {
                            db.CommandFeedbacks.Add(feedbackEntry);
                            await db.SaveChangesAsync(stoppingToken);
                        }
                    }

                    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: CancellationToken.None);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing or persisting device feedback. Re-queuing...");
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

            await channel.BasicConsumeAsync(queue: "controller_status_queue", autoAck: false, consumer: consumer, cancellationToken: stoppingToken);
            await channel.BasicConsumeAsync(queue: "controller_feedback_queue", autoAck: false, consumer: consumer, cancellationToken: stoppingToken);
            _logger.LogInformation("Started consuming from 'controller_status_queue' and 'controller_feedback_queue'.");

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                _logger.LogInformation("DeviceFeedbackWorker is shutting down...");
            }
            finally
            {
                if (channel != null) await channel.CloseAsync(cancellationToken: CancellationToken.None);
                if (connection != null) await connection.CloseAsync(cancellationToken: CancellationToken.None);
            }
        }
    }
}
