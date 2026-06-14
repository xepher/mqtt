using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MQTTnet;
using Polly.Retry;
using System.Buffers;
using System.Text;
using System.Text.Json;

namespace IoTDevice
{
    public class Program
    {
        private static string _deviceId = string.Empty;
        private static string _mqttHost = string.Empty;
        private static string _otaUrl = string.Empty;
        private static IMqttClient? _mqttClient;
        private static readonly HttpClient _httpClient = new HttpClient();
        private static readonly Random _random = new Random();

        // Topic Constants
        private const string TopicPrefix = "mqttsystem/org1";
        private const string StatusUpSuffix = "/status/up";
        private const string ControlDownSuffix = "/control/down";
        private const string ControlUpSuffix = "/control/up";
        private const string ConfigDownSuffix = "/config/down";
        private const string ConfigUpSuffix = "/config/up";
        private const string OtaDownSuffix = "/ota/down";
        private const string OtaUpSuffix = "/ota/up";
        private const string TelemetryUpSuffix = "/telemetry/up";
        private const string LogsUpSuffix = "/logs/up";
        private const string MetricsUpSuffix = "/metrics/up";
        private const string PeripheralUpSuffix = "/peripheral/up";
        private const string AuditUpSuffix = "/audit/up";

        private static string BuildTopic(string suffix) => $"{TopicPrefix}/{_deviceId}{suffix}";

        public static async Task Main(string[] args)
        {
            // Load configuration
            var environmentName = Environment.GetEnvironmentVariable("NETCORE_ENVIRONMENT") ?? "Production";
            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables();
            
            var configuration = configBuilder.Build();

            _deviceId = configuration["DEVICE_ID"] ?? string.Empty;
            if (string.IsNullOrEmpty(_deviceId))
            {
                _deviceId = $"device-{Guid.NewGuid().ToString().Substring(0, 6)}";
            }
            _mqttHost = configuration["MQTT_HOST"] ?? "localhost";
            _otaUrl = configuration["OTA_URL"] ?? "http://localhost/ota/firmware-v1.2.0.bin";

            // Setup DI container
            var services = new ServiceCollection();
            services.Configure<RetryStrategyOptions>(configuration.GetSection("MqttRetryStrategy"));
            services.AddSingleton<IMqttClient>(sp => new MqttClientFactory().CreateMqttClient());
            services.AddSingleton<ResilientMqttClient>();
            var serviceProvider = services.BuildServiceProvider();

            var retryOptions = serviceProvider.GetRequiredService<IOptions<RetryStrategyOptions>>().Value;

            Console.WriteLine($"Starting IoT Device simulator: {_deviceId}");
            Console.WriteLine($"MQTT Host: {_mqttHost}");
            Console.WriteLine($"OTA URL: {_otaUrl}");
            Console.WriteLine($"Polly config: BaseDelay={retryOptions.Delay.TotalSeconds}s, " +
                              $"MaxDelay={(retryOptions.MaxDelay.HasValue ? retryOptions.MaxDelay.Value.TotalSeconds.ToString() : "N/A")}s, " +
                              $"MaxRetryAttempts={retryOptions.MaxRetryAttempts}, " +
                              $"BackoffType={retryOptions.BackoffType}, " +
                              $"UseJitter={retryOptions.UseJitter}");

            _mqttClient = serviceProvider.GetRequiredService<ResilientMqttClient>();

            // Configure MQTT connection options with LWT
            var options = new MqttClientOptionsBuilder()
                .WithClientId(_deviceId)
                .WithTcpServer(_mqttHost, 1883)
                .WithWillPayload("offline")
                .WithWillTopic(BuildTopic(StatusUpSuffix))
                .WithWillQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithWillRetain(true)
                .WithCleanSession(true)
                .Build();

            // Initialize global cancellation source early to cancel connection retries on exit
            var cts = new CancellationTokenSource();

            // Setup message received handler
            _mqttClient.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;

            // Setup reconnect loop on disconnect
            _mqttClient.DisconnectedAsync += async e =>
            {
                Console.WriteLine($"Disconnected from MQTT broker. Reason: {e.ReasonString}. Reconnecting via Polly...");
                try
                {
                    await ConnectAndSubscribeAsync(options, cts.Token);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Reconnection process failed or stopped: {ex.Message}");
                }
            };

            // Connect for the first time in the background
            _ = Task.Run(async () =>
            {
                try
                {
                    await ConnectAndSubscribeAsync(options, cts.Token);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Initial connection/subscription failed: {ex.Message}. Will retry in the background via Polly.");
                }
            });

            // Periodic reporting loop
            _ = Task.Run(() => PeriodicPublishLoopAsync(cts.Token));

            // Wait for exit
            var exitEvent = new ManualResetEvent(false);
            AppDomain.CurrentDomain.ProcessExit += (sender, e) =>
            {
                Console.WriteLine("Exiting IoT Device...");
                cts.Cancel();
                exitEvent.Set();
            };

            exitEvent.WaitOne();
        }

        private static async Task ConnectAndSubscribeAsync(MqttClientOptions options, CancellationToken token)
        {
            if (_mqttClient == null) return;
            if (!_mqttClient.IsConnected)
            {
                var result = await _mqttClient.ConnectAsync(options, token);
                if (result.ResultCode != MqttClientConnectResultCode.Success)
                {
                    Console.WriteLine($"Failed to connect to MQTT broker: {result.ResultCode} ({result.ReasonString})");
                    return;
                }
                Console.WriteLine("Connected to MQTT broker successfully!");

                // Publish Birth message
                var birthMessage = new MqttApplicationMessageBuilder()
                    .WithTopic(BuildTopic(StatusUpSuffix))
                    .WithPayload("online")
                    .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithRetainFlag(true)
                    .Build();

                await _mqttClient.PublishAsync(birthMessage, token);
                Console.WriteLine($"Published birth message to {BuildTopic(StatusUpSuffix)}");

                // Subscribe to control, config, and ota down topics
                var subOptions = new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(BuildTopic(ControlDownSuffix))
                    .WithTopicFilter(BuildTopic(ConfigDownSuffix))
                    .WithTopicFilter(BuildTopic(OtaDownSuffix))
                    .Build();

                await _mqttClient.SubscribeAsync(subOptions, token);
                Console.WriteLine("Subscribed to command & control topics.");
            }
        }

        private static async Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
        {
            var topic = args.ApplicationMessage.Topic;
            var payloadBytes = args.ApplicationMessage.Payload.ToArray();
            var payload = Encoding.UTF8.GetString(payloadBytes);
            Console.WriteLine($"Received message on {topic}: {payload}");

            try
            {
                switch (topic)
                {
                    case string t when t.EndsWith(ControlDownSuffix):
                        // Command control
                        Console.WriteLine($"Executing control command: {payload}");
                        var ackTopic = BuildTopic(ControlUpSuffix);
                        var ackPayload = JsonSerializer.Serialize(new
                        {
                            device_id = _deviceId,
                            status = "completed",
                            command_received = payload,
                            timestamp = DateTime.UtcNow.ToString("o")
                        });
                        
                        await PublishMessageAsync(ackTopic, ackPayload);
                        break;

                    case string t when t.EndsWith(ConfigDownSuffix):
                        // Config change
                        Console.WriteLine($"Applying new configuration: {payload}");
                        var configAckTopic = BuildTopic(ConfigUpSuffix);
                        var configAckPayload = JsonSerializer.Serialize(new
                        {
                            device_id = _deviceId,
                            config_applied = true,
                            timestamp = DateTime.UtcNow.ToString("o")
                        });
                        
                        await PublishMessageAsync(configAckTopic, configAckPayload);
                        break;

                    case string t when t.EndsWith(OtaDownSuffix):
                        // OTA trigger
                        Console.WriteLine("OTA Upgrade command received. Starting download...");
                        var otaUpTopic = BuildTopic(OtaUpSuffix);
                        await PublishMessageAsync(otaUpTopic, JsonSerializer.Serialize(new
                        {
                            device_id = _deviceId,
                            status = "downloading",
                            timestamp = DateTime.UtcNow.ToString("o")
                        }));

                        try
                        {
                            await PerformOtaDownloadAsync();
                            Console.WriteLine("OTA Download completed successfully!");

                            await PublishMessageAsync(otaUpTopic, JsonSerializer.Serialize(new
                            {
                                device_id = _deviceId,
                                status = "success",
                                details = "Firmware updated to 1.2.0",
                                timestamp = DateTime.UtcNow.ToString("o")
                            }));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"OTA Download failed: {ex.Message}");
                            await PublishMessageAsync(otaUpTopic, JsonSerializer.Serialize(new
                            {
                                device_id = _deviceId,
                                status = "failed",
                                error = ex.Message,
                                timestamp = DateTime.UtcNow.ToString("o")
                            }));
                        }
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling message: {ex.Message}");
            }
        }

        private static async Task PerformOtaDownloadAsync()
        {
            Console.WriteLine($"Downloading firmware from {_otaUrl}...");
            var response = await _httpClient.GetAsync(_otaUrl);
            response.EnsureSuccessStatusCode();

            var directoryPath = "downloads";
            Directory.CreateDirectory(directoryPath);

            var fileName = Path.GetFileName(_otaUrl);
            var filePath = Path.Combine(directoryPath, fileName);

            using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await response.Content.CopyToAsync(fs);
            }

            Console.WriteLine($"Saved firmware binary to {filePath}");
        }

        private static async Task PeriodicPublishLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (_mqttClient != null && _mqttClient.IsConnected)
                {
                    try
                    {
                        // 1. Telemetry (QoS 0)
                        var temp = 20.0 + _random.NextDouble() * 5.0; // 20 - 25 C
                        var hum = 50.0 + _random.NextDouble() * 10.0; // 50 - 60 %
                        var telemetryPayload = JsonSerializer.Serialize(new
                        {
                            device_id = _deviceId,
                            temperature = temp,
                            humidity = hum,
                            status = "active",
                            timestamp = DateTime.UtcNow.ToString("o")
                        });
                        await PublishMessageAsync(BuildTopic(TelemetryUpSuffix), telemetryPayload, qos: 0);

                        // 2. Logs (QoS 0)
                        var logPayload = JsonSerializer.Serialize(new
                        {
                            device_id = _deviceId,
                            level = _random.Next(10) > 8 ? "warn" : "info",
                            message = $"Telemetry report generated successfully. Temp={temp:F2}, Hum={hum:F2}",
                            timestamp = DateTime.UtcNow.ToString("o")
                        });
                        await PublishMessageAsync(BuildTopic(LogsUpSuffix), logPayload, qos: 0);

                        // 3. Metrics (QoS 0)
                        var cpu = _random.NextDouble() * 25.0 + 5.0; // 5 - 30 %
                        var mem = _random.NextDouble() * 20.0 + 30.0; // 30 - 50 %
                        var metricsPayload = JsonSerializer.Serialize(new
                        {
                            device_id = _deviceId,
                            cpu_usage = cpu,
                            memory_usage = mem,
                            timestamp = DateTime.UtcNow.ToString("o")
                        });
                        await PublishMessageAsync(BuildTopic(MetricsUpSuffix), metricsPayload, qos: 0);

                        // 4. Peripheral (QoS 1, occasionally)
                        if (_random.Next(5) == 0)
                        {
                            var peripheralPayload = JsonSerializer.Serialize(new
                            {
                                device_id = _deviceId,
                                battery_level = 100 - (_random.Next(10)),
                                sensors_ok = true,
                                timestamp = DateTime.UtcNow.ToString("o")
                            });
                            await PublishMessageAsync(BuildTopic(PeripheralUpSuffix), peripheralPayload, qos: 1);
                        }

                        // 5. Audit logs (QoS 1, occasionally)
                        if (_random.Next(10) == 0)
                        {
                            var auditPayload = JsonSerializer.Serialize(new
                            {
                                device_id = _deviceId,
                                user = "operator_sim",
                                action = "health_check_ping",
                                result = "success",
                                timestamp = DateTime.UtcNow.ToString("o")
                            });
                            await PublishMessageAsync(BuildTopic(AuditUpSuffix), auditPayload, qos: 1);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error publishing periodic reports: {ex.Message}");
                    }
                }

                await Task.Delay(5000, token);
            }
        }

        private static async Task PublishMessageAsync(string topic, string payload, int qos = 1, bool retain = false)
        {
            if (_mqttClient == null || !_mqttClient.IsConnected)
            {
                Console.WriteLine($"[Warning] Cannot publish to {topic}: MQTT client is not connected.");
                return;
            }

            var mqttQos = qos switch
            {
                0 => MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
                2 => MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
                _ => MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce
            };

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(mqttQos)
                .WithRetainFlag(retain)
                .Build();

            await _mqttClient.PublishAsync(message, CancellationToken.None);
            Console.WriteLine($"Published to {topic} (QoS {qos})");
        }
    }
}
