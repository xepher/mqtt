using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Buffers;
using MQTTnet;
using MQTTnet.Packets;

namespace IoTDevice
{
    public class Program
    {
        private static string _deviceId;
        private static string _mqttHost;
        private static string _otaUrl;
        private static IMqttClient _mqttClient;
        private static readonly HttpClient _httpClient = new HttpClient();
        private static readonly Random _random = new Random();

        public static async Task Main(string[] args)
        {
            _deviceId = Environment.GetEnvironmentVariable("DEVICE_ID") ?? $"device-{Guid.NewGuid().ToString().Substring(0, 6)}";
            _mqttHost = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "localhost";
            _otaUrl = Environment.GetEnvironmentVariable("OTA_URL") ?? "http://localhost/ota/firmware-v1.2.0.bin";

            Console.WriteLine($"Starting IoT Device simulator: {_deviceId}");
            Console.WriteLine($"MQTT Host: {_mqttHost}");
            Console.WriteLine($"OTA URL: {_otaUrl}");

            var factory = new MqttClientFactory();
            _mqttClient = factory.CreateMqttClient();

            var statusTopic = $"mqttsystem/org1/{_deviceId}/status/up";

            // Configure MQTT connection options with LWT
            var options = new MqttClientOptionsBuilder()
                .WithClientId(_deviceId)
                .WithTcpServer(_mqttHost, 1883)
                .WithWillPayload("offline")
                .WithWillTopic(statusTopic)
                .WithWillQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithWillRetain(true)
                .WithCleanSession(true)
                .Build();

            // Setup message received handler
            _mqttClient.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;

            // Setup reconnect loop on disconnect
            _mqttClient.DisconnectedAsync += async e =>
            {
                Console.WriteLine($"Disconnected from MQTT broker. Reason: {e.ReasonString}. Reconnecting in 5 seconds...");
                await Task.Delay(5000);
                try
                {
                    await ConnectAndSubscribeAsync(options, statusTopic);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Reconnection failed: {ex.Message}");
                }
            };

            // Connect for the first time
            try
            {
                await ConnectAndSubscribeAsync(options, statusTopic);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Initial connection failed: {ex.Message}. Will retry in the background.");
            }

            // Periodic reporting loop
            var cts = new CancellationTokenSource();
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

        private static async Task ConnectAndSubscribeAsync(MqttClientOptions options, string statusTopic)
        {
            if (!_mqttClient.IsConnected)
            {
                await _mqttClient.ConnectAsync(options, CancellationToken.None);
                Console.WriteLine("Connected to MQTT broker successfully!");

                // Publish Birth message
                var birthMessage = new MqttApplicationMessageBuilder()
                    .WithTopic(statusTopic)
                    .WithPayload("online")
                    .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithRetainFlag(true)
                    .Build();

                await _mqttClient.PublishAsync(birthMessage, CancellationToken.None);
                Console.WriteLine($"Published birth message to {statusTopic}");

                // Subscribe to control, config, and ota down topics
                var subOptions = new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter($"mqttsystem/org1/{_deviceId}/control/down")
                    .WithTopicFilter($"mqttsystem/org1/{_deviceId}/config/down")
                    .WithTopicFilter($"mqttsystem/org1/{_deviceId}/ota/down")
                    .Build();

                await _mqttClient.SubscribeAsync(subOptions, CancellationToken.None);
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
                if (topic.EndsWith("/control/down"))
                {
                    // Command control
                    Console.WriteLine($"Executing control command: {payload}");
                    var ackTopic = $"mqttsystem/org1/{_deviceId}/control/up";
                    var ackPayload = JsonSerializer.Serialize(new
                    {
                        device_id = _deviceId,
                        status = "completed",
                        command_received = payload,
                        timestamp = DateTime.UtcNow.ToString("o")
                    });
                    
                    await PublishMessageAsync(ackTopic, ackPayload);
                }
                else if (topic.EndsWith("/config/down"))
                {
                    // Config change
                    Console.WriteLine($"Applying new configuration: {payload}");
                    var ackTopic = $"mqttsystem/org1/{_deviceId}/config/up";
                    var ackPayload = JsonSerializer.Serialize(new
                    {
                        device_id = _deviceId,
                        config_applied = true,
                        timestamp = DateTime.UtcNow.ToString("o")
                    });
                    
                    await PublishMessageAsync(ackTopic, ackPayload);
                }
                else if (topic.EndsWith("/ota/down"))
                {
                    // OTA trigger
                    Console.WriteLine("OTA Upgrade command received. Starting download...");
                    var otaUpTopic = $"mqttsystem/org1/{_deviceId}/ota/up";
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
                if (_mqttClient.IsConnected)
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
                        await PublishMessageAsync($"mqttsystem/org1/{_deviceId}/telemetry/up", telemetryPayload, qos: 0);

                        // 2. Logs (QoS 0)
                        var logPayload = JsonSerializer.Serialize(new
                        {
                            device_id = _deviceId,
                            level = _random.Next(10) > 8 ? "warn" : "info",
                            message = $"Telemetry report generated successfully. Temp={temp:F2}, Hum={hum:F2}",
                            timestamp = DateTime.UtcNow.ToString("o")
                        });
                        await PublishMessageAsync($"mqttsystem/org1/{_deviceId}/logs/up", logPayload, qos: 0);

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
                        await PublishMessageAsync($"mqttsystem/org1/{_deviceId}/metrics/up", metricsPayload, qos: 0);

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
                            await PublishMessageAsync($"mqttsystem/org1/{_deviceId}/peripheral/up", peripheralPayload, qos: 1);
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
                            await PublishMessageAsync($"mqttsystem/org1/{_deviceId}/audit/up", auditPayload, qos: 1);
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
