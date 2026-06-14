using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace CentralController
{
    public class EmqxBridgeConfigurator : BackgroundService
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<EmqxBridgeConfigurator> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _baseUrl;

        public EmqxBridgeConfigurator(ILogger<EmqxBridgeConfigurator> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _httpClient = new HttpClient();
            _baseUrl = _configuration["EMQX_API_URL"] ?? "http://nginx:18083";

            var apiKey = _configuration["EMQX_API_KEY"] ?? "adminkey";
            var apiSecret = _configuration["EMQX_API_SECRET"] ?? "adminsecret";

            var authString = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{apiKey}:{apiSecret}"));
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authString);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("EMQX Bridge Configurator started. Waiting for EMQX to be available...");

            bool emqxAvailable = false;
            while (!emqxAvailable && !stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var response = await _httpClient.GetAsync($"{_baseUrl}/api/v5/status", stoppingToken);
                    if (response.IsSuccessStatusCode)
                    {
                        var body = await response.Content.ReadAsStringAsync(stoppingToken);
                        _logger.LogInformation($"EMQX API responded with status: {body}");
                        emqxAvailable = true;
                    }
                    else
                    {
                        _logger.LogWarning($"EMQX API returned status code: {response.StatusCode}. Retrying...");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Cannot reach EMQX API at {_baseUrl}: {ex.Message}. Retrying in 5 seconds...");
                }

                if (!emqxAvailable)
                {
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

            if (stoppingToken.IsCancellationRequested) return;

            _logger.LogInformation("EMQX is online. Checking configuration strategy...");

            try
            {
                // Try modern API (v5.5+)
                var checkConnectors = await _httpClient.GetAsync($"{_baseUrl}/api/v5/connectors", stoppingToken);
                if (checkConnectors.IsSuccessStatusCode)
                {
                    _logger.LogInformation("EMQX supports modern API (connectors). Configuring using modern schema...");
                    try
                    {
                        await ConfigureModernBridge(stoppingToken);
                        _logger.LogInformation("Modern bridge configured successfully.");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"Modern bridge configuration failed: {ex.Message}. Falling back to legacy bridges API...");
                        await ConfigureLegacyBridge(stoppingToken);
                    }
                }
                else
                {
                    _logger.LogInformation("EMQX does not support modern API or returned error. Falling back to legacy bridges API...");
                    await ConfigureLegacyBridge(stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error configuring EMQX bridge: {ex.Message}");
            }
        }

        private async Task ConfigureModernBridge(CancellationToken ct)
        {
            // 1. Create Connector
            var connectorPayload = new
            {
                type = "mqtt",
                name = "rabbitmq_connector",
                enable = true,
                server = "rabbitmq:1883",
                proto_ver = "v4",
                username = "guest",
                password = "guest",
                clean_start = true,
                keepalive = "60s"
            };

            var connectorJson = JsonSerializer.Serialize(connectorPayload);
            var content = new StringContent(connectorJson, Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync($"{_baseUrl}/api/v5/connectors", content, ct);
            var respBody = await response.Content.ReadAsStringAsync(ct);
            _logger.LogInformation($"Create Connector response: {response.StatusCode} - {respBody}");

            if (!response.IsSuccessStatusCode && response.StatusCode != System.Net.HttpStatusCode.BadRequest)
            {
                throw new Exception($"Connector creation failed with status code {response.StatusCode}: {respBody}");
            }

            // 2. Create Action (Sink)
            var actionPayload = new
            {
                type = "mqtt",
                name = "rabbitmq_action",
                connector = "rabbitmq_connector",
                enable = true,
                parameters = new
                {
                    topic = "${topic}",
                    qos = 1,
                    retain = false
                }
            };

            var actionJson = JsonSerializer.Serialize(actionPayload);
            content = new StringContent(actionJson, Encoding.UTF8, "application/json");

            response = await _httpClient.PostAsync($"{_baseUrl}/api/v5/actions", content, ct);
            respBody = await response.Content.ReadAsStringAsync(ct);
            _logger.LogInformation($"Create Action response: {response.StatusCode} - {respBody}");

            if (!response.IsSuccessStatusCode)
            {
                // Clean up the connector we just created
                try
                {
                    _logger.LogInformation("Deleting connector due to action failure...");
                    await _httpClient.DeleteAsync($"{_baseUrl}/api/v5/connectors/mqtt:rabbitmq_connector", ct);
                }
                catch { }
                throw new Exception($"Action creation failed with status code {response.StatusCode}: {respBody}");
            }

            // 3. Create Rule
            var rulePayload = new
            {
                sql = "SELECT * FROM \"mqttsystem/org1/#\"",
                actions = new[] { "mqtt:rabbitmq_action" },
                enable = true,
                description = "Forward all mqttsystem messages to RabbitMQ"
            };

            var ruleJson = JsonSerializer.Serialize(rulePayload);
            content = new StringContent(ruleJson, Encoding.UTF8, "application/json");

            response = await _httpClient.PostAsync($"{_baseUrl}/api/v5/rules", content, ct);
            respBody = await response.Content.ReadAsStringAsync(ct);
            _logger.LogInformation($"Create Rule response: {response.StatusCode} - {respBody}");

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Rule creation failed with status code {response.StatusCode}: {respBody}");
            }
        }

        private async Task ConfigureLegacyBridge(CancellationToken ct)
        {
            var bridgePayload = new
            {
                type = "mqtt",
                name = "rabbitmq_bridge",
                server = "rabbitmq:1883",
                proto_ver = "v4",
                username = "guest",
                password = "guest",
                clean_start = true,
                ssl = new { enable = false },
                keepalive = "60s",
                egress = new
                {
                    local = new { topic = "mqttsystem/org1/#" },
                    remote = new { topic = "${topic}", qos = 1, retain = false }
                },
                enable = true
            };

            var bridgeJson = JsonSerializer.Serialize(bridgePayload);
            var content = new StringContent(bridgeJson, Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync($"{_baseUrl}/api/v5/bridges", content, ct);
            var respBody = await response.Content.ReadAsStringAsync(ct);
            _logger.LogInformation($"Create Legacy Bridge response: {response.StatusCode} - {respBody}");
        }
    }
}
