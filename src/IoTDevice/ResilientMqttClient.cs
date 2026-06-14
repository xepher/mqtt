using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Diagnostics.PacketInspection;
using Polly;
using Polly.Retry;

namespace IoTDevice
{
    // Use Decorator Pattern to implement a resilient MQTT client that wraps the standard MQTTnet client and adds Polly retry logic for connection attempts
    public class ResilientMqttClient : IMqttClient
    {
        private readonly IMqttClient _innerClient;
        private readonly ResiliencePipeline _retryPipeline;

        public ResilientMqttClient(IMqttClient innerClient, IOptions<RetryStrategyOptions> options)
        {
            _innerClient = innerClient ?? throw new ArgumentNullException(nameof(innerClient));
            var retryOptions = (options ?? throw new ArgumentNullException(nameof(options))).Value;

            // Apply predicate and callback which cannot be read from static configuration file
            retryOptions.ShouldHandle = new PredicateBuilder().Handle<Exception>();
            retryOptions.OnRetry = args =>
            {
                Console.WriteLine($"[Polly] MQTT connection attempt failed (Attempt {args.AttemptNumber + 1}). " +
                                  $"Retrying in {args.RetryDelay.TotalSeconds:F2} seconds... " +
                                  $"Reason: {args.Outcome.Exception?.Message}");
                return default;
            };

            // Configure Polly retry pipeline with the strongly-typed options
            _retryPipeline = new ResiliencePipelineBuilder()
                .AddRetry(retryOptions)
                .Build();
        }

        public bool IsConnected => _innerClient.IsConnected;
        public MqttClientOptions Options => _innerClient.Options;

        public event Func<MqttClientConnectingEventArgs, Task> ConnectingAsync
        {
            add => _innerClient.ConnectingAsync += value;
            remove => _innerClient.ConnectingAsync -= value;
        }

        public event Func<MqttClientConnectedEventArgs, Task> ConnectedAsync
        {
            add => _innerClient.ConnectedAsync += value;
            remove => _innerClient.ConnectedAsync -= value;
        }

        public event Func<MqttClientDisconnectedEventArgs, Task> DisconnectedAsync
        {
            add => _innerClient.DisconnectedAsync += value;
            remove => _innerClient.DisconnectedAsync -= value;
        }

        public event Func<MqttApplicationMessageReceivedEventArgs, Task> ApplicationMessageReceivedAsync
        {
            add => _innerClient.ApplicationMessageReceivedAsync += value;
            remove => _innerClient.ApplicationMessageReceivedAsync -= value;
        }

        public event Func<InspectMqttPacketEventArgs, Task> InspectPacketAsync
        {
            add => _innerClient.InspectPacketAsync += value;
            remove => _innerClient.InspectPacketAsync -= value;
        }

        public Task<MqttClientConnectResult> ConnectAsync(MqttClientOptions options, CancellationToken cancellationToken = default)
        {
            return _retryPipeline.ExecuteAsync(
                async token => await _innerClient.ConnectAsync(options, token),
                cancellationToken).AsTask();
        }

        public Task SendEnhancedAuthenticationExchangeDataAsync(MqttEnhancedAuthenticationExchangeData data, CancellationToken cancellationToken = default)
        {
            return _innerClient.SendEnhancedAuthenticationExchangeDataAsync(data, cancellationToken);
        }

        public Task DisconnectAsync(MqttClientDisconnectOptions options, CancellationToken cancellationToken = default)
        {
            return _innerClient.DisconnectAsync(options, cancellationToken);
        }

        public Task PingAsync(CancellationToken cancellationToken = default)
        {
            return _innerClient.PingAsync(cancellationToken);
        }

        public Task<MqttClientPublishResult> PublishAsync(MqttApplicationMessage message, CancellationToken cancellationToken = default)
        {
            return _innerClient.PublishAsync(message, cancellationToken);
        }

        public Task<MqttClientSubscribeResult> SubscribeAsync(MqttClientSubscribeOptions options, CancellationToken cancellationToken = default)
        {
            return _innerClient.SubscribeAsync(options, cancellationToken);
        }

        public Task<MqttClientUnsubscribeResult> UnsubscribeAsync(MqttClientUnsubscribeOptions options, CancellationToken cancellationToken = default)
        {
            return _innerClient.UnsubscribeAsync(options, cancellationToken);
        }

        public void Dispose()
        {
            _innerClient.Dispose();
        }
    }
}
