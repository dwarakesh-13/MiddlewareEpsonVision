using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

namespace MiddlewareEpsonVision
{
    /// <summary>
    /// MQTT handler for both publishing and subscribing (MQTTnet 4.3.x).
    /// </summary>
    public class MqttHandler
    {
        private IMqttClient _client;
        private readonly string _defaultTopic;

        /// <summary>Raised when a message is received on a subscribed topic.</summary>
        public event EventHandler<MqttMessageReceivedEventArgs> MessageReceived;

        /// <summary>Gets whether the client is connected to the broker.</summary>
        public bool IsConnected
        {
            get { return _client != null && _client.IsConnected; }
        }

        public MqttHandler(string defaultTopic = null)
        {
            _defaultTopic = defaultTopic;
        }

        /// <summary>
        /// Connects to the MQTT broker.
        /// </summary>
        /// <param name="timeoutMs">Connection timeout in milliseconds (default 10000). Prevents hanging if broker is unreachable.</param>
        public async Task<bool> ConnectAsync(
            string brokerHost,
            int port = 1883,
            string clientId = null,
            string username = null,
            string password = null,
            bool useTls = false,
            int timeoutMs = 10000)
        {
            if (_client != null && _client.IsConnected)
                return true;

            var factory = new MqttFactory();
            _client = factory.CreateMqttClient();

            var builder = new MqttClientOptionsBuilder()
                .WithTcpServer(brokerHost, port)
                .WithClientId(clientId ?? "MiddlewareEpsonVision_" + Guid.NewGuid().ToString("N").Substring(0, 8))
                .WithCleanSession();

            if (useTls)
                builder.WithTls();

            if (!string.IsNullOrEmpty(username))
                builder.WithCredentials(username, password ?? string.Empty);

            var options = builder.Build();

            _client.ApplicationMessageReceivedAsync += OnMessageReceived;

            try
            {
                using (var cts = new CancellationTokenSource(timeoutMs))
                {
                    var result = await _client.ConnectAsync(options, cts.Token);
                    return result.ResultCode == MqttClientConnectResultCode.Success;
                }
            }
            catch (OperationCanceledException)
            {
                return false; // Timeout
            }
            catch (Exception)
            {
                return false;
            }
        }

        private Task OnMessageReceived(MqttApplicationMessageReceivedEventArgs e)
        {
            var topic = e.ApplicationMessage.Topic ?? string.Empty;
            var payloadBytes = e.ApplicationMessage.PayloadSegment;
            var payload = payloadBytes.Count == 0
                ? string.Empty
                : Encoding.UTF8.GetString(payloadBytes.Array, payloadBytes.Offset, payloadBytes.Count);
            MessageReceived?.Invoke(this, new MqttMessageReceivedEventArgs(topic, payload));
            return Task.CompletedTask;
        }

        /// <summary>
        /// Disconnects from the broker.
        /// </summary>
        public async Task DisconnectAsync()
        {
            if (_client != null && _client.IsConnected)
            {
                var disconnectOptions = new MqttClientDisconnectOptionsBuilder().Build();
                await _client.DisconnectAsync(disconnectOptions, CancellationToken.None);
            }
        }

        /// <summary>
        /// Publishes a string payload to a topic.
        /// </summary>
        public async Task<bool> PublishAsync(string payload, string topic = null, int qos = 1, bool retain = false)
        {
            var t = topic ?? _defaultTopic;
            if (string.IsNullOrEmpty(t))
                throw new InvalidOperationException("Topic must be specified or set as default topic.");

            if (_client == null || !_client.IsConnected)
                return false;

            var qosLevel = GetQosLevel(qos);

            var payloadBytes = Encoding.UTF8.GetBytes(payload ?? string.Empty);
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(t)
                .WithPayload(payloadBytes)
                .WithQualityOfServiceLevel(qosLevel)
                .WithRetainFlag(retain)
                .Build();

            try
            {
                await _client.PublishAsync(message, CancellationToken.None);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Subscribes to a topic. Incoming messages will raise <see cref="MessageReceived"/>.
        /// </summary>
        public async Task<bool> SubscribeAsync(string topic, int qos = 1)
        {
            if (_client == null || !_client.IsConnected)
                return false;

            var qosLevel = GetQosLevel(qos);

            try
            {
                var factory = new MqttFactory();
                var subscribeOptions = factory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(f => f.WithTopic(topic).WithQualityOfServiceLevel(qosLevel))
                    .Build();
                await _client.SubscribeAsync(subscribeOptions, CancellationToken.None);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Unsubscribes from a topic.
        /// </summary>
        public async Task UnsubscribeAsync(string topic)
        {
            if (_client != null && _client.IsConnected)
            {
                var factory = new MqttFactory();
                var options = factory.CreateUnsubscribeOptionsBuilder()
                    .WithTopicFilter(topic)
                    .Build();
                await _client.UnsubscribeAsync(options, CancellationToken.None);
            }
        }

        /// <summary>
        /// Disconnects and disposes the client.
        /// </summary>
        public async void Dispose()
        {
            await DisconnectAsync();
            _client?.Dispose();
            _client = null;
        }

        private static MqttQualityOfServiceLevel GetQosLevel(int qos)
        {
            if (qos == 0) return MqttQualityOfServiceLevel.AtMostOnce;
            if (qos == 2) return MqttQualityOfServiceLevel.ExactlyOnce;
            return MqttQualityOfServiceLevel.AtLeastOnce;
        }
    }

    /// <summary>
    /// Event args for received MQTT messages.
    /// </summary>
    public class MqttMessageReceivedEventArgs : EventArgs
    {
        public string Topic { get; }
        public string Payload { get; }

        public MqttMessageReceivedEventArgs(string topic, string payload)
        {
            Topic = topic;
            Payload = payload ?? string.Empty;
        }
    }
}
