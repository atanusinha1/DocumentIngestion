using System.Text;
using System.Text.Json;
using DocumentIngestion.Contracts.Interfaces;
using RabbitMQ.Client;
using Newtonsoft.Json;

namespace DocumentIngestion.Messaging.RabbitMQ;

public class RabbitMqPublisher : IMessagePublisher, IAsyncDisposable
{
    private IConnection?     _connection;
    private IChannel?        _channel;
    private readonly RabbitMqConnectionFactory _factory;
    private readonly RabbitMqOptions           _options;
    private readonly SemaphoreSlim             _initLock = new(1, 1);

    // All topics in the system — publisher pre-declares every queue
    // so messages are never dropped due to startup order.
    private static readonly string[] AllTopics =
    [
        "document-ingested",
        "chunks-ready",
        "embeddings-ready"
    ];

    public RabbitMqPublisher(RabbitMqConnectionFactory factory, RabbitMqOptions options)
    {
        _factory = factory;
        _options = options;
    }

    public async Task PublishAsync<T>(
        T message, string topic, CancellationToken ct = default)
        where T : class
    {
        await EnsureInitializedAsync(ct);

        var json = JsonConvert.SerializeObject(message);
        var body = Encoding.UTF8.GetBytes(json).AsMemory();

        await _channel!.BasicPublishAsync(
            exchange:        _options.ExchangeName,
            routingKey:      topic,
            mandatory:       false,
            basicProperties: new BasicProperties
            {
                ContentType  = "application/json",
                DeliveryMode = DeliveryModes.Persistent,
                Type         = typeof(T).Name,
                MessageId    = Guid.NewGuid().ToString()
            },
            body:              body,
            cancellationToken: ct);
    }

    private async Task EnsureInitializedAsync(CancellationToken ct)
    {
        if (_channel is not null) return;

        await _initLock.WaitAsync(ct);
        try
        {
            if (_channel is not null) return;

            _connection = await _factory.CreateConnectionAsync(ct);
            _channel    = await _connection.CreateChannelAsync(cancellationToken: ct);

            // ── Declare the exchange ───────────────────────────────────────
            await _channel.ExchangeDeclareAsync(
                exchange:          _options.ExchangeName,
                type:              ExchangeType.Topic,
                durable:           _options.Durable,
                autoDelete:        false,
                cancellationToken: ct);

            // ── Pre-declare every queue and bind it to the exchange ────────
            //
            // WHY: A topic exchange drops messages if no queue is bound to
            // the routing key at the moment of publish. Consumers declare
            // their own queues in SubscribeAsync, but if a consumer starts
            // AFTER a publisher sends a message, that message is lost.
            //
            // By declaring all queues here, in the publisher, they exist
            // from the moment the first service starts — even if the
            // consuming service hasn't started yet. RabbitMQ will hold the
            // messages (durable = survives broker restart) until a consumer
            // connects and processes them.
            //
            // QueueDeclareAsync is idempotent — calling it multiple times
            // with the same parameters is safe and has no side effects.
            foreach (var topic in AllTopics)
            {
                await _channel.QueueDeclareAsync(
                    queue:             topic,
                    durable:           true,   // survives RabbitMQ restart
                    exclusive:         false,  // shared across connections
                    autoDelete:        false,  // stays even when consumers disconnect
                    cancellationToken: ct);

                await _channel.QueueBindAsync(
                    queue:             topic,
                    exchange:          _options.ExchangeName,
                    routingKey:        topic,
                    cancellationToken: ct);
            }
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel    is not null) await _channel.CloseAsync();
        if (_connection is not null) await _connection.CloseAsync();
    }
}