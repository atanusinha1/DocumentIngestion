using System.Text;
using System.Text.Json;
using DocumentIngestion.Contracts.Interfaces;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;

namespace DocumentIngestion.Messaging.RabbitMQ;

public class RabbitMqConsumer : IMessageConsumer, IAsyncDisposable
{
    private readonly RabbitMqConnectionFactory _factory;
    private readonly RabbitMqOptions           _options;
    private readonly ILogger<RabbitMqConsumer> _logger;      // ← add this
    private readonly List<IConnection>         _connections = new();

    public RabbitMqConsumer(RabbitMqConnectionFactory factory, RabbitMqOptions options,ILogger<RabbitMqConsumer> logger)
    {
        _factory = factory;
        _options = options;
        _logger = logger;
    }

    public async Task SubscribeAsync<T>(
        string topic,
        Func<T, CancellationToken, Task> handler,
        CancellationToken ct = default)
        where T : class
    {
        var connection = await _factory.CreateConnectionAsync(ct);
        _connections.Add(connection);

        // v7: CreateChannelAsync (CreateModel removed)
        var channel = await connection.CreateChannelAsync(cancellationToken: ct);

        await channel.ExchangeDeclareAsync(
            exchange:   _options.ExchangeName,
            type:       ExchangeType.Topic,
            durable:    _options.Durable,
            autoDelete: false,
            cancellationToken: ct);

        await channel.QueueDeclareAsync(
            queue:      topic,
            durable:    _options.Durable,
            exclusive:  false,
            autoDelete: false,
            cancellationToken: ct);

        await channel.QueueBindAsync(
            queue:      topic,
            exchange:   _options.ExchangeName,
            routingKey: topic,
            cancellationToken: ct);

        await channel.BasicQosAsync(
            prefetchSize:  0,
            prefetchCount: 10,
            global:        false,
            cancellationToken: ct);

        // v7: AsyncEventingBasicConsumer uses ReceivedAsync (not Received)
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                var body    = Encoding.UTF8.GetString(ea.Body.ToArray());
                // ── TEMPORARY DEBUG — remove after confirming fix ─────────────────
                Console.Error.WriteLine(
                    $"[DEBUG] Body length: {body.Length}, contains 'Vector': {body.Contains("\"Vector\":")}");
                
                // ─────────────────────────────────────────────────────────────────
                var message =  JsonConvert.DeserializeObject<T>(body)!;
            
                await handler(message, ct);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(
                    $"[RabbitMqConsumer] Handler FAILED for topic '{topic}': {ex}");

                await channel.BasicNackAsync(
                    ea.DeliveryTag, multiple: false,
                    requeue: !ea.Redelivered, ct);
            }
        };

        // v7: BasicConsumeAsync requires all parameters
        await channel.BasicConsumeAsync(
            queue:       topic,
            autoAck:     false,
            consumerTag: string.Empty,
            noLocal:     false,
            exclusive:   false,
            arguments:   null,
            consumer:    consumer,
            cancellationToken: ct);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in _connections)
            await c.CloseAsync();
    }
}