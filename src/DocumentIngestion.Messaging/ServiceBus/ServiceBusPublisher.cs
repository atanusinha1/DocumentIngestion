using Azure.Messaging.ServiceBus;
using DocumentIngestion.Contracts.Interfaces;
using System.Text.Json;

namespace DocumentIngestion.Messaging.ServiceBus;

public class ServiceBusOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    // Map topic names → Service Bus queue/topic names
    public Dictionary<string, string> TopicMappings { get; set; } = new();
}

public class ServiceBusPublisher : IMessagePublisher, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly ServiceBusOptions _options;
    private readonly Dictionary<string, ServiceBusSender> _senders = new();

    public ServiceBusPublisher(ServiceBusClient client, ServiceBusOptions options)
    {
        _client  = client;
        _options = options;
    }

    public async Task PublishAsync<T>(T message, string topic, CancellationToken ct = default)
        where T : class
    {
        var entityName = _options.TopicMappings.GetValueOrDefault(topic, topic);

        if (!_senders.TryGetValue(entityName, out var sender))
        {
            sender = _client.CreateSender(entityName);
            _senders[entityName] = sender;
        }

        var json    = JsonSerializer.Serialize(message);
        var sbMsg   = new ServiceBusMessage(json)
        {
            ContentType     = "application/json",
            Subject         = typeof(T).Name,
            MessageId       = Guid.NewGuid().ToString(),
            SessionId       = (message as dynamic)?.DocumentId?.ToString()  // optional session
        };

        await sender.SendMessageAsync(sbMsg, ct);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var sender in _senders.Values)
            await sender.DisposeAsync();
        await _client.DisposeAsync();
    }
}