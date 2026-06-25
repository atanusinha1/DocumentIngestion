using Azure.Messaging.ServiceBus;
using DocumentIngestion.Contracts.Interfaces;
using System.Text.Json;

namespace DocumentIngestion.Messaging.ServiceBus;

public class ServiceBusConsumer : IMessageConsumer, IAsyncDisposable
{
    private readonly ServiceBusClient  _client;
    private readonly ServiceBusOptions _options;
    private readonly List<ServiceBusProcessor> _processors = new();

    public ServiceBusConsumer(ServiceBusClient client, ServiceBusOptions options)
    {
        _client  = client;
        _options = options;
    }

    public async Task SubscribeAsync<T>(
        string topic,
        Func<T, CancellationToken, Task> handler,
        CancellationToken ct = default)
        where T : class
    {
        var entityName = _options.TopicMappings.GetValueOrDefault(topic, topic);

        var processor = _client.CreateProcessor(entityName, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 8,
            AutoCompleteMessages = false
        });

        processor.ProcessMessageAsync += async args =>
        {
            var body    = args.Message.Body.ToString();
            var message = JsonSerializer.Deserialize<T>(body)!;

            try
            {
                await handler(message, args.CancellationToken);
                await args.CompleteMessageAsync(args.Message, args.CancellationToken);
            }
            catch (Exception)
            {
                // Move to DLQ after max delivery attempts
                await args.AbandonMessageAsync(args.Message, cancellationToken: args.CancellationToken);
                throw;
            }
        };

        processor.ProcessErrorAsync += args =>
        {
            // Log error — real impl: use ILogger
            Console.Error.WriteLine(args.Exception);
            return Task.CompletedTask;
        };

        _processors.Add(processor);
        await processor.StartProcessingAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var p in _processors)
            await p.DisposeAsync();
        await _client.DisposeAsync();
    }
}