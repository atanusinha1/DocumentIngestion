using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Messaging.RabbitMQ;
using DocumentIngestion.VectorDbWriter.Writers;
using Qdrant.Client;
using Microsoft.Extensions.Options;
using DocumentIngestion.Messaging.ServiceBus;

var builder = Host.CreateApplicationBuilder(args);

// ── Message Queue
if (builder.Environment.IsDevelopment())
{
    var rabbitOptions = builder.Configuration.GetSection("RabbitMQ").Get<RabbitMqOptions>()!;
    builder.Services.AddSingleton(rabbitOptions);
    builder.Services.AddSingleton(new RabbitMqConnectionFactory(rabbitOptions));
    builder.Services.AddSingleton<IMessagePublisher, RabbitMqPublisher>();
    builder.Services.AddSingleton<IMessageConsumer, RabbitMqConsumer>();
}
else
{
    var sbOptions = builder.Configuration.GetSection("ServiceBus").Get<ServiceBusOptions>()!;
    builder.Services.AddSingleton(sbOptions);
    builder.Services.AddSingleton(
        new Azure.Messaging.ServiceBus.ServiceBusClient(sbOptions.ConnectionString));
    builder.Services.AddSingleton<IMessagePublisher,
        DocumentIngestion.Messaging.ServiceBus.ServiceBusPublisher>();
    builder.Services.AddSingleton<IMessageConsumer,
        DocumentIngestion.Messaging.ServiceBus.ServiceBusConsumer>();
}

// ── Vector Database
builder.Services.Configure<QdrantOptions>(
    builder.Configuration.GetSection("Qdrant"));

builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<QdrantOptions>>().Value;
    return new QdrantClient(opts.Host, opts.Port, https:false);
});

builder.Services.AddSingleton<QdrantWriter>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<QdrantWriter>());

var host = builder.Build();

// Create Qdrant collection before starting the worker
var writer = host.Services.GetRequiredService<QdrantWriter>();
await writer.EnsureCollectionAsync();

await host.RunAsync();