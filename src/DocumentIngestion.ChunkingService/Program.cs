using Azure.Storage.Blobs;
using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Messaging.RabbitMQ;
using DocumentIngestion.ChunkingService.Workers;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using DocumentIngestion.Messaging.ServiceBus;

var builder = Host.CreateApplicationBuilder(args);

// ── Blob Storage (Azurite locally, Azure Blob in prod — same SDK, different connection string)
var blobConn = builder.Configuration["BlobStorage:ConnectionString"]!;
var blobCont = builder.Configuration["BlobStorage:ContainerName"] ?? "documents-dev";
builder.Services.AddSingleton(new BlobContainerClient(blobConn, blobCont));

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

builder.Services.AddHostedService<ChunkingWorker>();

var host = builder.Build();
await host.RunAsync();