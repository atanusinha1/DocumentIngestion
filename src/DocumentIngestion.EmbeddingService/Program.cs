using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Messaging.RabbitMQ;
using DocumentIngestion.EmbeddingService.Workers;
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

// ── Embedding Model
if (builder.Environment.IsDevelopment())
{
    builder.Services.Configure<OllamaOptions>(
        builder.Configuration.GetSection("Ollama"));

    builder.Services.AddHttpClient("Ollama", client =>
    {
        client.BaseAddress = new Uri(
            builder.Configuration["Ollama:Endpoint"] ?? "http://localhost:11434");
        client.Timeout = TimeSpan.FromSeconds(60);
    });

    builder.Services.AddHostedService<OllamaEmbeddingWorker>();
}
else
{
    builder.Services.Configure<EmbeddingOptions>(
        builder.Configuration.GetSection("Embedding"));
    builder.Services.AddHostedService<EmbeddingWorker>();
}


var host = builder.Build();
await host.RunAsync();