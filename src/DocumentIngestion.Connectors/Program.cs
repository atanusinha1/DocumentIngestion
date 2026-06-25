using Azure.Storage.Blobs;
using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Messaging.RabbitMQ;
using DocumentIngestion.Connectors.Pdf;

var builder = Host.CreateApplicationBuilder(args);

// ── Blob Storage (Azurite locally)
var blobConn = builder.Configuration["BlobStorage:ConnectionString"]!;
var blobCont = builder.Configuration["BlobStorage:ContainerName"] ?? "documents-dev";

// Ensure the container exists in Azurite before starting
var blobServiceClient = new BlobServiceClient(blobConn);
await blobServiceClient
    .GetBlobContainerClient(blobCont)
    .CreateIfNotExistsAsync();

builder.Services.AddSingleton(
    new BlobContainerClient(blobConn, blobCont));

// ── Message Queue (RabbitMQ)
var rabbitOptions = builder.Configuration
    .GetSection("RabbitMQ").Get<RabbitMqOptions>()!;
builder.Services.AddSingleton(rabbitOptions);
builder.Services.AddSingleton(new RabbitMqConnectionFactory(rabbitOptions));
builder.Services.AddSingleton<IMessagePublisher, RabbitMqPublisher>();

// ── PDF Connector
builder.Services.Configure<PdfConnectorOptions>(
    builder.Configuration.GetSection("PdfConnector"));
builder.Services.AddSingleton<PdfConnector>();
builder.Services.AddHostedService<PdfWorker>();

var host = builder.Build();
await host.RunAsync();