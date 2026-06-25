using DocumentIngestion.Contracts.Events;
using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Contracts.Models;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using System.Text.Json;

namespace DocumentIngestion.Connectors.Base;

public abstract class ConnectorBase
{
    protected readonly IMessagePublisher Publisher;
    protected readonly BlobContainerClient BlobContainer;
    protected readonly ILogger Logger;

    protected ConnectorBase(
        IMessagePublisher publisher,
        BlobContainerClient blobContainer,
        ILogger logger)
    {
        Publisher      = publisher;
        BlobContainer  = blobContainer;
        Logger         = logger;
    }

    /// <summary>Pull documents from source and stage them for processing.</summary>
    public abstract IAsyncEnumerable<RawDocument> PollAsync(CancellationToken ct);

    public async Task IngestAsync(RawDocument doc, CancellationToken ct)
    {
        try
        {
            // 1. Upload raw doc to blob staging area
            var blobName = $"{doc.SourceType}/{doc.Id}.json";
            var json     = JsonSerializer.Serialize(doc);
            var client   = BlobContainer.GetBlobClient(blobName);
            await client.UploadAsync(
                BinaryData.FromString(json),
                overwrite: true,
                cancellationToken: ct);

            // 2. Publish event to message queue
            var evt = new DocumentIngestedEvent
            {
                DocumentId = doc.Id,
                SourceType = doc.SourceType,
                BlobUri    = client.Uri.ToString()
            };

            await Publisher.PublishAsync(evt, Topics.DocumentIngested, ct);
            Logger.LogInformation("Ingested document {Id} from {Source}", doc.Id, doc.SourceType);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to ingest document {Id}", doc.Id);
            throw;
        }
    }
}

public static class Topics
{
    public const string DocumentIngested = "document-ingested";
    public const string ChunksReady      = "chunks-ready";
    public const string EmbeddingsReady  = "embeddings-ready";
}