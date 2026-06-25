using Azure.Storage.Blobs;
using DocumentIngestion.Contracts.Events;
using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Contracts.Models;
using DocumentIngestion.ChunkingService.Chunking;

namespace DocumentIngestion.ChunkingService.Workers;

public class ChunkingWorker : BackgroundService
{
    private readonly IMessageConsumer        _consumer;
    private readonly IMessagePublisher       _publisher;
    private readonly BlobContainerClient     _blob;
    private readonly RecursiveTextChunker    _chunker;
    private readonly ILogger<ChunkingWorker> _logger;

    public ChunkingWorker(
        IMessageConsumer consumer,
        IMessagePublisher publisher,
        BlobContainerClient blob,
        ILogger<ChunkingWorker> logger)
    {
        _consumer  = consumer;
        _publisher = publisher;
        _blob      = blob;
        _logger    = logger;
        _chunker   = new RecursiveTextChunker(512, 64);
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await _consumer.SubscribeAsync<DocumentIngestedEvent>(
            Topics.DocumentIngested, HandleAsync, ct);

        await Task.Delay(Timeout.Infinite, ct);
    }

    private async Task HandleAsync(DocumentIngestedEvent evt, CancellationToken ct)
    {
        _logger.LogInformation("Chunking document {Id} from {BlobUri}",
            evt.DocumentId, evt.BlobUri);

        // Extract blob name from URI and use the authenticated BlobContainerClient
        // BlobUri format: http://127.0.0.1:10000/devstoreaccount1/documents-dev/PDF/guid.json
        var uri      = new Uri(evt.BlobUri);
        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        // segments: [0]=devstoreaccount1  [1]=documents-dev  [2]=PDF  [3]=guid.json
        var blobName = string.Join("/", segments.Skip(2));

        _logger.LogInformation("Downloading blob: {BlobName}", blobName);

        var download = await _blob.GetBlobClient(blobName)
                                  .DownloadContentAsync(ct);

        var rawDoc = System.Text.Json.JsonSerializer
            .Deserialize<RawDocument>(download.Value.Content.ToString())!;

        _logger.LogInformation(
            "Downloaded doc '{Title}', content length: {Len}",
            rawDoc.Title, rawDoc.Content.Length);

        var texts  = _chunker.Split(rawDoc.Content);
        var chunks = texts.Select((text, i) => new Chunk
        {
            DocumentId = rawDoc.Id,
            Index      = i,
            Text       = text,
            TokenCount = text.Length / 4,
            Metadata   = new(rawDoc.Metadata)
            {
                ["source"]  = rawDoc.SourceType,
                ["title"]   = rawDoc.Title,
                ["url"]     = rawDoc.Url,
                ["chunkOf"] = texts.Count.ToString()
            }
        }).ToList();

        _logger.LogInformation(
            "Document {Id} split into {Count} chunks", rawDoc.Id, chunks.Count);

        await _publisher.PublishAsync(
            new ChunksReadyEvent { DocumentId = rawDoc.Id, Chunks = chunks },
            Topics.ChunksReady, ct);
    }
}

public static class Topics
{
    public const string DocumentIngested = "document-ingested";
    public const string ChunksReady      = "chunks-ready";
    public const string EmbeddingsReady  = "embeddings-ready";
}