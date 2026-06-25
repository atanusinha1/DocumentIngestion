using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using Azure.Search.Documents.Models;
using DocumentIngestion.Contracts.Events;
using DocumentIngestion.Contracts.Interfaces;

namespace DocumentIngestion.VectorDbWriter.Writers;

public class AzureAiSearchWriter : BackgroundService
{
    private readonly IMessageConsumer     _consumer;
    private readonly SearchClient         _searchClient;
    private readonly ILogger<AzureAiSearchWriter> _logger;

    private const string IndexName = "document-chunks";

    public AzureAiSearchWriter(
        IMessageConsumer consumer,
        SearchIndexClient indexClient,
        SearchClient searchClient,
        ILogger<AzureAiSearchWriter> logger)
    {
        _consumer     = consumer;
        _searchClient = searchClient;
        _logger       = logger;
    }

    protected override Task ExecuteAsync(CancellationToken ct)
        => _consumer.SubscribeAsync<EmbeddingsReadyEvent>(
            Topics.EmbeddingsReady, HandleAsync, ct);

    private async Task HandleAsync(EmbeddingsReadyEvent evt, CancellationToken ct)
    {
        var documents = evt.Embeddings.Select(e => new SearchDocument
        {
            ["id"]         = e.ChunkId.ToString(),
            ["documentId"] = evt.DocumentId.ToString(),
            ["text"]       = e.Text,
            ["vector"]     = e.Vector,            // must match index vector field
            ["title"]      = e.Metadata.GetValueOrDefault("title", ""),
            ["url"]        = e.Metadata.GetValueOrDefault("url", ""),
            ["source"]     = e.Metadata.GetValueOrDefault("source", ""),
        }).ToList();

        await _searchClient.UploadDocumentsAsync(documents, cancellationToken: ct);

        _logger.LogInformation(
            "Upserted {Count} chunks for document {Id}",
            documents.Count, evt.DocumentId);
    }

    /// <summary>Run once at startup to create/update the search index.</summary>
    public static async Task EnsureIndexAsync(
        SearchIndexClient indexClient, int vectorDimensions = 3072)
    {
        var fields = new List<SearchField>
        {
            new SimpleField("id",         SearchFieldDataType.String) { IsKey = true },
            new SimpleField("documentId", SearchFieldDataType.String) { IsFilterable = true },
            new SearchableField("text"),
            new SearchableField("title"),
            new SimpleField("url",        SearchFieldDataType.String),
            new SimpleField("source",     SearchFieldDataType.String) { IsFilterable = true },
            new VectorSearchField("vector", vectorDimensions, "hnsw-config"),
        };

        var index = new SearchIndex(IndexName)
        {
            Fields = fields,
            VectorSearch = new VectorSearch
            {
                Algorithms = { new HnswAlgorithmConfiguration("hnsw-config") },
                Profiles    = { new VectorSearchProfile("default", "hnsw-config") }
            }
        };

        await indexClient.CreateOrUpdateIndexAsync(index);
    }
}

public static class Topics
{
    public const string DocumentIngested = "document-ingested";
    public const string ChunksReady      = "chunks-ready";
    public const string EmbeddingsReady  = "embeddings-ready";
}