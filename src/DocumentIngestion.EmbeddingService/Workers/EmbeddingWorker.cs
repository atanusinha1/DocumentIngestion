using System.ClientModel;               // ApiKeyCredential, ClientResultException
using Azure.AI.OpenAI;                  // AzureOpenAIClient
using OpenAI.Embeddings;                // EmbeddingClient, OpenAIEmbedding
using DocumentIngestion.Contracts.Events;
using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Contracts.Models;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace DocumentIngestion.EmbeddingService.Workers;

public class EmbeddingOptions
{
    public string AzureOpenAiEndpoint { get; set; } = string.Empty;
    public string AzureOpenAiKey      { get; set; } = string.Empty;
    public string DeploymentName      { get; set; } = "text-embedding-3-large";
    public int    BatchSize           { get; set; } = 16;
    public int    MaxRetries          { get; set; } = 3;
}

public class EmbeddingWorker : BackgroundService
{
    private readonly IMessageConsumer         _consumer;
    private readonly IMessagePublisher        _publisher;
    private readonly EmbeddingClient          _embeddingClient; // scoped to one deployment
    private readonly EmbeddingOptions         _options;
    private readonly ResiliencePipeline       _retryPipeline;
    private readonly ILogger<EmbeddingWorker> _logger;

    public EmbeddingWorker(
        IMessageConsumer consumer,
        IMessagePublisher publisher,
        IOptions<EmbeddingOptions> options,
        ILogger<EmbeddingWorker> logger)
    {
        _consumer  = consumer;
        _publisher = publisher;
        _options   = options.Value;
        _logger    = logger;

        // v2: ApiKeyCredential is from System.ClientModel, not Azure.Core
        var azureClient = new AzureOpenAIClient(
            new Uri(_options.AzureOpenAiEndpoint),
            new ApiKeyCredential(_options.AzureOpenAiKey));

        // Returns a typed EmbeddingClient bound to the deployment name
        _embeddingClient = azureClient.GetEmbeddingClient(_options.DeploymentName);

        _retryPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = _options.MaxRetries,
                Delay            = TimeSpan.FromSeconds(2),
                BackoffType      = DelayBackoffType.Exponential,
                // v2: HTTP errors surface as ClientResultException, not RequestFailedException
                ShouldHandle = new PredicateBuilder()
                    .Handle<ClientResultException>(ex =>
                        ex.Status == 429 || ex.Status >= 500)
            })
            .Build();
    }

    protected override Task ExecuteAsync(CancellationToken ct)
        => _consumer.SubscribeAsync<ChunksReadyEvent>(
            Topics.ChunksReady, HandleAsync, ct);

    private async Task HandleAsync(ChunksReadyEvent evt, CancellationToken ct)
    {
        _logger.LogInformation(
            "Embedding {Count} chunks for document {Id}",
            evt.Chunks.Count, evt.DocumentId);

        var embeddings = new List<ChunkEmbedding>();

        foreach (var batch in evt.Chunks.Chunk(_options.BatchSize))
        {
            var texts   = batch.Select(c => c.Text).ToList();
            var vectors = await EmbedBatchAsync(texts, ct);

            for (var i = 0; i < batch.Length; i++)
            {
                embeddings.Add(new ChunkEmbedding
                {
                    ChunkId  = batch[i].Id,
                    Vector   = vectors[i],
                    Text     = batch[i].Text,
                    Metadata = batch[i].Metadata
                });
            }

            await Task.Delay(100, ct);  // gentle throttle between batches
        }

        await _publisher.PublishAsync(
            new EmbeddingsReadyEvent
            {
                DocumentId = evt.DocumentId,
                Embeddings = embeddings
            },
            Topics.EmbeddingsReady, ct);

        _logger.LogInformation("Embeddings ready for document {Id}", evt.DocumentId);
    }

    private async Task<List<float[]>> EmbedBatchAsync(
        List<string> texts, CancellationToken ct)
    {
        return await _retryPipeline.ExecuteAsync(async tok =>
        {
            // GenerateEmbeddingsAsync → ClientResult<EmbeddingCollection>
            // EmbeddingCollection : IReadOnlyList<OpenAIEmbedding>
            var result = await _embeddingClient
                .GenerateEmbeddingsAsync(texts, cancellationToken: tok);

            // ToFloats() returns ReadOnlyMemory<float> — .ToArray() gives plain float[]
            return result.Value
                .Select(e => e.ToFloats().ToArray())
                .ToList();
        }, ct);
    }
}

public static class Topics
{
    public const string DocumentIngested = "document-ingested";
    public const string ChunksReady      = "chunks-ready";
    public const string EmbeddingsReady  = "embeddings-ready";
}