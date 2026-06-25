using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DocumentIngestion.Contracts.Events;
using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Contracts.Models;
using Microsoft.Extensions.Options;

namespace DocumentIngestion.EmbeddingService.Workers;

public class OllamaOptions
{
    public string Endpoint         { get; set; } = "http://localhost:11434";
    public string Model            { get; set; } = "nomic-embed-text";
    public int    VectorDimensions { get; set; } = 768;
    public int    BatchSize        { get; set; } = 16;
}

public class OllamaEmbeddingWorker : BackgroundService
{
    private readonly IMessageConsumer               _consumer;
    private readonly IMessagePublisher              _publisher;
    private readonly HttpClient                     _http;
    private readonly OllamaOptions                  _options;
    private readonly ILogger<OllamaEmbeddingWorker> _logger;

    public OllamaEmbeddingWorker(
        IMessageConsumer consumer,
        IMessagePublisher publisher,
        IHttpClientFactory httpFactory,
        IOptions<OllamaOptions> options,
        ILogger<OllamaEmbeddingWorker> logger)
    {
        _consumer  = consumer;
        _publisher = publisher;
        _http      = httpFactory.CreateClient("Ollama");
        _options   = options.Value;
        _logger    = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await _consumer.SubscribeAsync<ChunksReadyEvent>(
            Topics.ChunksReady, HandleAsync, ct);

        await Task.Delay(Timeout.Infinite, ct); // ← add this
    }

    private async Task HandleAsync(ChunksReadyEvent evt, CancellationToken ct)
    {
        _logger.LogInformation(
            "Ollama: embedding {Count} chunks for document {Id}",
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
        }

        await _publisher.PublishAsync(
            new EmbeddingsReadyEvent { DocumentId = evt.DocumentId, Embeddings = embeddings },
            Topics.EmbeddingsReady, ct);

        _logger.LogInformation(
            "Ollama: embeddings ready for document {Id}", evt.DocumentId);
    }

    private async Task<List<float[]>> EmbedBatchAsync(List<string> texts, CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new OllamaEmbedRequest
        {
            Model = _options.Model,
            Input = texts
        });

        var response = await _http.PostAsync(
            $"{_options.Endpoint}/api/embed",
            new StringContent(body, Encoding.UTF8, "application/json"),
            ct);

        response.EnsureSuccessStatusCode();

        var json   = await response.Content.ReadAsStringAsync(ct);
        var result = JsonSerializer.Deserialize<OllamaEmbedResponse>(json)
                     ?? throw new InvalidOperationException("Null response from Ollama");

        if (result.Embeddings is null || result.Embeddings.Count != texts.Count)
            throw new InvalidOperationException(
                $"Ollama returned {result.Embeddings?.Count ?? 0} embeddings for {texts.Count} inputs");

        return result.Embeddings;
    }
}

record OllamaEmbedRequest
{
    [JsonPropertyName("model")] public string       Model { get; init; } = string.Empty;
    [JsonPropertyName("input")] public List<string> Input { get; init; } = new();
}

record OllamaEmbedResponse
{
    [JsonPropertyName("embeddings")] public List<float[]>? Embeddings { get; init; }
}