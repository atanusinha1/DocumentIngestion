using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Qdrant.Client;
using Qdrant.Client.Grpc;
using DocumentIngestion.Contracts.Events;
using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Contracts.Models;
using Microsoft.Extensions.Options;

namespace DocumentIngestion.VectorDbWriter.Writers;

public class QdrantOptions
{
    public string Host             { get; set; } = "localhost";
    public int    Port             { get; set; } = 6334;
    public int    RestPort         { get; set; } = 6333;   // ← REST port
    public string CollectionName   { get; set; } = "document_chunks";
    public uint   VectorDimensions { get; set; } = 768;
}

public class QdrantWriter : BackgroundService
{
    private readonly IMessageConsumer      _consumer;
    private readonly QdrantClient          _qdrant;    // for collection management
    private readonly HttpClient            _http;      // for upsert via REST
    private readonly QdrantOptions         _options;
    private readonly ILogger<QdrantWriter> _logger;

    public QdrantWriter(
        IMessageConsumer consumer,
        QdrantClient qdrant,
        IOptions<QdrantOptions> options,
        ILogger<QdrantWriter> logger)
    {
        _consumer = consumer;
        _qdrant   = qdrant;
        _options  = options.Value;
        _logger   = logger;

        // REST client for upsert — bypasses gRPC float serialization bug
        _http = new HttpClient
        {
            BaseAddress = new Uri($"http://{_options.Host}:{_options.RestPort}")
        };
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await _consumer.SubscribeAsync<EmbeddingsReadyEvent>(
            Topics.EmbeddingsReady, HandleAsync, ct);

        await Task.Delay(Timeout.Infinite, ct);
    }

    private async Task HandleAsync(EmbeddingsReadyEvent evt, CancellationToken ct)
    {
        _logger.LogInformation(
            "QdrantWriter: upserting {Count} points for document {Id}",
            evt.Embeddings.Count, evt.DocumentId);

        // Build REST payload — float[] serializes perfectly to JSON array
        var payload = new
        {
            points = evt.Embeddings.Select(e => new
            {
                id      = e.ChunkId.ToString(),
                vector  = e.Vector,       // float[] → JSON [0.123, -0.456, ...]
                payload = new Dictionary<string, object>
                {
                    ["documentId"] = evt.DocumentId.ToString(),
                    ["text"]       = e.Text,
                    ["title"]      = e.Metadata.GetValueOrDefault("title",  ""),
                    ["url"]        = e.Metadata.GetValueOrDefault("url",    ""),
                    ["source"]     = e.Metadata.GetValueOrDefault("source", "")
                }
            }).ToArray()
        };

        var json     = JsonConvert.SerializeObject(payload);
        var content  = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _http.PutAsync(
            $"/collections/{_options.CollectionName}/points?wait=true",
            content, ct);

        var body = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException(
                $"Qdrant REST upsert failed: {response.StatusCode} — {body}");

        _logger.LogInformation(
            "QdrantWriter: ✅ saved {Count} points for document {Id}",
            evt.Embeddings.Count, evt.DocumentId);
    }

    public async Task EnsureCollectionAsync()
    {
        var exists = await _qdrant.CollectionExistsAsync(_options.CollectionName);
        if (exists)
        {
            _logger.LogInformation(
                "Qdrant collection '{Name}' already exists", _options.CollectionName);
            return;
        }

        await _qdrant.CreateCollectionAsync(
            _options.CollectionName,
            new VectorParams
            {
                Size     = _options.VectorDimensions,
                Distance = Distance.Cosine
            });

        _logger.LogInformation(
            "Created Qdrant collection '{Name}' with {Dims} dimensions",
            _options.CollectionName, _options.VectorDimensions);
    }
}