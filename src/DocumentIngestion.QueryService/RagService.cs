using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DocumentIngestion.QueryService;

public class QueryRequest
{
    public string Question { get; set; } = string.Empty;
}

public class SourceChunk
{
    public string Text   { get; set; } = string.Empty;
    public string Title  { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public string Url    { get; set; } = string.Empty;
    public double Score  { get; set; }
}

public class QueryResponse
{
    public string            Answer   { get; set; } = string.Empty;
    public List<SourceChunk> Sources  { get; set; } = new();
    public long              EmbedMs  { get; set; }
    public long              SearchMs { get; set; }
    public long              LlmMs    { get; set; }
}

public class RagService
{
    private readonly HttpClient          _ollama;
    private readonly HttpClient          _qdrant;
    private readonly IConfiguration      _config;
    private readonly ILogger<RagService> _logger;

    public RagService(
        IHttpClientFactory factory,
        IConfiguration config,
        ILogger<RagService> logger)
    {
        _ollama = factory.CreateClient("Ollama");
        _qdrant = factory.CreateClient("Qdrant");
        _config = config;
        _logger = logger;
    }

    public async Task<QueryResponse> QueryAsync(string question, CancellationToken ct)
    {
        // ── 1. Embed ───────────────────────────────────────────────────────
        var t0     = DateTimeOffset.UtcNow;
        var vector = await EmbedAsync(question, ct);
        var embedMs = (long)(DateTimeOffset.UtcNow - t0).TotalMilliseconds;

        // ── 2. Search ──────────────────────────────────────────────────────
        t0 = DateTimeOffset.UtcNow;
        var chunks   = await SearchAsync(vector, ct);
        var searchMs = (long)(DateTimeOffset.UtcNow - t0).TotalMilliseconds;

        // ── 3. Generate ────────────────────────────────────────────────────
        t0 = DateTimeOffset.UtcNow;
        var answer = await GenerateAsync(question, chunks, ct);
        var llmMs  = (long)(DateTimeOffset.UtcNow - t0).TotalMilliseconds;

        return new QueryResponse
        {
            Answer   = answer,
            Sources  = chunks,
            EmbedMs  = embedMs,
            SearchMs = searchMs,
            LlmMs    = llmMs
        };
    }

    private async Task<float[]> EmbedAsync(string text, CancellationToken ct)
    {
        var model = _config["Ollama:EmbedModel"] ?? "nomic-embed-text";
        _logger.LogInformation("Embedding with model '{Model}' via {Url}",
            model, _ollama.BaseAddress);

        var body = JsonConvert.SerializeObject(new
        {
            model = model,
            input = new[] { text }
        });

        var resp = await _ollama.PostAsync("/api/embed",
            new StringContent(body, Encoding.UTF8, "application/json"), ct);

        if (!resp.IsSuccessStatusCode)
        {
            var err = await resp.Content.ReadAsStringAsync(ct);
            throw new InvalidOperationException(
                $"Ollama /api/embed failed {(int)resp.StatusCode}: {err}");
        }

        var json = await resp.Content.ReadAsStringAsync(ct);
        var root = JObject.Parse(json);
        return root["embeddings"]?[0]?.ToObject<float[]>()
               ?? throw new InvalidOperationException("No embeddings in Ollama response");
    }

    private async Task<List<SourceChunk>> SearchAsync(float[] vector, CancellationToken ct)
    {
        var collection = _config["Qdrant:CollectionName"] ?? "document_chunks";
        var topK       = int.Parse(_config["Qdrant:TopK"] ?? "5");

        _logger.LogInformation("Searching collection '{Col}' via {Url}",
            collection, _qdrant.BaseAddress);

        var body = JsonConvert.SerializeObject(new
        {
            vector          = vector,
            limit           = topK,
            with_payload    = true,
            score_threshold = 0.1
        });

        var resp = await _qdrant.PostAsync(
            $"/collections/{collection}/points/search",
            new StringContent(body, Encoding.UTF8, "application/json"), ct);

        if (!resp.IsSuccessStatusCode)
        {
            var err = await resp.Content.ReadAsStringAsync(ct);
            throw new InvalidOperationException(
                $"Qdrant search failed {(int)resp.StatusCode}: {err}");
        }

        var json   = await resp.Content.ReadAsStringAsync(ct);
        var root   = JObject.Parse(json);
        var result = root["result"] as JArray ?? new JArray();

        _logger.LogInformation("Found {Count} results", result.Count);

        return result.Select(r => new SourceChunk
        {
            Text   = r["payload"]?["text"]?.ToString()   ?? "",
            Title  = r["payload"]?["title"]?.ToString()  ?? "",
            Source = r["payload"]?["source"]?.ToString() ?? "",
            Url    = r["payload"]?["url"]?.ToString()    ?? "",
            Score  = r["score"]?.Value<double>()         ?? 0
        }).ToList();
    }

    private async Task<string> GenerateAsync(
    string question, List<SourceChunk> chunks, CancellationToken ct)
    {
        var model = _config["Ollama:ChatModel"] ?? "mistral:latest";
        _logger.LogInformation("Generating with model '{Model}'", model);

        var context = chunks.Count > 0
            ? string.Join("\n\n---\n\n",
                chunks.Select((c, i) => $"[Source {i + 1}: {c.Title}]\n{c.Text}"))
            : "No relevant documents found in the knowledge base.";

        var body = JsonConvert.SerializeObject(new
        {
            model  = model,
            stream = false,
            messages = new[]
            {
                new { role = "system", content =
                    "You are a helpful assistant. Answer based only on the provided context. " +
                    "Cite sources as [Source N]. If the context lacks enough info, say so." },
                new { role = "user", content =
                    $"Context:\n{context}\n\nQuestion: {question}" }
            }
        });

        var resp = await _ollama.PostAsync("/api/chat",
            new StringContent(body, Encoding.UTF8, "application/json"), ct);

        if (!resp.IsSuccessStatusCode)
        {
            var err = await resp.Content.ReadAsStringAsync(ct);
            throw new InvalidOperationException(
                $"Ollama /api/chat failed {(int)resp.StatusCode}: {err}");
        }

        var json = await resp.Content.ReadAsStringAsync(ct);
        return JObject.Parse(json)["message"]?["content"]?.ToString()
            ?? "No response generated.";
    }
}