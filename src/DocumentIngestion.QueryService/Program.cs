using DocumentIngestion.QueryService;

var builder = WebApplication.CreateBuilder(args);

// ── HTTP clients ────────────────────────────────────────────────────────────
builder.Services.AddHttpClient("Ollama", client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Ollama:BaseUrl"] ?? "http://localhost:11434");
    client.Timeout = TimeSpan.FromMinutes(3);  // LLM generation can take time
});

builder.Services.AddHttpClient("Qdrant", client =>
{
    client.BaseAddress = new Uri(
        builder.Configuration["Qdrant:BaseUrl"] ?? "http://localhost:6333");
    client.Timeout = TimeSpan.FromSeconds(10);
});

// ── Services ────────────────────────────────────────────────────────────────
builder.Services.AddScoped<RagService>();

// ── CORS (for the UI calling the API) ───────────────────────────────────────
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();

app.UseCors();
app.UseStaticFiles();   // serves wwwroot/index.html

// ── API endpoints ────────────────────────────────────────────────────────────

// Health check
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// Main RAG query endpoint
app.MapPost("/api/query", async (
    QueryRequest request,
    RagService rag,
    CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.Question))
        return Results.BadRequest(new { error = "Question cannot be empty" });

    try
    {
        var response = await rag.QueryAsync(request.Question.Trim(), ct);
        return Results.Ok(response);
    }
    catch (Exception ex)
    {
        return Results.Problem(
            detail:     ex.Message,
            title:      "Query failed",
            statusCode: 500);
    }
});

// Serve chat UI at root
app.MapFallbackToFile("index.html");

app.Run();