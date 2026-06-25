using DocumentIngestion.Contracts.Models;

public class EmbeddingsReadyEvent     // ← class, not record
{
    public Guid                   DocumentId { get; set; }
    public List<ChunkEmbedding>   Embeddings { get; set; } = new();
    public DateTimeOffset         OccurredAt { get; set; } = DateTimeOffset.UtcNow;
}