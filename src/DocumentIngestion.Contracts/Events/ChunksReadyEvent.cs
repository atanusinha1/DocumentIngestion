using DocumentIngestion.Contracts.Models;

public class ChunksReadyEvent         // ← class, not record
{
    public Guid        DocumentId { get; set; }
    public List<Chunk> Chunks     { get; set; } = new();
    public DateTimeOffset OccurredAt { get; set; } = DateTimeOffset.UtcNow;
}