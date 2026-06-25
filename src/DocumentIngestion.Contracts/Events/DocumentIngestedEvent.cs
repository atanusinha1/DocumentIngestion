using DocumentIngestion.Contracts.Models;

namespace DocumentIngestion.Contracts.Events;

public record DocumentIngestedEvent
{
    public Guid        DocumentId  { get; init; }
    public string      SourceType  { get; init; } = string.Empty;
    public string      BlobUri     { get; init; } = string.Empty; // where raw doc is stored
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
}