namespace DocumentIngestion.Contracts.Models;

public record RawDocument
{
    public Guid   Id          { get; init; } = Guid.NewGuid();
    public string SourceType  { get; init; } = string.Empty; // "SharePoint", "Jira", etc.
    public string SourceId    { get; init; } = string.Empty; // original ID in the source system
    public string Title       { get; init; } = string.Empty;
    public string Content     { get; init; } = string.Empty; // plain text extracted
    public string ContentType { get; init; } = "text/plain";
    public string Url         { get; init; } = string.Empty;
    public string Author      { get; init; } = string.Empty;
    public DateTimeOffset CreatedAt  { get; init; }
    public DateTimeOffset ModifiedAt { get; init; }
    public Dictionary<string, string> Metadata { get; init; } = new();
}