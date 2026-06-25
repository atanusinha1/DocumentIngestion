public class Chunk                    // ← class, not record
{
    public Guid   Id         { get; set; } = Guid.NewGuid();
    public Guid   DocumentId { get; set; }
    public int    Index      { get; set; }
    public string Text       { get; set; } = string.Empty;
    public int    TokenCount { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();
}