namespace DocumentIngestion.Contracts.Models;

public class ChunkEmbedding           // ← class, not record
{
    public Guid     ChunkId  { get; set; }                          // ← set not init
    public float[]  Vector   { get; set; } = Array.Empty<float>(); // ← set not init
    public string   Text     { get; set; } = string.Empty;
    public Dictionary<string, string> Metadata { get; set; } = new();
}