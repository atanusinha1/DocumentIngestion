namespace DocumentIngestion.ChunkingService.Chunking;

/// <summary>
/// Recursive character text splitter — mirrors LangChain's splitter.
/// Tries to split on paragraph → sentence → word boundaries.
/// </summary>
public class RecursiveTextChunker
{
    private static readonly string[] Separators = ["\n\n", "\n", ". ", " ", ""];
    private readonly int _chunkSize;
    private readonly int _chunkOverlap;

    public RecursiveTextChunker(int chunkSize = 512, int chunkOverlap = 64)
    {
        _chunkSize    = chunkSize;
        _chunkOverlap = chunkOverlap;
    }

    public List<string> Split(string text)
    {
        var chunks = new List<string>();
        SplitRecursive(text, Separators, chunks);
        return chunks;
    }

    private void SplitRecursive(string text, string[] separators, List<string> output)
    {
        if (text.Length <= _chunkSize)
        {
            if (!string.IsNullOrWhiteSpace(text)) output.Add(text.Trim());
            return;
        }

        var separator = separators.FirstOrDefault(s => text.Contains(s)) ?? "";
        var splits    = separator.Length > 0
            ? text.Split(separator)
            : new[] { text };

        var current = new System.Text.StringBuilder();

        foreach (var split in splits)
        {
            if (current.Length + split.Length + separator.Length > _chunkSize)
            {
                if (current.Length > 0)
                {
                    output.Add(current.ToString().Trim());

                    // Overlap: retain last N characters
                    if (_chunkOverlap > 0 && current.Length > _chunkOverlap)
                    {
                        var overlap = current.ToString()[^_chunkOverlap..];
                        current.Clear().Append(overlap);
                    }
                    else
                    {
                        current.Clear();
                    }
                }

                if (split.Length > _chunkSize)
                {
                    // Recurse with smaller separators
                    var remainingSeps = separators
                        .SkipWhile(s => s != separator)
                        .Skip(1)
                        .ToArray();

                    SplitRecursive(split, remainingSeps, output);
                    continue;
                }
            }

            current.Append(split).Append(separator);
        }

        if (current.Length > 0 && !string.IsNullOrWhiteSpace(current.ToString()))
            output.Add(current.ToString().Trim());
    }
}