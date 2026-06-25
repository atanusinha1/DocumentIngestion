using UglyToad.PdfPig;
using UglyToad.PdfPig.Content;
using DocumentIngestion.Contracts.Models;
using DocumentIngestion.Connectors.Base;
using Microsoft.Extensions.Options;
using DocumentIngestion.Contracts.Interfaces;
using Azure.Storage.Blobs;

namespace DocumentIngestion.Connectors.Pdf;

public class PdfConnectorOptions
{
    public string WatchDirectory    { get; set; } = "/data/pdfs/incoming";
    public string ProcessedDirectory{ get; set; } = "/data/pdfs/processed";
    public int    PollIntervalSeconds { get; set; } = 30;
}

public class PdfConnector : ConnectorBase
{
    private readonly PdfConnectorOptions _options;
    private readonly FileSystemWatcher   _watcher;
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> _queue = new();

    public PdfConnector(
        IMessagePublisher publisher,
        BlobContainerClient blob,
        IOptions<PdfConnectorOptions> options,
        ILogger<PdfConnector> logger)
        : base(publisher, blob, logger)
    {
        _options = options.Value;

        Directory.CreateDirectory(_options.WatchDirectory);
        Directory.CreateDirectory(_options.ProcessedDirectory);

        // File system watcher for hot-folder pattern
        _watcher = new FileSystemWatcher(_options.WatchDirectory, "*.pdf")
        {
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite,
            EnableRaisingEvents = true
        };
        _watcher.Created += (_, e) => _queue.Enqueue(e.FullPath);
    }

    public override async IAsyncEnumerable<RawDocument> PollAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        // Also pick up any files already in the directory on startup
        foreach (var f in Directory.GetFiles(_options.WatchDirectory, "*.pdf"))
            _queue.Enqueue(f);

        while (!ct.IsCancellationRequested)
        {
            while (_queue.TryDequeue(out var filePath))
            {
                if (ct.IsCancellationRequested) yield break;

                var doc = await ProcessPdfAsync(filePath, ct);
                if (doc is not null)
                {
                    // Move to processed
                    var dest = Path.Combine(
                        _options.ProcessedDirectory,
                        Path.GetFileName(filePath));
                    File.Move(filePath, dest, overwrite: true);

                    yield return doc;
                }
            }
            await Task.Delay(
                TimeSpan.FromSeconds(_options.PollIntervalSeconds), ct);
        }
    }

    private Task<RawDocument?> ProcessPdfAsync(string filePath, CancellationToken _)
    {
        try
        {
            using var pdf    = PdfDocument.Open(filePath);
            var text         = new System.Text.StringBuilder();
            var pageCount    = pdf.NumberOfPages;

            for (var pageNum = 1; pageNum <= pageCount; pageNum++)
            {
                var page  = pdf.GetPage(pageNum);
                var words = page.GetWords();
                text.AppendLine(string.Join(" ", words.Select(w => w.Text)));
                text.AppendLine(); // paragraph break between pages
            }

            var info      = pdf.Information;
            var fileInfo  = new FileInfo(filePath);

            return Task.FromResult<RawDocument?>(new RawDocument
            {
                SourceType  = "PDF",
                SourceId    = fileInfo.Name,
                Title       = info.Title ?? Path.GetFileNameWithoutExtension(filePath),
                Content     = text.ToString().Trim(),
                Author      = info.Author ?? string.Empty,
                CreatedAt   = fileInfo.CreationTimeUtc,
                ModifiedAt  = fileInfo.LastWriteTimeUtc,
                Metadata    = new()
                {
                    ["fileName"]  = fileInfo.Name,
                    ["pageCount"] = pageCount.ToString(),
                    ["creator"]   = info.Creator ?? string.Empty
                }
            });
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to process PDF: {File}", filePath);
            return Task.FromResult<RawDocument?>(null);
        }
    }
}