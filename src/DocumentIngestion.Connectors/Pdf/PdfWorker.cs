using Microsoft.Extensions.Options;

namespace DocumentIngestion.Connectors.Pdf;

public class PdfWorker : BackgroundService
{
    private readonly PdfConnector              _connector;
    private readonly PdfConnectorOptions       _options;
    private readonly ILogger<PdfWorker>        _logger;

    public PdfWorker(
        PdfConnector connector,
        IOptions<PdfConnectorOptions> options,
        ILogger<PdfWorker> logger)
    {
        _connector = connector;
        _options   = options.Value;
        _logger    = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _logger.LogInformation(
            "PDF connector watching: {Dir}", _options.WatchDirectory);

        await foreach (var doc in _connector.PollAsync(ct))
        {
            _logger.LogInformation(
                "Processing PDF: {Title} ({Pages} pages)",
                doc.Title,
                doc.Metadata.GetValueOrDefault("pageCount", "?"));

            await _connector.IngestAsync(doc, ct);

            _logger.LogInformation(
                "Ingested PDF: {Title} → blob staged, event published", doc.Title);
        }
    }
}