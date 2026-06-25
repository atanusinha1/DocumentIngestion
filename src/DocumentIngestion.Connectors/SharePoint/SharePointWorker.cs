using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DocumentIngestion.Connectors.SharePoint;

public class SharePointWorker : BackgroundService
{
    private readonly SharePointConnector      _connector;
    private readonly IDistributedCache        _cache;
    private readonly SharePointOptions        _options;
    private readonly ILogger<SharePointWorker> _logger;
    private const string CacheKey = "sharepoint:deltalink";

    public SharePointWorker(
        SharePointConnector connector,
        IDistributedCache cache,
        IOptions<SharePointOptions> opts,
        ILogger<SharePointWorker> logger)
    {
        _connector = connector;
        _cache     = cache;
        _options   = opts.Value;
        _logger    = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var stored = await _cache.GetStringAsync(CacheKey, ct);
        if (stored is not null)
        {
            _connector.SetDeltaLink(stored);
            _logger.LogInformation("Resumed SharePoint delta sync");
        }

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var doc in _connector.PollAsync(ct))
                    await _connector.IngestAsync(doc, ct);

                if (_connector.DeltaLink is not null)
                    await _cache.SetStringAsync(CacheKey, _connector.DeltaLink, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SharePoint poll failed — retrying after interval");
            }

            await Task.Delay(TimeSpan.FromMinutes(_options.PollIntervalMinutes), ct);
        }
    }
}