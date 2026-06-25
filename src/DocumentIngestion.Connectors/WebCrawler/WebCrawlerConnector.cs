using HtmlAgilityPack;
using Polly;
using DocumentIngestion.Contracts.Models;
using DocumentIngestion.Connectors.Base;
using Microsoft.Extensions.Options;
using DocumentIngestion.Contracts.Interfaces;
using Azure.Storage.Blobs;

namespace DocumentIngestion.Connectors.WebCrawler;

public class SeedUrl
{
    public string Url        { get; set; } = string.Empty;
    public int    MaxDepth   { get; set; } = 2;
    public string? UrlPrefix { get; set; }      // only crawl URLs starting with this
}

public class WebCrawlerOptions
{
    public List<SeedUrl> Seeds         { get; set; } = new();
    public int RequestDelayMs          { get; set; } = 500;
    public int CrawlIntervalHours      { get; set; } = 24;
}

public class WebCrawlerConnector : ConnectorBase
{
    private readonly HttpClient          _http;
    private readonly WebCrawlerOptions   _options;

    public WebCrawlerConnector(
        IMessagePublisher publisher,
        BlobContainerClient blob,
        IHttpClientFactory httpFactory,
        IOptions<WebCrawlerOptions> options,
        ILogger<WebCrawlerConnector> logger)
        : base(publisher, blob, logger)
    {
        _options = options.Value;
        _http    = httpFactory.CreateClient("WebCrawler");
    }

    public override async IAsyncEnumerable<RawDocument> PollAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var seed in _options.Seeds)
        {
            if (ct.IsCancellationRequested) yield break;

            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var queue   = new Queue<(string Url, int Depth)>();
            queue.Enqueue((seed.Url, 0));

            while (queue.Count > 0 && !ct.IsCancellationRequested)
            {
                var (url, depth) = queue.Dequeue();
                if (!visited.Add(url)) continue;
                if (depth > seed.MaxDepth)  continue;

                var doc = await CrawlPageAsync(url, seed, ct);
                if (doc is not null)
                {
                    // Enqueue discovered links
                    if (depth < seed.MaxDepth)
                    {
                        foreach (var link in ExtractLinks(doc, url, seed.UrlPrefix))
                            if (!visited.Contains(link))
                                queue.Enqueue((link, depth + 1));
                    }
                    yield return doc;
                }

                await Task.Delay(_options.RequestDelayMs, ct);
            }
        }
    }

    private async Task<RawDocument?> CrawlPageAsync(
        string url, SeedUrl seed, CancellationToken ct)
    {
        try
        {
            var html = await _http.GetStringAsync(url, ct);
            var htmlDoc = new HtmlDocument();
            htmlDoc.LoadHtml(html);

            // Remove script/style nodes
            var nodes = htmlDoc.DocumentNode.SelectNodes("//script|//style|//nav|//footer|//header");
            if (nodes is not null)
                foreach (var node in nodes.ToList())
                    node.Remove();
            var title   = htmlDoc.DocumentNode
                .SelectSingleNode("//title")?.InnerText.Trim() ?? url;
            var content = htmlDoc.DocumentNode
                .SelectSingleNode("//main|//article|//body")?
                .InnerText.Trim() ?? string.Empty;

            content = System.Text.RegularExpressions.Regex
                .Replace(content, @"\s{2,}", " ")
                .Trim();

            if (string.IsNullOrWhiteSpace(content)) return null;

            return new RawDocument
            {
                SourceType  = "WebSite",
                SourceId    = url,
                Title       = System.Net.WebUtility.HtmlDecode(title),
                Content     = System.Net.WebUtility.HtmlDecode(content),
                Url         = url,
                ModifiedAt  = DateTimeOffset.UtcNow,
                CreatedAt   = DateTimeOffset.UtcNow,
                Metadata    = new() { ["seed"] = seed.Url }
            };
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to crawl {Url}", url);
            return null;
        }
    }

    private static IEnumerable<string> ExtractLinks(
        RawDocument _, string baseUrl, string? prefix)
    {
        // In real impl: re-parse the HTML for anchor hrefs
        // Simplified here — use HtmlAgilityPack on the fetched HTML
        yield break;
    }
}