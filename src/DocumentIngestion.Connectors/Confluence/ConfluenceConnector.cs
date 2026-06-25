using RestSharp;
using RestSharp.Authenticators;
using HtmlAgilityPack;
using System.Text.Json.Serialization;
using DocumentIngestion.Contracts.Models;
using DocumentIngestion.Connectors.Base;
using Microsoft.Extensions.Options;
using DocumentIngestion.Contracts.Interfaces;
using Azure.Storage.Blobs;

namespace DocumentIngestion.Connectors.Confluence;

public class ConfluenceOptions
{
    public string BaseUrl    { get; set; } = string.Empty; // https://your-domain.atlassian.net/wiki
    public string Username   { get; set; } = string.Empty;
    public string ApiToken   { get; set; } = string.Empty;
    public string SpaceKey   { get; set; } = string.Empty; // e.g. "TEAM"
    public int    PollIntervalMinutes { get; set; } = 10;
}

public class ConfluenceConnector : ConnectorBase
{
    private readonly RestClient       _client;
    private readonly ConfluenceOptions _options;
    private DateTimeOffset _lastPolled = DateTimeOffset.UtcNow.AddMinutes(-60);

    public ConfluenceConnector(
        IMessagePublisher publisher,
        BlobContainerClient blob,
        IOptions<ConfluenceOptions> options,
        ILogger<ConfluenceConnector> logger)
        : base(publisher, blob, logger)
    {
        _options = options.Value;
        _client  = new RestClient(
            new RestClientOptions(_options.BaseUrl)
            {
                Authenticator = new HttpBasicAuthenticator(
                    _options.Username, _options.ApiToken)
            });
    }

    public override async IAsyncEnumerable<RawDocument> PollAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var since      = _lastPolled.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        _lastPolled    = DateTimeOffset.UtcNow;
        var start      = 0;
        const int limit = 50;

        while (true)
        {
            var request = new RestRequest("rest/api/content")
                .AddParameter("spaceKey", _options.SpaceKey)
                .AddParameter("type",     "page")
                .AddParameter("expand",   "body.storage,version,history.lastUpdated")
                .AddParameter("limit",    limit)
                .AddParameter("start",    start);

            var response = await _client
                .GetAsync<ConfluencePageList>(request, ct);

            if (response?.Results is null || response.Results.Count == 0)
                break;

            foreach (var page in response.Results)
            {
                if (ct.IsCancellationRequested) yield break;

                var modified = page.History?.LastUpdated?.When ?? DateTimeOffset.MinValue;
                if (modified < _lastPolled.AddMinutes(-_options.PollIntervalMinutes * 2))
                    continue;

                var plainText = StripHtml(page.Body?.Storage?.Value ?? string.Empty);
                if (string.IsNullOrWhiteSpace(plainText)) continue;

                yield return new RawDocument
                {
                    SourceType  = "Confluence",
                    SourceId    = page.Id,
                    Title       = page.Title,
                    Content     = plainText,
                    Url         = $"{_options.BaseUrl}/display/{_options.SpaceKey}/{page.Title}",
                    Author      = page.History?.LastUpdated?.By?.DisplayName ?? string.Empty,
                    ModifiedAt  = modified,
                    CreatedAt   = modified,
                    Metadata    = new() { ["spaceKey"] = _options.SpaceKey }
                };
            }

            if (response.Results.Count < limit) break;
            start += limit;
        }
    }

    private static string StripHtml(string html)
    {
        var doc  = new HtmlDocument();
        doc.LoadHtml(html);
        return doc.DocumentNode.InnerText
            .Replace("&nbsp;", " ")
            .Trim();
    }
}

// --- DTOs ---
record ConfluencePageList(
    [property: JsonPropertyName("results")] List<ConfluencePage> Results);

record ConfluencePage(
    [property: JsonPropertyName("id")]      string Id,
    [property: JsonPropertyName("title")]   string Title,
    [property: JsonPropertyName("body")]    ConfluenceBody? Body,
    [property: JsonPropertyName("history")] ConfluenceHistory? History);

record ConfluenceBody(
    [property: JsonPropertyName("storage")] ConfluenceStorage? Storage);

record ConfluenceStorage(
    [property: JsonPropertyName("value")] string? Value);

record ConfluenceHistory(
    [property: JsonPropertyName("lastUpdated")] ConfluenceVersion? LastUpdated);

record ConfluenceVersion(
    [property: JsonPropertyName("when")] DateTimeOffset When,
    [property: JsonPropertyName("by")]   ConfluenceUser? By);

record ConfluenceUser(
    [property: JsonPropertyName("displayName")] string DisplayName);