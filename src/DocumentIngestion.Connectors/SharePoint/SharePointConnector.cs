// ── Imports ──────────────────────────────────────────────────────────────────
// Rule for Graph SDK v6 / Kiota 2.0:
//   • Never import namespaces deeper than Microsoft.Graph.Models
//   • Never import Microsoft.Kiota.* in user code (interface locations shift between versions)
//   • Use `var` for all generated return types — let the compiler infer them
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Azure.Identity;
using Azure.Storage.Blobs;
using DocumentIngestion.Contracts.Interfaces;
using DocumentIngestion.Contracts.Models;
using DocumentIngestion.Connectors.Base;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using Microsoft.Graph.Models.ODataErrors;

namespace DocumentIngestion.Connectors.SharePoint;

public class SharePointOptions
{
    public string TenantId            { get; set; } = string.Empty;
    public string ClientId            { get; set; } = string.Empty;
    public string ClientSecret        { get; set; } = string.Empty;
    public string SiteId              { get; set; } = string.Empty;
    public string DriveId             { get; set; } = string.Empty;
    public int    PollIntervalMinutes { get; set; } = 5;
}

public class SharePointConnector : ConnectorBase
{
    private readonly GraphServiceClient _graphClient;
    private readonly SharePointOptions  _options;

    /// <summary>
    /// Server-side sync cursor. Persist this across restarts (Redis / Azure Table).
    /// null = run a full initial sync on next poll.
    /// </summary>
    public string? DeltaLink { get; private set; }

    public SharePointConnector(
        IMessagePublisher publisher,
        BlobContainerClient blob,
        IOptions<SharePointOptions> options,
        ILogger<SharePointConnector> logger)
        : base(publisher, blob, logger)
    {
        _options = options.Value;

        var credential = new ClientSecretCredential(
            _options.TenantId,
            _options.ClientId,
            _options.ClientSecret);

        _graphClient = new GraphServiceClient(credential,
            ["https://graph.microsoft.com/.default"]);
    }

    public void SetDeltaLink(string link) => DeltaLink = link;

    public override async IAsyncEnumerable<RawDocument> PollAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var (items, newLink) = await FetchDeltaItemsAsync(ct);
        if (newLink is not null) DeltaLink = newLink;

        foreach (var item in items)
        {
            if (ct.IsCancellationRequested) yield break;
            if (item.File    is null) continue;    // folder
            if (item.Deleted is not null) continue; // tombstone — handle deletes separately

            var content = await DownloadContentAsync(item, ct);
            if (content is null) continue;

            yield return new RawDocument
            {
                SourceType = "SharePoint",
                SourceId   = item.Id    ?? string.Empty,
                Title      = item.Name  ?? string.Empty,
                Content    = content,
                Url        = item.WebUrl ?? string.Empty,
                Author     = item.LastModifiedBy?.User?.DisplayName ?? string.Empty,
                CreatedAt  = item.CreatedDateTime      ?? DateTimeOffset.UtcNow,
                ModifiedAt = item.LastModifiedDateTime ?? DateTimeOffset.UtcNow,
                Metadata   = new()
                {
                    ["driveId"]   = _options.DriveId,
                    ["siteId"]    = _options.SiteId,
                    ["sizeBytes"] = item.Size?.ToString() ?? "0"
                }
            };
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Manual delta pagination — zero Kiota interface imports, zero PageIterator.
    //
    // v6 path change (confirmed from generated source):
    //   ❌  .Drives[id].Root.Delta          Root builder has no Delta property
    //   ✅  .Drives[id].Items["root"].Delta  Delta is on DriveItemItemRequestBuilder;
    //                                        "root" is a Graph API magic alias
    //
    // Response chain (BaseDeltaFunctionResponse → DeltaGetResponse → DeltaResponse):
    //   page.Value          List<DriveItem>  — items in this page
    //   page.OdataNextLink  string?          — more pages in the same sync window
    //   page.OdataDeltaLink string?          — cursor for the NEXT poll (last page only)
    //
    // Both OdataNextLink and OdataDeltaLink are DIRECT typed properties on
    // BaseDeltaFunctionResponse — no AdditionalData lookup, no interface import.
    // ─────────────────────────────────────────────────────────────────────────
    private async Task<(List<DriveItem> Items, string? NewDeltaLink)> FetchDeltaItemsAsync(
        CancellationToken ct)
    {
        var allItems    = new List<DriveItem>();
        string? newLink = null;

        // `var` infers the concrete DeltaResponse / DeltaGetResponse type silently.
        var page = DeltaLink is null

            // ── Initial full sync ─────────────────────────────────────────────
            ? await _graphClient
                .Drives[_options.DriveId]
                .Items["root"]               // ← "root" alias, NOT .Root
                .Delta                       // Delta lives here in v6
                .GetAsDeltaGetResponseAsync(req =>
                {
                    req.QueryParameters.Select = new[]
                    {
                        "id", "name", "webUrl", "file", "deleted",
                        "lastModifiedDateTime", "createdDateTime",
                        "lastModifiedBy", "size"
                    };
                }, ct)

            // ── Incremental sync ──────────────────────────────────────────────
            // WithUrl(string) is generated on every Kiota builder — replaces
            // the old ctor(rawUrl, adapter) pattern for raw-URL continuation.
            : await _graphClient
                .Drives[_options.DriveId]
                .Items["root"]
                .Delta
                .WithUrl(DeltaLink)
                .GetAsDeltaGetResponseAsync(cancellationToken: ct);

        // ── Paginate ──────────────────────────────────────────────────────────
        while (page is not null)
        {
            allItems.AddRange(page.Value ?? []);

            // OdataDeltaLink is a direct property — present on the LAST page only.
            // Saving it gives us the incremental sync cursor for the next poll.
            if (page.OdataDeltaLink is not null)
            {
                newLink = page.OdataDeltaLink;
                break;
            }

            // OdataNextLink — more pages within this sync window
            if (page.OdataNextLink is not null)
            {
                page = await _graphClient
                    .Drives[_options.DriveId]
                    .Items["root"]
                    .Delta
                    .WithUrl(page.OdataNextLink)
                    .GetAsDeltaGetResponseAsync(cancellationToken: ct);
            }
            else
            {
                break;
            }
        }

        return (allItems, newLink);
    }

    private async Task<string?> DownloadContentAsync(DriveItem item, CancellationToken ct)
    {
        try
        {
            using var stream = await _graphClient
                .Drives[_options.DriveId]
                .Items[item.Id]
                .Content
                .GetAsync(cancellationToken: ct);

            if (stream is null) return null;
            using var reader = new StreamReader(stream);
            return await reader.ReadToEndAsync(ct);
        }
        catch (ODataError ex) when (ex.ResponseStatusCode == 415)
        {
            Logger.LogDebug("Skipping binary file {Name}", item.Name);
            return null;
        }
        catch (ODataError ex) when (ex.ResponseStatusCode == 403)
        {
            Logger.LogWarning("Access denied for {Id} ({Name})", item.Id, item.Name);
            return null;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Download failed for {Id} ({Name})", item.Id, item.Name);
            return null;
        }
    }
}