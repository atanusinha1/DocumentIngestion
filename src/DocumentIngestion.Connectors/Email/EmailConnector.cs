using MailKit;
using MailKit.Net.Imap;
using MailKit.Search;
using MimeKit;
using DocumentIngestion.Contracts.Models;
using DocumentIngestion.Connectors.Base;
using Microsoft.Extensions.Options;
using DocumentIngestion.Contracts.Interfaces;
using Azure.Storage.Blobs;

namespace DocumentIngestion.Connectors.Email;

public class EmailConnectorOptions
{
    public string ImapHost     { get; set; } = string.Empty;
    public int    ImapPort     { get; set; } = 993;
    public bool   UseSsl       { get; set; } = true;
    public string Username     { get; set; } = string.Empty;
    public string Password     { get; set; } = string.Empty;
    public string Mailbox      { get; set; } = "INBOX";
    public int    PollIntervalMinutes { get; set; } = 5;
}

public class EmailConnector : ConnectorBase
{
    private readonly EmailConnectorOptions _options;
    private DateTimeOffset _lastPolled = DateTimeOffset.UtcNow.AddHours(-24);

    public EmailConnector(
        IMessagePublisher publisher,
        BlobContainerClient blob,
        IOptions<EmailConnectorOptions> options,
        ILogger<EmailConnector> logger)
        : base(publisher, blob, logger)
    {
        _options = options.Value;
    }

    public override async IAsyncEnumerable<RawDocument> PollAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        using var client = new ImapClient();
        await client.ConnectAsync(_options.ImapHost, _options.ImapPort, _options.UseSsl, ct);
        await client.AuthenticateAsync(_options.Username, _options.Password, ct);

        var inbox = await client.GetFolderAsync(_options.Mailbox, ct);
        await inbox.OpenAsync(FolderAccess.ReadOnly, ct);

        // Search for messages received since last poll
        var query   = SearchQuery.DeliveredAfter(_lastPolled.UtcDateTime);
        _lastPolled = DateTimeOffset.UtcNow;

        var uids = await inbox.SearchAsync(query, ct);

        foreach (var uid in uids)
        {
            if (ct.IsCancellationRequested) yield break;

            var message = await inbox.GetMessageAsync(uid, ct);
            var content = ExtractBody(message);

            yield return new RawDocument
            {
                SourceType  = "Email",
                SourceId    = message.MessageId,
                Title       = message.Subject ?? "(no subject)",
                Content     = content,
                Url         = string.Empty,
                Author      = message.From.Mailboxes.FirstOrDefault()?.Address ?? string.Empty,
                CreatedAt   = message.Date,
                ModifiedAt  = message.Date,
                Metadata    = new()
                {
                    ["from"]        = message.From.ToString(),
                    ["to"]          = message.To.ToString(),
                    ["messageId"]   = message.MessageId ?? string.Empty,
                    ["attachments"] = message.Attachments.Count().ToString()
                }
            };
        }

        await client.DisconnectAsync(true, ct);
    }

    private static string ExtractBody(MimeMessage message)
    {
        // Prefer plain text; fall back to stripping HTML
        if (!string.IsNullOrWhiteSpace(message.TextBody))
            return message.TextBody;

        if (!string.IsNullOrWhiteSpace(message.HtmlBody))
        {
            var doc = new HtmlAgilityPack.HtmlDocument();
            doc.LoadHtml(message.HtmlBody);
            return doc.DocumentNode.InnerText.Trim();
        }

        return string.Empty;
    }
}