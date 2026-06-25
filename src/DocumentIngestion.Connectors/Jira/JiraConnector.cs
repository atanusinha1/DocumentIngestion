using RestSharp;
using RestSharp.Authenticators;
using System.Text.Json.Serialization;
using DocumentIngestion.Contracts.Models;
using DocumentIngestion.Connectors.Base;
using Microsoft.Extensions.Options;
using DocumentIngestion.Contracts.Interfaces;
using Azure.Storage.Blobs;

namespace DocumentIngestion.Connectors.Jira;

public class JiraOptions
{
    public string BaseUrl    { get; set; } = string.Empty; // https://your-domain.atlassian.net
    public string Username   { get; set; } = string.Empty;
    public string ApiToken   { get; set; } = string.Empty;
    public string ProjectKey { get; set; } = string.Empty; // e.g. "PROJ"
    public int    PollIntervalMinutes { get; set; } = 5;
}

public class JiraConnector : ConnectorBase
{
    private readonly RestClient  _client;
    private readonly JiraOptions _options;
    private DateTimeOffset _lastPolled = DateTimeOffset.UtcNow.AddMinutes(-60);

    public JiraConnector(
        IMessagePublisher publisher,
        BlobContainerClient blob,
        IOptions<JiraOptions> options,
        ILogger<JiraConnector> logger)
        : base(publisher, blob, logger)
    {
        _options = options.Value;
        _client  = new RestClient(
            new RestClientOptions($"{_options.BaseUrl}/rest/api/3")
            {
                Authenticator = new HttpBasicAuthenticator(
                    _options.Username, _options.ApiToken)
            });
    }

    public override async IAsyncEnumerable<RawDocument> PollAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var since   = _lastPolled.ToString("yyyy-MM-dd HH:mm");
        _lastPolled = DateTimeOffset.UtcNow;

        var jql     = $"project = {_options.ProjectKey} AND updated >= \"{since}\" ORDER BY updated ASC";
        var start   = 0;
        const int maxResults = 50;

        while (true)
        {
            var request = new RestRequest("search")
                .AddParameter("jql",        jql)
                .AddParameter("startAt",    start)
                .AddParameter("maxResults", maxResults)
                .AddParameter("fields",     "summary,description,comment,creator,updated,created,status,issuetype");

            var response = await _client.GetAsync<JiraSearchResult>(request, ct);
            if (response?.Issues is null || response.Issues.Count == 0) break;

            foreach (var issue in response.Issues)
            {
                if (ct.IsCancellationRequested) yield break;

                // Concatenate summary + description + comments for full-text search
                var content = BuildIssueText(issue);
                if (string.IsNullOrWhiteSpace(content)) continue;

                yield return new RawDocument
                {
                    SourceType  = "Jira",
                    SourceId    = issue.Key,
                    Title       = $"[{issue.Key}] {issue.Fields?.Summary}",
                    Content     = content,
                    Url         = $"{_options.BaseUrl}/browse/{issue.Key}",
                    Author      = issue.Fields?.Creator?.DisplayName ?? string.Empty,
                    CreatedAt   = issue.Fields?.Created ?? DateTimeOffset.UtcNow,
                    ModifiedAt  = issue.Fields?.Updated ?? DateTimeOffset.UtcNow,
                    Metadata    = new()
                    {
                        ["project"]   = _options.ProjectKey,
                        ["status"]    = issue.Fields?.Status?.Name ?? string.Empty,
                        ["issueType"] = issue.Fields?.IssueType?.Name ?? string.Empty
                    }
                };
            }

            if (response.Issues.Count < maxResults) break;
            start += maxResults;
        }
    }

    private static string BuildIssueText(JiraIssue issue)
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"Summary: {issue.Fields?.Summary}");
        sb.AppendLine($"Status: {issue.Fields?.Status?.Name}");

        if (!string.IsNullOrWhiteSpace(issue.Fields?.Description))
            sb.AppendLine($"Description:\n{issue.Fields.Description}");

        if (issue.Fields?.Comment?.Comments is { Count: > 0 })
        {
            sb.AppendLine("Comments:");
            foreach (var c in issue.Fields.Comment.Comments)
                sb.AppendLine($"- {c.Author?.DisplayName}: {c.Body}");
        }

        return sb.ToString();
    }
}

// --- DTOs ---
record JiraSearchResult(
    [property: JsonPropertyName("issues")] List<JiraIssue> Issues);

record JiraIssue(
    [property: JsonPropertyName("id")]     string Id,
    [property: JsonPropertyName("key")]    string Key,
    [property: JsonPropertyName("fields")] JiraFields? Fields);

record JiraFields(
    [property: JsonPropertyName("summary")]     string? Summary,
    [property: JsonPropertyName("description")] string? Description,
    [property: JsonPropertyName("creator")]     JiraUser? Creator,
    [property: JsonPropertyName("created")]     DateTimeOffset Created,
    [property: JsonPropertyName("updated")]     DateTimeOffset Updated,
    [property: JsonPropertyName("status")]      JiraStatus? Status,
    [property: JsonPropertyName("issuetype")]   JiraIssueType? IssueType,
    [property: JsonPropertyName("comment")]     JiraCommentList? Comment);

record JiraUser(
    [property: JsonPropertyName("displayName")] string DisplayName);

record JiraStatus(
    [property: JsonPropertyName("name")] string Name);

record JiraIssueType(
    [property: JsonPropertyName("name")] string Name);

record JiraCommentList(
    [property: JsonPropertyName("comments")] List<JiraComment>? Comments);

record JiraComment(
    [property: JsonPropertyName("author")] JiraUser? Author,
    [property: JsonPropertyName("body")]   string? Body);