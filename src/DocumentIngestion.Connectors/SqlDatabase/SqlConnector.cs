using Dapper;
using Microsoft.Data.SqlClient;
using DocumentIngestion.Contracts.Models;
using DocumentIngestion.Connectors.Base;
using Microsoft.Extensions.Options;
using DocumentIngestion.Contracts.Interfaces;
using Azure.Storage.Blobs;

namespace DocumentIngestion.Connectors.SqlDatabase;

public class SqlTableMapping
{
    public string TableName       { get; set; } = string.Empty;
    public string IdColumn        { get; set; } = "Id";
    public string TitleColumn     { get; set; } = "Title";
    public string ContentColumn   { get; set; } = "Content";
    public string ModifiedColumn  { get; set; } = "ModifiedAt";
    public string? AuthorColumn   { get; set; }
    public List<string> MetadataColumns { get; set; } = new();
}

public class SqlConnectorOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    public List<SqlTableMapping> Tables { get; set; } = new();
    public int PollIntervalMinutes { get; set; } = 15;
}

public class SqlConnector : ConnectorBase
{
    private readonly SqlConnectorOptions _options;
    private DateTimeOffset _lastPolled = DateTimeOffset.UtcNow.AddHours(-24);

    public SqlConnector(
        IMessagePublisher publisher,
        BlobContainerClient blob,
        IOptions<SqlConnectorOptions> options,
        ILogger<SqlConnector> logger)
        : base(publisher, blob, logger)
    {
        _options = options.Value;
    }

    public override async IAsyncEnumerable<RawDocument> PollAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var since   = _lastPolled;
        _lastPolled = DateTimeOffset.UtcNow;

        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(ct);

        foreach (var table in _options.Tables)
        {
            if (ct.IsCancellationRequested) yield break;

            var metaCols = table.MetadataColumns.Count > 0
                ? ", " + string.Join(", ", table.MetadataColumns.Select(c => $"[{c}]"))
                : string.Empty;

            var authorCol = table.AuthorColumn is not null
                ? $", [{table.AuthorColumn}]"
                : string.Empty;

            var sql = $"""
                SELECT [{table.IdColumn}], [{table.TitleColumn}], [{table.ContentColumn}],
                       [{table.ModifiedColumn}]{authorCol}{metaCols}
                FROM   [{table.TableName}]
                WHERE  [{table.ModifiedColumn}] >= @Since
                ORDER  BY [{table.ModifiedColumn}] ASC
                """;

            var rows = await conn.QueryAsync(sql, new { Since = since });

            foreach (var row in rows)
            {
                if (ct.IsCancellationRequested) yield break;

                var dict = (IDictionary<string, object>)row;

                var metadata = new Dictionary<string, string>
                {
                    ["table"] = table.TableName
                };

                foreach (var col in table.MetadataColumns)
                {
                    if (dict.TryGetValue(col, out var val) && val is not null)
                        metadata[col] = val.ToString() ?? string.Empty;
                }

                yield return new RawDocument
                {
                    SourceType  = "SQL",
                    SourceId    = dict[table.IdColumn]?.ToString() ?? string.Empty,
                    Title       = dict[table.TitleColumn]?.ToString() ?? string.Empty,
                    Content     = dict[table.ContentColumn]?.ToString() ?? string.Empty,
                    Author      = table.AuthorColumn is not null
                                    ? dict[table.AuthorColumn]?.ToString() ?? string.Empty
                                    : string.Empty,
                    ModifiedAt  = (DateTimeOffset)(DateTime)dict[table.ModifiedColumn],
                    CreatedAt   = (DateTimeOffset)(DateTime)dict[table.ModifiedColumn],
                    Metadata    = metadata
                };
            }
        }
    }
}