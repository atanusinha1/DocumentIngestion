using RabbitMQ.Client;

namespace DocumentIngestion.Messaging.RabbitMQ;

public class RabbitMqConnectionFactory
{
    private readonly RabbitMqOptions _options;

    public RabbitMqConnectionFactory(RabbitMqOptions options)
        => _options = options;

    // v7: CreateConnectionAsync (sync CreateConnection removed)
    public async Task<IConnection> CreateConnectionAsync(
        CancellationToken ct = default)
    {
        var factory = new ConnectionFactory
        {
            HostName    = _options.Host,
            Port        = _options.Port,
            VirtualHost = _options.VirtualHost,
            UserName    = _options.Username,
            Password    = _options.Password,
            AutomaticRecoveryEnabled = true,
        };
        return await factory.CreateConnectionAsync(cancellationToken: ct);
    }
}