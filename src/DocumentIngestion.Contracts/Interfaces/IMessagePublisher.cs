namespace DocumentIngestion.Contracts.Interfaces;

public interface IMessagePublisher
{
    Task PublishAsync<T>(T message, string topic, CancellationToken ct = default)
        where T : class;
}