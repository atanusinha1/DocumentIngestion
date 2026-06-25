namespace DocumentIngestion.Contracts.Interfaces;

public interface IMessageConsumer
{
    Task SubscribeAsync<T>(
        string topic,
        Func<T, CancellationToken, Task> handler,
        CancellationToken ct = default)
        where T : class;
}