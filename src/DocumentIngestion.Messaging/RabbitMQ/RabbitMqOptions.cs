namespace DocumentIngestion.Messaging.RabbitMQ;

public class RabbitMqOptions
{
    public string Host         { get; set; } = "localhost";
    public int    Port         { get; set; } = 5672;
    public string VirtualHost  { get; set; } = "/";
    public string Username     { get; set; } = "guest";
    public string Password     { get; set; } = "guest";
    public string ExchangeName { get; set; } = "document-ingestion";
    public bool   Durable      { get; set; } = true;
}