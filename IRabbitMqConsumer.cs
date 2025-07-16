using RabbitMQ.Client;

namespace Infrastructure.RabbitMq;

public interface IRabbitMqConsumer
{
    public Task Register(IChannel channel, string exchangeName, string queueName, string routingKey);
}