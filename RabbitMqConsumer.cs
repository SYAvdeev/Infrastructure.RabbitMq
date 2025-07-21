using System.Diagnostics;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Infrastructure.RabbitMq;

public abstract class RabbitMqConsumer : IRabbitMqConsumer
{
    public async Task Register(IChannel channel, string exchangeName, string queueName, string routingKey)
    {
        await channel.BasicQosAsync(0, 10, false);
        
        await WaitForExchangeAsync(channel, exchangeName);
        
        await channel.QueueDeclareAsync(queueName, false, false, false, null);
        await channel.QueueBindAsync(queueName, exchangeName, routingKey, null);

        var consumer = new AsyncEventingBasicConsumer(channel);
        
        consumer.ReceivedAsync += async (sender, e) =>
        {
            await OnConsumerOnReceivedAsync(sender, e);
            await channel.BasicAckAsync(e.DeliveryTag, false);
        };

        await channel.BasicConsumeAsync(queueName, false, consumer);
    }
    
    private async Task WaitForExchangeAsync(IChannel channel, string exchangeName, int timeoutSeconds = 30)
    {
        var timeout = TimeSpan.FromSeconds(timeoutSeconds);
        var delay = TimeSpan.FromSeconds(2);
        var sw = Stopwatch.StartNew();

        while (sw.Elapsed < timeout)
        {
            try
            {
                await channel.ExchangeDeclarePassiveAsync(exchangeName);
                return;
            }
            catch
            {
                await Task.Delay(delay);
            }
        }

        throw new TimeoutException($"Timed out waiting for exchange '{exchangeName}' to appear.");
    }


    protected abstract Task OnConsumerOnReceivedAsync(object sender, BasicDeliverEventArgs e);
}