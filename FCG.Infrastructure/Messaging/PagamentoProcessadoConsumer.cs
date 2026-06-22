using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using FCG.Application.Events;
using FCG.Application.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Polly;
using Polly.CircuitBreaker;

namespace FCG.Infrastructure.Messaging
{
    public class PagamentoProcessadoConsumer : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<PagamentoProcessadoConsumer> _logger;
        private readonly IConnection _connection;
        private IChannel? _channel;
        private readonly AsyncCircuitBreakerPolicy _circuitBreaker;

        public PagamentoProcessadoConsumer(
            IServiceScopeFactory scopeFactory,
            ILogger<PagamentoProcessadoConsumer> logger,
            IConnection connection,
            AsyncCircuitBreakerPolicy circuitBreaker)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
            _connection = connection;
            _circuitBreaker = circuitBreaker;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _channel = await _connection.CreateChannelAsync();

            await _channel.ExchangeDeclareAsync("dlx", ExchangeType.Direct, durable: true);

            await _channel.QueueDeclareAsync(
                queue: "payment.processed.notificacoes.dlq",
                durable: true,
                exclusive: false,
                autoDelete: false
            );
            await _channel.QueueBindAsync("payment.processed.notificacoes.dlq", "dlx", "payment.processed.notificacoes");

            var args = new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", "dlx" },
                { "x-dead-letter-routing-key", "payment.processed.notificacoes" }
            };

            await _channel.QueueDeclareAsync(
                queue: "payment.processed.notificacoes",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: args
            );

            await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

            var consumer = new AsyncEventingBasicConsumer(_channel);

            consumer.ReceivedAsync += async (sender, ea) =>
            {
                if (_circuitBreaker.CircuitState == CircuitState.Open)
                {
                    _logger.LogWarning("[NOTIFICACOES] Circuito aberto — reenfileirando mensagem.");
                    await _channel!.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
                    return;
                }

                var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                var tentativas = 0;

                if (ea.BasicProperties.Headers != null &&
                    ea.BasicProperties.Headers.TryGetValue("x-retry-count", out var retryObj))
                {
                    tentativas = Convert.ToInt32(retryObj);
                }

                try
                {
                    var evento = JsonSerializer.Deserialize<PaymentProcessedEvent>(json,
                        new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                    if (evento is null)
                    {
                        _logger.LogWarning("[NOTIFICACOES] Evento null — descartando.");
                        await _channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                        return;
                    }

                    if (evento.Status == "Approved")
                    {
                        using var scope = _scopeFactory.CreateScope();
                        var notificacaoService = scope.ServiceProvider.GetRequiredService<INotificacaoService>();
                        await notificacaoService.EnviarConfirmacaoCompraAsync(evento);
                        _logger.LogInformation("[NOTIFICACOES] Confirmação de compra enviada | UserId: {Id}", evento.UserId);
                    }
                    else
                    {
                        _logger.LogInformation("[NOTIFICACOES] Pagamento rejeitado — sem notificação | UserId: {Id}", evento.UserId);
                    }

                    await _channel.BasicAckAsync(ea.DeliveryTag, false);
                }
                catch (BrokenCircuitException ex)
                {
                    _logger.LogError(ex, "[NOTIFICACOES] Circuit breaker aberto — enviando para DLQ.");
                    await _channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                }
                catch (Exception ex)
                {
                    tentativas++;

                    if (tentativas >= 3)
                    {
                        _logger.LogError(ex, "[NOTIFICACOES] Máximo de tentativas — enviando para DLQ.");
                        await _channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                        return;
                    }

                    _logger.LogWarning("[NOTIFICACOES] Tentativa {N} falhou — reenfileirando.", tentativas);

                    var props = new BasicProperties
                    {
                        Persistent = true,
                        Headers = new Dictionary<string, object?>
                        {
                            { "x-retry-count", tentativas }
                        }
                    };

                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, tentativas)));

                    await _channel.BasicPublishAsync("", "payment.processed.notificacoes", false, props, ea.Body);
                    await _channel.BasicAckAsync(ea.DeliveryTag, false);
                }
            };

            await _channel.BasicConsumeAsync("payment.processed.notificacoes", autoAck: false, consumer: consumer);
            _logger.LogInformation("[NOTIFICACOES] Aguardando mensagens na fila payment.processed.notificacoes...");

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        public override void Dispose()
        {
            _channel?.CloseAsync().GetAwaiter().GetResult();
            _channel?.DisposeAsync().GetAwaiter().GetResult();
            base.Dispose();
        }
    }
}