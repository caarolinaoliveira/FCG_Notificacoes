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
        public const string QueueName = "payment.processed.notificacoes";

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

        private async Task<IChannel> EnsureChannelAsync()
        {
            if (_channel is not null)
                return _channel;

            var channel = await _connection.CreateChannelAsync();

            await channel.ExchangeDeclareAsync("dlx", ExchangeType.Direct, durable: true);

            await channel.QueueDeclareAsync(
                queue: "payment.processed.notificacoes.dlq",
                durable: true,
                exclusive: false,
                autoDelete: false
            );
            await channel.QueueBindAsync("payment.processed.notificacoes.dlq", "dlx", QueueName);

            var args = new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", "dlx" },
                { "x-dead-letter-routing-key", QueueName }
            };

            await channel.QueueDeclareAsync(
                queue: QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: args
            );

            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

            _channel = channel;
            return _channel;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var channel = await EnsureChannelAsync();

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (sender, ea) =>
                ProcessDeliveryAsync(ea.Body.ToArray(), ea.BasicProperties.Headers, ea.DeliveryTag);

            await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer);
            _logger.LogInformation("[NOTIFICACOES] Aguardando mensagens na fila {Queue}...", QueueName);

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        // Drena a fila em lote (pull), sem manter um listener aberto — usado pelo handler Lambda,
        // que precisa retornar em vez de escutar a fila indefinidamente.
        public async Task<int> DrainOnceAsync(int maxMessages = 100)
        {
            var channel = await EnsureChannelAsync();
            var processed = 0;

            while (processed < maxMessages)
            {
                var result = await channel.BasicGetAsync(QueueName, autoAck: false);
                if (result is null)
                    break;

                await ProcessDeliveryAsync(result.Body.ToArray(), result.BasicProperties.Headers, result.DeliveryTag);
                processed++;
            }

            return processed;
        }

        private async Task ProcessDeliveryAsync(byte[] body, IDictionary<string, object?>? headers, ulong deliveryTag)
        {
            var channel = _channel!;

            if (_circuitBreaker.CircuitState == CircuitState.Open)
            {
                _logger.LogWarning("[NOTIFICACOES] Circuito aberto — reenfileirando mensagem.");
                await channel.BasicNackAsync(deliveryTag, false, requeue: true);
                return;
            }

            var json = Encoding.UTF8.GetString(body);
            var tentativas = 0;

            if (headers != null && headers.TryGetValue("x-retry-count", out var retryObj))
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
                    await channel.BasicNackAsync(deliveryTag, false, requeue: false);
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

                await channel.BasicAckAsync(deliveryTag, false);
            }
            catch (BrokenCircuitException ex)
            {
                _logger.LogError(ex, "[NOTIFICACOES] Circuit breaker aberto — enviando para DLQ.");
                await channel.BasicNackAsync(deliveryTag, false, requeue: false);
            }
            catch (Exception ex)
            {
                tentativas++;

                if (tentativas >= 3)
                {
                    _logger.LogError(ex, "[NOTIFICACOES] Máximo de tentativas — enviando para DLQ.");
                    await channel.BasicNackAsync(deliveryTag, false, requeue: false);
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

                await channel.BasicPublishAsync("", QueueName, false, props, body);
                await channel.BasicAckAsync(deliveryTag, false);
            }
        }

        public override void Dispose()
        {
            _channel?.CloseAsync().GetAwaiter().GetResult();
            _channel?.DisposeAsync().GetAwaiter().GetResult();
            base.Dispose();
        }
    }
}
