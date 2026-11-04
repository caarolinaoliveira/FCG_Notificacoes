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
        private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

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
                using var activity = Telemetry.Source.StartActivity("notificacoes.pagamento-processado");
                var traceId = activity?.TraceId.ToString();

                if (_circuitBreaker.CircuitState == CircuitState.Open)
                {
                    _logger.LogWarning("[NOTIFICACOES] TraceId: {TraceId} | Circuito aberto — reenfileirando mensagem.", traceId);
                    // Pausa curta para a mensagem não voltar em loop apertado enquanto o circuito está aberto.
                    await Task.Delay(TimeSpan.FromSeconds(1));
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
                    var evento = JsonSerializer.Deserialize<PaymentProcessedEvent>(json, JsonOptions);

                    if (evento is null)
                    {
                        _logger.LogWarning("[NOTIFICACOES] TraceId: {TraceId} | Evento null — enviando para DLQ.", traceId);
                        await _channel!.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                        return;
                    }

                    if (evento.Status == "Approved")
                    {
                        await _circuitBreaker.ExecuteAsync(async () =>
                        {
                            using var scope = _scopeFactory.CreateScope();
                            var notificacaoService = scope.ServiceProvider.GetRequiredService<INotificacaoService>();
                            await notificacaoService.EnviarConfirmacaoCompraAsync(evento);
                        });
                        _logger.LogInformation("[NOTIFICACOES] TraceId: {TraceId} | Confirmação de compra enviada | UserId: {UserId}", traceId, evento.UserId);
                    }
                    else
                    {
                        _logger.LogInformation("[NOTIFICACOES] TraceId: {TraceId} | Pagamento rejeitado — sem notificação | UserId: {UserId}", traceId, evento.UserId);
                    }

                    await _channel!.BasicAckAsync(ea.DeliveryTag, false);
                }
                catch (JsonException ex)
                {
                    // Mensagem malformada nunca vai ser processada: retentar só atrasa. Vai direto para a DLQ.
                    _logger.LogError(ex, "[NOTIFICACOES] TraceId: {TraceId} | Mensagem malformada — enviando para DLQ.", traceId);
                    await _channel!.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                }
                catch (BrokenCircuitException ex)
                {
                    _logger.LogError(ex, "[NOTIFICACOES] TraceId: {TraceId} | Circuit breaker aberto — enviando para DLQ.", traceId);
                    await _channel!.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                }
                catch (Exception ex)
                {
                    tentativas++;

                    if (tentativas >= 3)
                    {
                        _logger.LogError(ex, "[NOTIFICACOES] TraceId: {TraceId} | Máximo de tentativas — enviando para DLQ.", traceId);
                        await _channel!.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                        return;
                    }

                    _logger.LogWarning("[NOTIFICACOES] TraceId: {TraceId} | Tentativa {Tentativa} falhou — reenfileirando.", traceId, tentativas);

                    var props = new BasicProperties
                    {
                        Persistent = true,
                        Headers = new Dictionary<string, object?>
                        {
                            { "x-retry-count", tentativas }
                        }
                    };

                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, tentativas)));

                    await _channel!.BasicPublishAsync("", "payment.processed.notificacoes", false, props, ea.Body);
                    await _channel.BasicAckAsync(ea.DeliveryTag, false);
                }
            };

            await _channel.BasicConsumeAsync("payment.processed.notificacoes", autoAck: false, consumer: consumer);
            _logger.LogInformation("[NOTIFICACOES] Aguardando mensagens na fila {Fila}...", "payment.processed.notificacoes");

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
