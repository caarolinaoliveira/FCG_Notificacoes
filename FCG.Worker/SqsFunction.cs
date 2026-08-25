using System.Text.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using FCG.Application.Events;
using FCG.Application.Interfaces;
using FCG.Application.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace FCG.Worker
{
    public class SqsFunction
    {
        private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

        private readonly ServiceProvider _serviceProvider;

        public SqsFunction()
        {
            var services = new ServiceCollection();
            services.AddLogging(builder => builder.AddConsole());
            services.AddScoped<INotificacaoService, NotificacaoService>();
            _serviceProvider = services.BuildServiceProvider();
        }

        public async Task<SQSBatchResponse> UsuarioCriadoHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            var failures = new List<SQSBatchResponse.BatchItemFailure>();

            foreach (var record in sqsEvent.Records)
            {
                try
                {
                    var evento = JsonSerializer.Deserialize<UserRegisteredEvent>(record.Body, JsonOptions);
                    if (evento is null)
                    {
                        context.Logger.LogWarning($"[NOTIFICACOES] Evento null — descartando mensagem {record.MessageId}.");
                        continue;
                    }

                    using var scope = _serviceProvider.CreateScope();
                    var notificacaoService = scope.ServiceProvider.GetRequiredService<INotificacaoService>();
                    await notificacaoService.EnviarBoasVindasAsync(evento);

                    context.Logger.LogInformation($"[NOTIFICACOES] Processado | UsuarioId: {evento.UserId}");
                }
                catch (Exception ex)
                {
                    context.Logger.LogError($"[NOTIFICACOES] Falha ao processar mensagem {record.MessageId}: {ex.Message}");
                    failures.Add(new SQSBatchResponse.BatchItemFailure { ItemIdentifier = record.MessageId });
                }
            }

            return new SQSBatchResponse(failures);
        }

        public async Task<SQSBatchResponse> PagamentoProcessadoHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            var failures = new List<SQSBatchResponse.BatchItemFailure>();

            foreach (var record in sqsEvent.Records)
            {
                try
                {
                    var evento = JsonSerializer.Deserialize<PaymentProcessedEvent>(record.Body, JsonOptions);
                    if (evento is null)
                    {
                        context.Logger.LogWarning($"[NOTIFICACOES] Evento null — descartando mensagem {record.MessageId}.");
                        continue;
                    }

                    if (evento.Status == "Approved")
                    {
                        using var scope = _serviceProvider.CreateScope();
                        var notificacaoService = scope.ServiceProvider.GetRequiredService<INotificacaoService>();
                        await notificacaoService.EnviarConfirmacaoCompraAsync(evento);
                        context.Logger.LogInformation($"[NOTIFICACOES] Confirmação de compra enviada | UserId: {evento.UserId}");
                    }
                    else
                    {
                        context.Logger.LogInformation($"[NOTIFICACOES] Pagamento rejeitado — sem notificação | UserId: {evento.UserId}");
                    }
                }
                catch (Exception ex)
                {
                    context.Logger.LogError($"[NOTIFICACOES] Falha ao processar mensagem {record.MessageId}: {ex.Message}");
                    failures.Add(new SQSBatchResponse.BatchItemFailure { ItemIdentifier = record.MessageId });
                }
            }

            return new SQSBatchResponse(failures);
        }
    }
}
