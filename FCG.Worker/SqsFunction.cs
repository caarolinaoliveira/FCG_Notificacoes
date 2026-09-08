using System.Diagnostics;
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

        private static readonly ActivitySource ActivitySource = new("FCG.Notificacoes.SqsFunction");

        static SqsFunction()
        {
            ActivitySource.AddActivityListener(new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            });
        }

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
                using var activity = ActivitySource.StartActivity("notificacoes.usuario-criado");

                try
                {
                    var evento = JsonSerializer.Deserialize<UserRegisteredEvent>(record.Body, JsonOptions);
                    if (evento is null)
                    {
                        context.Logger.LogWarning($"[NOTIFICACOES] TraceId: {activity?.TraceId} | Evento null — descartando mensagem {record.MessageId}.");
                        continue;
                    }

                    using var scope = _serviceProvider.CreateScope();
                    var notificacaoService = scope.ServiceProvider.GetRequiredService<INotificacaoService>();
                    await notificacaoService.EnviarBoasVindasAsync(evento);

                    context.Logger.LogInformation($"[NOTIFICACOES] TraceId: {activity?.TraceId} | Processado | UsuarioId: {evento.UserId}");
                }
                catch (Exception ex)
                {
                    context.Logger.LogError($"[NOTIFICACOES] TraceId: {activity?.TraceId} | Falha ao processar mensagem {record.MessageId}: {ex.Message}");
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
                using var activity = ActivitySource.StartActivity("notificacoes.pagamento-processado");

                try
                {
                    var evento = JsonSerializer.Deserialize<PaymentProcessedEvent>(record.Body, JsonOptions);
                    if (evento is null)
                    {
                        context.Logger.LogWarning($"[NOTIFICACOES] TraceId: {activity?.TraceId} | Evento null — descartando mensagem {record.MessageId}.");
                        continue;
                    }

                    if (evento.Status == "Approved")
                    {
                        using var scope = _serviceProvider.CreateScope();
                        var notificacaoService = scope.ServiceProvider.GetRequiredService<INotificacaoService>();
                        await notificacaoService.EnviarConfirmacaoCompraAsync(evento);
                        context.Logger.LogInformation($"[NOTIFICACOES] TraceId: {activity?.TraceId} | Confirmação de compra enviada | UserId: {evento.UserId}");
                    }
                    else
                    {
                        context.Logger.LogInformation($"[NOTIFICACOES] TraceId: {activity?.TraceId} | Pagamento rejeitado — sem notificação | UserId: {evento.UserId}");
                    }
                }
                catch (Exception ex)
                {
                    context.Logger.LogError($"[NOTIFICACOES] TraceId: {activity?.TraceId} | Falha ao processar mensagem {record.MessageId}: {ex.Message}");
                    failures.Add(new SQSBatchResponse.BatchItemFailure { ItemIdentifier = record.MessageId });
                }
            }

            return new SQSBatchResponse(failures);
        }
    }
}
