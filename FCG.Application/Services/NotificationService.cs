using FCG.Application.Events;
using FCG.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace FCG.Application.Services
{
    public class NotificacaoService : INotificacaoService
    {
        private readonly ILogger<NotificacaoService> _logger;

        public NotificacaoService(ILogger<NotificacaoService> logger)
        {
            _logger = logger;
        }

        public async Task EnviarBoasVindasAsync(UserRegisteredEvent evento)
        {
            _logger.LogInformation(
                "[EMAIL] Boas-vindas enviado para {Email} | UsuarioId: {Id}",
                evento.Email,
                evento.UserId);

            await Task.CompletedTask;
        }

        public async Task EnviarConfirmacaoCompraAsync(PaymentProcessedEvent evento)
        {
            _logger.LogInformation(
                "[EMAIL] Confirmação de compra enviada | UserId: {UserId} | GameId: {GameId} | Valor: {Price}",
                evento.UserId, evento.GameId, evento.Price);

            await Task.CompletedTask;
        
        }
    }
}