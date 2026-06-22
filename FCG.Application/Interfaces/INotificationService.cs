using FCG.Application.Events;

namespace FCG.Application.Interfaces
{
    public interface INotificacaoService
    {
        Task EnviarBoasVindasAsync(UserRegisteredEvent evento);
        Task EnviarConfirmacaoCompraAsync(PaymentProcessedEvent evento);
        
    }
}