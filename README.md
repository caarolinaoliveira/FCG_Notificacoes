# FCG Notificações

Microsserviço responsável pelo envio de notificações por e-mail da plataforma FIAP Cloud Games.

## Responsabilidades

- Enviar e-mail de boas-vindas quando um usuário é cadastrado
- Enviar e-mail de confirmação de compra quando um pagamento é aprovado

## Eventos consumidos

| Evento | Fila | Ação |
|--------|------|------|
| `UserRegisteredEvent` | `user.created` | Envia e-mail de boas-vindas |
| `PaymentProcessedEvent` | `payment.processed.notificacoes` | Envia e-mail de confirmação de compra |

## Variáveis de ambiente

| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `RabbitMQ__Host` | Host do RabbitMQ | `localhost` |
| `RabbitMQ__Port` | Porta do RabbitMQ | `5672` |
| `RabbitMQ__Usuario` | Usuário do RabbitMQ | `guest` |
| `RabbitMQ__Senha` | Senha do RabbitMQ | `guest` |

## Como rodar localmente

```bash
dotnet run --project FCG.Worker
```