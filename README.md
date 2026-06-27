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

## Resiliência

- Circuit breaker com 3 falhas antes de abrir (30s de break)
- Retry com exponential backoff — 2s, 4s, 8s
- Dead Letter Queue (DLQ) para mensagens que excederam as tentativas

## Variáveis de ambiente

| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `DOTNET_ENVIRONMENT` | Ambiente da aplicação | `Production` |
| `RabbitMQ__Host` | Host do RabbitMQ | `localhost` |
| `RabbitMQ__Port` | Porta do RabbitMQ | `5672` |
| `RabbitMQ__Usuario` | Usuário do RabbitMQ | `guest` |
| `RabbitMQ__Senha` | Senha do RabbitMQ | `guest` |

## Como rodar localmente

```bash
dotnet run --project FCG.Worker
```

## Como rodar com Docker

```bash
docker-compose up
```

## Deploy no Kubernetes

```bash
kubectl apply -f k8s/
```

> ⚠️ As credenciais neste repositório são exclusivas para ambiente de desenvolvimento local.