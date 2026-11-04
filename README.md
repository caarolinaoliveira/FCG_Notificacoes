# FCG Notificações

Worker contínuo (.NET 8) responsável pelo envio de notificações por e-mail da plataforma FIAP Cloud Games. Consome eventos do RabbitMQ e roda em container (AKS na Fase 4).

> A versão Lambda/SQS da Fase 3 está preservada na tag [`fase-3`](https://github.com/caarolinaoliveira/FCG_Notificacoes/tree/fase-3).

## O que o serviço faz

- Envia e-mail de boas-vindas quando um usuário é cadastrado
- Envia e-mail de confirmação de compra quando um pagamento é aprovado (pagamentos rejeitados são apenas registrados em log, sem e-mail)

## Filas consumidas

| Fila | DLQ | Evento | Ação |
|------|-----|--------|------|
| `user.created` | `user.created.dlq` | `UserRegisteredEvent` | Boas-vindas |
| `payment.processed.notificacoes` | `payment.processed.notificacoes.dlq` | `PaymentProcessedEvent` | Confirmação de compra (só `Status == "Approved"`) |

As filas e a DLQ (via exchange `dlx`) são declaradas pelo próprio worker na inicialização.

## Resiliência

- **Retry:** falha ao processar reenfileira a mensagem com backoff exponencial (2s, 4s), contador no header `x-retry-count`
- **DLQ:** após 3 tentativas, mensagem malformada (JSON inválido ou `null`) ou circuit breaker aberto, a mensagem vai para a DLQ
- **Circuit breaker (Polly):** abre após 3 falhas consecutivas por 30s; enquanto aberto, as mensagens são devolvidas à fila
- **Ack manual** e `prefetch = 1`

## Observabilidade

- Logs estruturados (`ILogger<T>`) com prefixo `[NOTIFICACOES]` e `TraceId` gerado via `ActivitySource` (`FCG.Notificacoes.Worker`)
- Agente New Relic .NET instalado na imagem Docker (app name `FCG-NotificacoesWorker`)

## Variáveis de ambiente

| Variável | Obrigatória | Padrão | Descrição |
|----------|-------------|--------|-----------|
| `RabbitMQ__Host` | não | `localhost` | Host do RabbitMQ |
| `RabbitMQ__Port` | não | `5672` | Porta AMQP |
| `RabbitMQ__Usuario` | **sim** | — | Usuário do RabbitMQ |
| `RabbitMQ__Senha` | **sim** | — | Senha do RabbitMQ |
| `NEW_RELIC_LICENSE_KEY` | em produção | — | Chave de licença da New Relic (injetada em runtime; nunca vai na imagem) |

**Não há credenciais no código nem no repositório.** Se `RabbitMQ__Usuario` ou `RabbitMQ__Senha` não forem informados, o worker registra um erro claro e encerra com exit code 1 na inicialização.

## Como rodar local

Requisitos: .NET 8 SDK (ou superior com o runtime 8) e um RabbitMQ acessível.

```bash
# RabbitMQ local (defina o usuário/senha que quiser)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=<usuario> -e RABBITMQ_DEFAULT_PASS=<senha> \
  rabbitmq:3-management

# Credenciais via user-secrets (o projeto já tem UserSecretsId)
dotnet user-secrets set "RabbitMQ:Usuario" "<usuario>" --project FCG.Worker
dotnet user-secrets set "RabbitMQ:Senha"   "<senha>"   --project FCG.Worker

dotnet run --project FCG.Worker
```

Alternativa sem user-secrets: exporte `RabbitMQ__Usuario` e `RabbitMQ__Senha` no ambiente antes do `dotnet run`.

## Como rodar em container

```bash
docker build -t fcg-notificacoes .

docker run --rm \
  -e RabbitMQ__Host=<host-do-rabbitmq> \
  -e RabbitMQ__Usuario=<usuario> \
  -e RabbitMQ__Senha=<senha> \
  -e NEW_RELIC_LICENSE_KEY=<license-key> \
  fcg-notificacoes
```

A imagem é multi-stage (`sdk:8.0` para build, `runtime:8.0` na final, sem ASP.NET porque o worker não expõe HTTP) e é **linux/amd64** (x64), compatível com os nós do AKS. No Kubernetes, `RabbitMQ__Usuario`, `RabbitMQ__Senha` e `NEW_RELIC_LICENSE_KEY` devem vir de um `Secret`.

## Estrutura

```
FCG.Application/     Eventos e INotificacaoService/NotificacaoService (lógica do e-mail)
FCG.Infrastructure/  Consumers RabbitMQ (BackgroundService), retry, circuit breaker e DLQ
FCG.Worker/          Host (Program.cs): conexão RabbitMQ, policy Polly e DI
```
