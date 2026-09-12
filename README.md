# FCG Notificações

Função serverless (AWS Lambda) responsável pelo envio de notificações por e-mail da plataforma FIAP Cloud Games. Substitui o antigo worker que rodava continuamente consumindo RabbitMQ — agora são duas funções acionadas diretamente por mensagens novas nas filas SQS, sem nenhum processo ocioso 24/7.

## Responsabilidades

- Enviar e-mail de boas-vindas quando um usuário é cadastrado
- Enviar e-mail de confirmação de compra quando um pagamento é aprovado

## Arquitetura

| Fila SQS | Handler | Ação |
|----------|---------|------|
| `user-created` | `FCG.Worker::FCG.Worker.SqsFunction::UsuarioCriadoHandler` | Envia e-mail de boas-vindas |
| `payment-processed-notificacoes` | `FCG.Worker::FCG.Worker.SqsFunction::PagamentoProcessadoHandler` | Envia e-mail de confirmação de compra (só para pagamentos `Approved`) |

Cada fila tem uma DLQ (`<fila>-dlq`) com `maxReceiveCount: 3` — mensagens que falham 3 vezes vão parar lá em vez de ficar reprocessando pra sempre. O trigger é um **Event Source Mapping** SQS→Lambda nativo da AWS (batch de 5, `ReportBatchItemFailures` para reentrega parcial de lote), não um polling manual.

O código da função está em [`FCG.Worker/SqsFunction.cs`](FCG.Worker/SqsFunction.cs); a lógica de negócio (o que cada e-mail faz) está em [`FCG.Application/Services/NotificationService.cs`](FCG.Application/Services/NotificationService.cs).

## Infraestrutura como código

O template SAM em [`template.yaml`](template.yaml) declara as duas funções, as filas SQS + DLQs e os Event Source Mappings — é a fonte de verdade da infraestrutura desta função, versionada neste repositório (não no de orquestração).

### Deploy em AWS real

```bash
sam build
sam deploy --guided
```

No modo guiado, informe `NewRelicLicenseKey`/`NewRelicAccountId` (opcional, para telemetria de APM) e `NewRelicLambdaLayerArn` (ARN da layer `NewRelicDotnet`/`NewRelicDotnetARM64` da sua região — consulte `https://<região>.layers.newrelic-external.com/get-layers?CompatibleRuntime=dotnet8`). Deixe os três em branco para publicar sem instrumentação de APM.

### Desenvolvimento local (LocalStack)

Para rodar localmente como parte do ambiente completo (`FCG_Orquestracao/docker-compose.yaml`), o LocalStack registra as mesmas duas funções via `localstack-init/00-setup-notificacoes.sh` a partir de um zip publicado manualmente (LocalStack Community não builda projetos .NET via `sam build`):

```bash
./package-lambda.ps1
```

Gera `../FCG_Orquestracao/localstack-init/artifacts/notificacoes.zip`; suba/reinicie o LocalStack (`docker compose restart localstack` no repo de orquestração) para registrar as funções e os Event Source Mappings a partir dele. Esse caminho é só para dev local — o `template.yaml` é a IaC de verdade para deploy em AWS.

## Resiliência

- Retry automático via Event Source Mapping + DLQ (`maxReceiveCount: 3`)
- `ReportBatchItemFailures` — mensagens com falha num lote não derrubam o lote inteiro, só são reentregues individualmente

## Observabilidade

Instrumentado com o agente .NET da New Relic (profiler + Lambda Layer). Ver [`FCG_Orquestracao/README.md`](../FCG_Orquestracao/README.md) para a documentação completa da stack de observabilidade do sistema.

> ⚠️ As credenciais/ARNs de exemplo neste repositório e no `localstack-init` são exclusivos para ambiente de desenvolvimento local. Não utilize em produção.
