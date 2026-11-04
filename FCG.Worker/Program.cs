using FCG.Application.Interfaces;
using FCG.Application.Services;
using FCG.Infrastructure.Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using RabbitMQ.Client;

var builder = Host.CreateApplicationBuilder(args);


var rabbitHost = builder.Configuration["RabbitMQ:Host"] ?? "localhost";
var rabbitPort = builder.Configuration.GetValue<int?>("RabbitMQ:Port") ?? 5672;
var rabbitUsuario = builder.Configuration["RabbitMQ:Usuario"];
var rabbitSenha = builder.Configuration["RabbitMQ:Senha"];

builder.Services.AddSingleton<IConnection>(sp =>
{
    var factory = new ConnectionFactory
    {
        HostName = rabbitHost,
        Port = rabbitPort,
        UserName = rabbitUsuario!,
        Password = rabbitSenha!
    };
    return factory.CreateConnectionAsync().GetAwaiter().GetResult();
});

builder.Services.AddSingleton(Policy
    .Handle<Exception>()
    .CircuitBreakerAsync(
        exceptionsAllowedBeforeBreaking: 3,
        durationOfBreak: TimeSpan.FromSeconds(30)
    ));

builder.Services.AddScoped<INotificacaoService, NotificacaoService>();

builder.Services.AddHostedService<UsuarioCadastradoConsumer>();
builder.Services.AddHostedService<PagamentoProcessadoConsumer>();

var app = builder.Build();


if (string.IsNullOrWhiteSpace(rabbitUsuario) || string.IsNullOrWhiteSpace(rabbitSenha))
{
    var logger = app.Services.GetRequiredService<ILogger<Program>>();
    logger.LogCritical(
        "[NOTIFICACOES] Credenciais do RabbitMQ não configuradas. Informe {ChaveUsuario} e {ChaveSenha} " +
        "via variáveis de ambiente (RabbitMQ__Usuario / RabbitMQ__Senha) ou dotnet user-secrets. " +
        "Não há valor padrão por segurança.",
        "RabbitMQ:Usuario", "RabbitMQ:Senha");
    return 1;
}

await app.RunAsync();
return 0;
