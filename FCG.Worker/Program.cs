using FCG.Application.Interfaces;
using FCG.Application.Services;
using FCG.Infrastructure.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polly;
using RabbitMQ.Client;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IConnection>(sp =>
{
    var factory = new ConnectionFactory
    {
        HostName = builder.Configuration["RabbitMQ:Host"] ?? "localhost",
        Port = int.Parse(builder.Configuration["RabbitMQ:Port"] ?? "5672"),
        UserName = builder.Configuration["RabbitMQ:Usuario"] ?? "guest",
        Password = builder.Configuration["RabbitMQ:Senha"] ?? "guest"
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
app.Run();