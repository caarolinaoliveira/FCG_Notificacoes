using Amazon.Lambda.Core;
using FCG.Application.Interfaces;
using FCG.Application.Services;
using FCG.Infrastructure.Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using RabbitMQ.Client;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace FCG.Worker
{
    public class Function
    {
        private readonly ServiceProvider _serviceProvider;

        public Function()
        {
            var configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            var services = new ServiceCollection();

            services.AddLogging(builder => builder.AddConsole());

            services.AddSingleton<IConnection>(sp =>
            {
                var factory = new ConnectionFactory
                {
                    HostName = configuration["RabbitMQ:Host"] ?? "localhost",
                    Port = int.Parse(configuration["RabbitMQ:Port"] ?? "5672"),
                    UserName = configuration["RabbitMQ:Usuario"] ?? "guest",
                    Password = configuration["RabbitMQ:Senha"] ?? "guest"
                };
                return factory.CreateConnectionAsync().GetAwaiter().GetResult();
            });

            services.AddSingleton(Policy
                .Handle<Exception>()
                .CircuitBreakerAsync(
                    exceptionsAllowedBeforeBreaking: 3,
                    durationOfBreak: TimeSpan.FromSeconds(30)
                ));

            services.AddScoped<INotificacaoService, NotificacaoService>();

            services.AddSingleton<UsuarioCadastradoConsumer>();
            services.AddSingleton<PagamentoProcessadoConsumer>();

            _serviceProvider = services.BuildServiceProvider();
        }

        public async Task<object> FunctionHandler(object input, ILambdaContext context)
        {
            var usuarioConsumer = _serviceProvider.GetRequiredService<UsuarioCadastradoConsumer>();
            var pagamentoConsumer = _serviceProvider.GetRequiredService<PagamentoProcessadoConsumer>();

            var usuariosProcessados = await usuarioConsumer.DrainOnceAsync();
            var pagamentosProcessados = await pagamentoConsumer.DrainOnceAsync();

            context.Logger.LogInformation(
                $"[NOTIFICACOES] Lambda drenou {usuariosProcessados} mensagem(ns) de {UsuarioCadastradoConsumer.QueueName} e {pagamentosProcessados} de {PagamentoProcessadoConsumer.QueueName}.");

            return new { usuariosProcessados, pagamentosProcessados };
        }
    }
}
