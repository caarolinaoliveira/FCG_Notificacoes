using System.Diagnostics;

namespace FCG.Infrastructure.Messaging
{
    internal static class Telemetry
    {
        public static readonly ActivitySource Source = new("FCG.Notificacoes.Worker");

        static Telemetry()
        {
            ActivitySource.AddActivityListener(new ActivityListener
            {
                ShouldListenTo = source => source.Name == Source.Name,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            });
        }
    }
}
