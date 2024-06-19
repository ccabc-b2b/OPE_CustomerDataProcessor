using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CustomerJsonData
{
    public class Logger
    {
        private readonly IConfiguration _configuration;
        public Logger(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        public async void ErrorLogData(Exception ex, string errorMessage)
        {
            var channel = new InMemoryChannel();
            try
            {
                IServiceCollection services = new ServiceCollection();
                services.Configure<TelemetryConfiguration>(config => config.TelemetryChannel = channel);
                services.AddLogging(builder =>
                {
                    
                    builder.AddApplicationInsights(
                        configureTelemetryConfiguration: (config) => config.ConnectionString = _configuration["AppInsightsConnectionString"],
                        configureApplicationInsightsLoggerOptions: (options) => { }
                    );
                });
                IServiceProvider serviceProvider = services.BuildServiceProvider();
                ILogger<Program> logger = serviceProvider.GetRequiredService<ILogger<Program>>();
                logger.LogError(ex, errorMessage);
            }
            finally
            {
                channel.Flush();
                await Task.Delay(TimeSpan.FromMilliseconds(1000));
                System.Environment.Exit(0);
            }
        }
    }
}
