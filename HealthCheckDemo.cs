using System;

namespace Demos
{
    /// <summary>
    /// Calculates CPU Load of instanse and reports health status
    /// </summary>
    public class HostMachineHardwareHealthCheck : IHealthCheck
    {
        private readonly Logger logger;
        private readonly double unhealthyThreshold = 0.7;
        private readonly double degradatedThreshold = 0.5;
        private readonly int measureWindowInSeconds = 5;

        public HostMachineHardwareHealthCheck(IConfiguration configuration, Logger logger)
        {
            if (configuration != null)
            {
                var section = configuration.GetSection("HostMachineHealthCheck");
                if (section != null)
                {
                    var parseResult = double.TryParse(section.GetValue<string>(nameof(this.unhealthyThreshold)), out var unhealthyThreshold);
                    if (parseResult) this.unhealthyThreshold = unhealthyThreshold;
                    parseResult = double.TryParse(section.GetValue<string>(nameof(this.degradatedThreshold)), out var degradatedThreshold);
                    if (parseResult) this.degradatedThreshold = degradatedThreshold;
                    parseResult = int.TryParse(section.GetValue<string>(nameof(this.measureWindowInSeconds)), out var measureWindowInSeconds);
                    if (parseResult) this.measureWindowInSeconds = measureWindowInSeconds;
                }
            }
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var cpuLoad = await GetCpuLoadAsync(TimeSpan.FromSeconds(this.measureWindowInSeconds));
            var healthResult = healthCheckBrancher(cpuLoad);
            if (healthResult.Status != HealthStatus.Healthy && logger != null) logger.Info(healthResult);
            return healthResult;
        }

        private HealthCheckResult healthCheckBrancher(double cpuLoad)
        {
            if (cpuLoad < degradatedThreshold)
                return HealthCheckResult.Healthy($"CPU Load for current instanse is lower than {degradatedThreshold}");
            else if (cpuLoad < unhealthyThreshold)
                return HealthCheckResult.Degraded($"CPU Load for current instanse is greater than {degradatedThreshold} and lower than {unhealthyThreshold}");
            else
                return HealthCheckResult.Unhealthy($"CPU Load for current instanse is greater than {unhealthyThreshold}");
        }

        public static async Task<double> GetCpuLoadAsync(TimeSpan MeasurementWindow)
        {
            Process CurrentProcess = Process.GetCurrentProcess();

            TimeSpan StartCpuTime = CurrentProcess.TotalProcessorTime;
            Stopwatch Timer = Stopwatch.StartNew();

            await Task.Delay(MeasurementWindow);

            TimeSpan EndCpuTime = CurrentProcess.TotalProcessorTime;
            Timer.Stop();

            var elapsedRealMiliseconds = (EndCpuTime - StartCpuTime).TotalMilliseconds;

            return elapsedRealMiliseconds / (Environment.ProcessorCount * Timer.ElapsedMilliseconds);
        }
    }
}
