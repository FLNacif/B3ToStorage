using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace B3ToStorageSchedule
{
    public static class B3ToBlobScheduler
    {
        [FunctionName("B3ToBlobScheduler")]
        public static void Run([TimerTrigger("0 0 2 * * TUE-SAT")] TimerInfo myTimer,
            ILogger log)
        {
            log.LogInformation($"Iniciado processo de importação da B3.");
            B3ToStorage.B3ToStorage.Run(log);

            log.LogInformation($"Processo de importação da B3 finalizado.");

        }
    }
}
