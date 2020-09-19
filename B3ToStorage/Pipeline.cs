using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;

namespace B3ToStorage
{
    public static class B3ToStorage
    {
        static HttpClient http = new HttpClient();
        static string connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");

        static string blobFolder = "b3";

        static ILogger log;
        static string baseUrl = $"http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D%date%.ZIP";

        public static void Run(ILogger _log)
        {
            log = _log;
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient("financeiro");


            // Baixar meu arquivo mais novo
            log.LogInformation($"Listando arquivos do storage na pasta '{blobFolder}'.");
            var listaDeArquivos = ListBlobsFlatListing(containerClient, blobFolder);
            var ultimoArquivo = listaDeArquivos.OrderBy(x => x).LastOrDefault();
            log.LogInformation($"Arquivo mais recente encontrado '{ultimoArquivo}'.");
            List<Cotacao> cotacoes;
            if (ultimoArquivo != null)
            {
                log.LogInformation($"Lendo arquivo mais recente.");
                cotacoes = ReadFromBlob(containerClient, ultimoArquivo);
                log.LogInformation($"Fim da leitura do arquivo mais recente.");
            }
            else
            {
                cotacoes = new List<Cotacao>();
            }
            // Verificar qual a última data deste arquivo. Se não possuir, é o início do ano atual.
            var ultimaData = cotacoes.OrderBy(x => x.Data).LastOrDefault()?.Data ?? new DateTime(DateTime.Today.Year, 1, 1);
            log.LogInformation($"Última data importada '{ultimaData}'");
            ultimaData = ultimaData.AddDays(1);
            log.LogInformation($"Importando dados a partir de '{ultimaData}'");
            // Começar a requisitar dados a partir desta data
            DateTime dataForLoop;
            for (dataForLoop = ultimaData; dataForLoop <= DateTime.Today; dataForLoop = dataForLoop.AddDays(1))
            {
                if (dataForLoop.Year != ultimaData.Year)
                {
                    // Se mudou de ano, então é um novo arquivo. Escreve essas cotacoes no storage e começa uma nova lista.
                    log.LogInformation($"Escrevendo arquivo referente ao ano '{ultimaData.Year}' no storage. Um novo ano se inicia.");
                    WriteToBlob(containerClient, cotacoes);
                    log.LogInformation($"Escrita do arquivo finalizada.");
                    cotacoes.Clear();
                    ultimaData = dataForLoop;
                }
                log.LogInformation($"Importando dados de '{dataForLoop}'");
                cotacoes.AddRange(BaixarDadoB3(dataForLoop));
            }
            // Escreve essas cotacoes no storage.
            log.LogInformation($"Escrevendo arquivo refernete ao ano '{dataForLoop.Year}' no storage.");
            WriteToBlob(containerClient, cotacoes);
            log.LogInformation($"Escrita do arquivo finalizada.");
            cotacoes.Clear();
        }


        private static List<Cotacao> TxtToCotacao(string data)
        {
            var cotacoes = new List<Cotacao>();
            foreach (var line in data.Split("\n").Skip(1).SkipLast(2))
            {
                cotacoes.Add(new Cotacao(line));
            }

            return cotacoes;
        }

        public static string DateB3Format(DateTime date)
        {
            return date.ToString("ddMMyyyy");
        }

        public static string DateToFileName(DateTime date)
        {
            return date.ToString("yyyy");
        }

        private static List<Cotacao> ReadFromBlob(BlobContainerClient containerClient, string fileName)
        {
            // Get a reference to a blob
            BlobClient blobClient = containerClient.GetBlobClient(fileName);
            if (blobClient.Exists())
            {
                BlobDownloadInfo download = blobClient.DownloadAsync().Result;
                using (MemoryStream downloadStream = new MemoryStream())
                {
                    download.Content.CopyToAsync(downloadStream).Wait();
                    downloadStream.Position = 0;
                    return new CsvReader(new StreamReader(downloadStream), new CsvConfiguration(CultureInfo.InvariantCulture) { TrimOptions = TrimOptions.Trim }).GetRecords<Cotacao>().ToList();
                }
            }
            return new List<Cotacao>();
        }

        private static List<string> ListBlobsFlatListing(BlobContainerClient container, string folder, int? segmentSize = null)
        {
            string continuationToken = null;
            var listaNomes = new List<string>();
            try
            {
                do
                {
                    var resultSegment = container.GetBlobs(prefix: folder)
                        .AsPages(continuationToken, segmentSize);

                    foreach (Page<BlobItem> blobPage in resultSegment)
                    {
                        foreach (BlobItem blobItem in blobPage.Values)
                        {
                            listaNomes.Add(blobItem.Name);
                        }
                        continuationToken = blobPage.ContinuationToken;
                    }

                } while (continuationToken != "");

            }
            catch (RequestFailedException e)
            {
                Console.WriteLine(e.Message);
                throw;
            }
            return listaNomes;
        }

        private static void WriteToBlob(BlobContainerClient container, List<Cotacao> cotacoes)
        {
            // Get a reference to a blob
            var fileName = cotacoes.FirstOrDefault()?.Ano;
            if (fileName == null)
            {
                return;
            }
            BlobClient blobClient = container.GetBlobClient("b3/" + fileName + ".csv");

            log.LogInformation($"Serão escritas {cotacoes.Count} linhas no arquivo.");

            using (var stream = new StringWriter())
            {
                using (var csv = new CsvWriter(stream, CultureInfo.InvariantCulture))
                {
                    log.LogInformation($"Transformando dados em CSV.");
                    csv.WriteRecords(cotacoes);
                    log.LogInformation($"Dados no formato CSV.");
                }
                log.LogInformation($"Escrevendo CSV no storage.");

                using (MemoryStream downloadStream = new MemoryStream())
                {
                    downloadStream.Write(Encoding.ASCII.GetBytes(stream.ToString()));
                    downloadStream.Position = 0;

                    blobClient.UploadAsync(downloadStream, true).Wait();
                }
                log.LogInformation($"Terminou de escrever CSV no storage.");
            }
        }

        private static List<Cotacao> BaixarDadoB3(DateTime data)
        {
            var url = baseUrl.Replace("%date%", DateB3Format(data));
            log.LogInformation($"Requisitando arquivo da B3 em: {url}");
            var b3Response = http.GetAsync(url).Result;
            log.LogInformation($"B3 respondeu com código '{b3Response.StatusCode}'");

            if (b3Response.IsSuccessStatusCode)
            {
                var content = b3Response.Content.ReadAsByteArrayAsync().Result;
                var fileAsText = ZipHelper.Unzip(content);
                return TxtToCotacao(fileAsText);
            }
            return new List<Cotacao>();
        }
    }
}