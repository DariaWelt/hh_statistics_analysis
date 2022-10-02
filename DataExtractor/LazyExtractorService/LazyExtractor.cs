using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HHVacansisETL {
  public class LazyExtractorConfig {
    public string stateDataPath { get; set; } = "extractor_state";
    public string elasticSearchURI {get; set;} = "http://localhost:9200";
    public string databaseName {get; set;} = "hh_vacations";
    public string sourceUrl {get; set;} = "https://api.hh.ru";
    public uint workers {get; set; } = 1;
  };

  public class HHLazyExtractor : IHostedService, IDisposable {
    private readonly ILogger _logger;
    private readonly IOptions<LazyExtractorConfig> _config;
    //private readonly IMongoDatabase _db;
    
    private static HHLazyExtractor _instance = null;
    private static readonly object _padlock = new object();
    private static uint _currentIdToRead = 0;

    public HHLazyExtractor(ILogger<HHLazyExtractor> logger, IOptions<LazyExtractorConfig> config) {
      _logger = logger;
      _config = config;

      //var mongoClient = new MongoClient(_config.Value.elasticSearchURI);
      //_db = mongoClient.GetDatabase(_config.Value.databaseName);
      
    }

    private IEnumerable<List<List<uint>>> GetIdBatches() {
      const uint batchSize = 10;
      var max = UInt32.MaxValue;
      max = 40;
      while (_currentIdToRead < max) {
        uint startIndex = _currentIdToRead;
        List<List<uint>> batches = new List<List<uint>>();
        for (uint i = 0; i < _config.Value.workers; ++i) {
          List<uint> batch = new List<uint>();
          uint j = 0;
          while(_currentIdToRead < max && j < batchSize) {
            batch.Add(_currentIdToRead);
            ++_currentIdToRead;
            ++j;
          }
          batches.Add(batch);
        }
        _logger.LogInformation(String.Format("prepared to process ids from {0} to {1}", startIndex, _currentIdToRead-1));
        yield return batches;
      }
    }

    private async void ExtractVacation(uint vacationId) {
      try {
        string responseJson = string.Empty;
        string url = String.Format("{0}/vacancies/{1}", _config.Value.sourceUrl, vacationId);
        
        using (HttpClient client = new HttpClient()) {
          client.DefaultRequestHeaders.Add("User-Agent", ".net core");

          var response = await client.GetAsync(url);
          if (!response.IsSuccessStatusCode) {
            throw new Exception("No vac with such id.");
          }
          responseJson = await response.Content.ReadAsStringAsync();
        }
        Console.Write(responseJson);
        //var products = new List<BsonDocument>()

        _logger.LogInformation("extracted vacation with id = " + vacationId);
      }
      catch (Exception ex) {
        _logger.LogInformation(String.Format("vacation with id = {0} was not loaded: {1}", vacationId, ex.Message));
      }
    }

    public Task StartAsync(CancellationToken cancellationToken) {
      var stateDataPath = _config.Value.stateDataPath;
      if (File.Exists(stateDataPath)) {
        string readText = File.ReadAllText(stateDataPath);
        bool isParsable = UInt32.TryParse(readText, out _currentIdToRead);
        if (!isParsable) {
          _currentIdToRead = 0;
          _logger.LogInformation("Can not parse config file. Extractor will start iterating by id from the beginning");
        }
      }
      _logger.LogInformation("Started hh vacansies extraction from id = " + _currentIdToRead);
      foreach (var idBatches in GetIdBatches()) {
        Parallel.ForEach(idBatches, batch => {
          foreach (var id in batch) {
            ExtractVacation(id);
          }
        });
      }
      File.WriteAllText(stateDataPath, _currentIdToRead.ToString());
      _logger.LogInformation("Loaded all vacations with id in range 0 - " + UInt32.MaxValue);
      return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) {
      var stateDataPath = _config.Value.stateDataPath;
      File.WriteAllText(stateDataPath, _currentIdToRead.ToString());
      _logger.LogInformation("Stopping lazy extractor service. Last readed id = " + (_currentIdToRead - 1));
      return Task.CompletedTask;
    }

    public void Dispose() {
      _logger.LogInformation("Disposing....");
    }
  };
}