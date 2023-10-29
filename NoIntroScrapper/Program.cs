using Newtonsoft.Json;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace NoIntroScraper
{
    class Program
    {
        private static readonly HttpClient httpClient = new HttpClient() { Timeout = TimeSpan.FromMilliseconds(MAX_TIMEOUT_IN_MS) };
        private const int MAX_GAMES_TO_POLL = 50;
        private const int MAX_TIMEOUT_IN_MS = 20000;
        private const string URL_TEMPLATE = "https://datomatic.no-intro.org/index.php?page=show_record&s={0}&n={1:D4}";
        private static string TEMPORARY_FILENAME_TEMPLATE = "{0}-Temp-{1}.json";
        private const string FINAL_FILENAME_TEMPLATE = "{0}-Final.json";
        private const string STATUS_FILENAME_TEMPLATE = "{0}-Status.log";

        private static readonly HashSet<GameStatus> RetryableErrors = new HashSet<GameStatus>
        {
            GameStatus.Timeout
        };

        private static readonly HashSet<GameStatus> NonRetryableErrors = new HashSet<GameStatus>
        {
           GameStatus.Success,
           GameStatus.NoTrustedDumpTable
        };

        public enum GameStatus
        {
            Success,
            Timeout,
            NoTrustedDumpTable
        }

        private static async Task<(string Content, double Duration)> FetchGameSite(int systemId, string system, int gameNumber)
        {
            string url = string.Format(URL_TEMPLATE, system, gameNumber);
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var content = await httpClient.GetStringAsync(url);

                if (content.Contains("To remove the ban"))
                {
                    Console.WriteLine("!!! Ban Detected !!! Ban Detected !!! Ban Detected !!! ");
                    return (null, 0);
                }
                stopwatch.Stop();
                return (content, stopwatch.Elapsed.TotalSeconds);
            }
            catch (TaskCanceledException)
            {
                LogStatus(system, systemId, gameNumber, GameStatus.Timeout);
                return (null, MAX_TIMEOUT_IN_MS);
            }
        }
        private static Dictionary<string, object>? ExtractGameInfo(string htmlContent, string system, int systemId, int gameId, double duration)
        {
            var parsedRomMetadata = new Dictionary<string, object>();
            var doc = new HtmlAgilityPack.HtmlDocument();
            doc.LoadHtml(htmlContent);
            var trustedDumpTable = doc.DocumentNode.SelectSingleNode("//table[@class='RecordTable' and contains(., 'Trusted Dump')]");

            if (trustedDumpTable == null)
            {
                LogStatus(system, systemId, gameId, GameStatus.NoTrustedDumpTable, duration);
                return null;
            }

            parsedRomMetadata["SystemId"] = systemId;
            parsedRomMetadata["GameId"] = gameId;

            foreach (var tr in trustedDumpTable.Descendants("tr").Where(tr => tr.Descendants("td").Count() == 3))
            {
                var key = tr.Descendants("td").ElementAt(1).InnerText.Trim().Replace(":", string.Empty);
                var value = tr.Descendants("td").ElementAt(2).InnerText.Trim();
                AddToDictionary(parsedRomMetadata, key, value);
            }

            // Get the <div> directly after the trustedDumpTable
            var additionalInfoDiv = trustedDumpTable.SelectSingleNode("following-sibling::div[1]");

            // Extract data from this div
            if (additionalInfoDiv != null)
            {
                foreach (var tr in additionalInfoDiv.Descendants("tr"))
                {
                    var keyNode = tr.Descendants("td").ElementAtOrDefault(1);
                    var valueNode = tr.Descendants("td").ElementAtOrDefault(2);

                    if (keyNode != null && valueNode != null)
                    {
                        var key = keyNode.InnerText.Trim().Replace(":", string.Empty);
                        var value = valueNode.InnerText.Trim();
                        AddToDictionary(parsedRomMetadata, key, value);
                    }
                }
            }

            return parsedRomMetadata;
        }
        private static (Dictionary<int, GameStatus>, int) LoadData(string consoleName)
        {
            var gameStatuses = new Dictionary<int, GameStatus>();
            int highestGameIdProcessed = 0;

            string statusFileName = string.Format(STATUS_FILENAME_TEMPLATE, consoleName);
            if (File.Exists(statusFileName))
            {
                var lines = File.ReadAllLines(statusFileName);
                foreach (var line in lines)
                {
                    var statusData = JsonConvert.DeserializeObject<dynamic>(line);
                    if (statusData != null && statusData.GameId != null)
                    {
                        int gameId = (int)statusData.GameId;
                        if (gameId > highestGameIdProcessed)
                        {
                            highestGameIdProcessed = gameId;
                        }
                        GameStatus gameStatus = Enum.Parse<GameStatus>(statusData.Status.ToString());
                        gameStatuses[gameId] = gameStatus;
                    }
                }
            }

            return (gameStatuses, highestGameIdProcessed);
        }
        private static void LogStatus(string system, int systemId, int gameId, GameStatus status, double duration = 0)
        {
            string statusFileName = string.Format(STATUS_FILENAME_TEMPLATE, system);
            var newStatusData = JsonConvert.SerializeObject(new
            {
                SystemId = systemId,
                System = system,
                GameId = gameId,
                Status = status.ToString(),
                UpdatedOn = DateTime.UtcNow,
                Duration = duration
            });

            // Log the new status
            using (StreamWriter writer = new StreamWriter(statusFileName, append: true))
            {
                writer.WriteLine(newStatusData);
            }
            Console.WriteLine(newStatusData);
        }
        private static async Task<int> GetCrawlDelay()
        {
            try
            {
                string robotsTxt = await httpClient.GetStringAsync("https://datomatic.no-intro.org/robots.txt");
                var match = Regex.Match(robotsTxt, @"Crawl-delay:\s*(\d+)");
                if (match.Success)
                {
                    return int.Parse(match.Groups[1].Value) * 1000/* Convert to milliseconds*/  * 2 /*already got banned ban */;
                }
            }
            catch
            {
                // Error fetching or parsing robots.txt, so we'll just default to 5000 ms
            }
            return 5000; // Default value
        }
        private static async Task Main(string[] args)
        {
            var delay = await GetCrawlDelay();
            var consoles = ReadConsolesFromFile();
            ConsolidateLocalMachineIntoFinalFiles(consoles);

            var shuffledConsoleNames = consoles.Keys.ToList();
            Shuffle(shuffledConsoleNames);

            foreach (string system in shuffledConsoleNames)
            {
                int systemId = consoles[system];

                var (gameStatuses, lastProcessedGameId) = LoadData(system); // Adjust as needed
                string fileName = string.Format(TEMPORARY_FILENAME_TEMPLATE, system, Environment.MachineName);

                List<int> gamesToProcess = new List<int>();
                gamesToProcess.AddRange(gameStatuses.Where(kv => kv.Value == GameStatus.Timeout).Select(kv => kv.Key));
                int newGamesCount = MAX_GAMES_TO_POLL - gamesToProcess.Count;
                for (int i = 1; i <= newGamesCount; i++)
                {
                    gamesToProcess.Add(lastProcessedGameId + i);
                }

                using (StreamWriter writer = new StreamWriter(fileName, append: true))
                {
                    foreach (int gameId in gamesToProcess)
                    {
                        if (gameStatuses.ContainsKey(gameId) && NonRetryableErrors.Contains(gameStatuses[gameId]))
                        {
                            var logData = JsonConvert.SerializeObject(new { ConsoleName = system, ConsoleId = systemId, GameId = gameId, Status = gameStatuses[gameId].ToString() });
                            Console.WriteLine($"{logData} -- SKIPPING");
                            continue;
                        }

                        (string htmlDoc, double duration) = await FetchGameSite(systemId, system, gameId); // Adjust as needed
                        if (!string.IsNullOrEmpty(htmlDoc))
                        {
                            var game = ExtractGameInfo(htmlDoc, system, systemId, gameId, duration); // Adjust as needed
                            if (game != null)
                            {
                                game["Duration"] = Math.Round(duration, 1);
                                game["ConsoleName"] = system;
                                writer.WriteLine($"{JsonConvert.SerializeObject(game)}");
                                LogStatus(system, systemId, gameId, GameStatus.Success, duration); // Adjust as needed
                                writer.Flush();
                            }
                        }
                        await Task.Delay(delay);
                    }
                }
                Console.WriteLine($"File {fileName} has been updated.");
            }
        }
        private static Dictionary<string, int> ReadConsolesFromFile()
        {
            var consoles = new Dictionary<string, int>();
            var lines = File.ReadAllLines("ConsolesToDownload.txt");

            foreach (var line in lines)
            {
                var parts = line.Split('=');

                if (parts.Length == 2)
                {
                    // Trim whitespace and remove commas
                    var consoleName = parts[0].Trim();
                    var numberPart = parts[1].Trim().Replace(",", "");

                    if (int.TryParse(numberPart, out var number))
                    {
                        consoles[consoleName] = number;
                    }
                }
            }

            return consoles;
        }
        public static void Shuffle<T>(IList<T> list)
        {
            Random rng = new Random();
            int n = list.Count;
            while (n > 1)
            {
                n--;
                int k = rng.Next(n + 1);
                T value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }
        private static void ConsolidateLocalMachineIntoFinalFiles(Dictionary<string, int> consoles)
        {
            Console.WriteLine($"Consolidating all local files into final versions. This may take a moment.");

            foreach (var consoleEntry in consoles)
            {
                string consoleName = consoleEntry.Key;
                string tempFilePattern = string.Format(TEMPORARY_FILENAME_TEMPLATE, consoleName, "*");
                string finalFileName = string.Format(FINAL_FILENAME_TEMPLATE, consoleName);

                List<Dictionary<string, object>> finalData = new List<Dictionary<string, object>>();

                // Load existing data from the final file
                if (File.Exists(finalFileName))
                {
                    var existingData = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(File.ReadAllText(finalFileName));
                    finalData.AddRange(existingData);
                }

                foreach (string tempFileName in Directory.GetFiles(".", tempFilePattern))
                {
                    // Read the file line by line and deserialize each line
                    foreach (var line in File.ReadLines(tempFileName))
                    {
                        var game = JsonConvert.DeserializeObject<Dictionary<string, object>>(line);

                        // Check if the game already exists in the final data
                        var existingGame = finalData?.FirstOrDefault(g => g["ConsoleName"].Equals(consoleName) && g["GameId"].Equals(game["GameId"]));

                        if (existingGame != null)
                        {
                            // Update the game data
                            finalData.Remove(existingGame);
                        }

                        finalData.Add(game);
                    }

                    // Optionally, delete the temp file after processing
                    // File.Delete(tempFileName);
                }

                // Save consolidated data back to the final file
                if (finalData.Count > 0)
                {
                    File.WriteAllText(finalFileName, JsonConvert.SerializeObject(finalData, Formatting.Indented));
                }
            }
        }
        private static string CleanKey(string key)
        {
            key = HtmlAgilityPack.HtmlEntity.DeEntitize(key);
            key = key.Trim().Replace(":", string.Empty);
            key = key.Replace("&nbsp;", " "); // You can add more replacements if needed
            return key;
        }
        private static string Clean(string value)
        {
            return CleanKey(HtmlAgilityPack.HtmlEntity.DeEntitize(value.Trim()));
        }
        private static void AddToDictionary(Dictionary<string, object> dict, string key, string value)
        {
            if (dict.ContainsKey(key))
            {
                int counter = 1;
                while (dict.ContainsKey(key + "_" + counter))
                    counter++;
                key += "_" + counter;
            }

            dict[Clean(key)] = Clean(value);
        }
    }
}
