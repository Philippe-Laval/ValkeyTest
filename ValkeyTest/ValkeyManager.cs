using System;
using System.Collections.Generic;
using System.Text;
using Valkey.Glide;
using Valkey.Glide.Pipeline;
using Valkey.Glide.ServerModules;
using static Valkey.Glide.ConnectionConfiguration;
using static Valkey.Glide.Errors;
using static Valkey.Glide.Pipeline.Options;

namespace ValkeyTest
{
    public class ValkeyManager
    {
        private static readonly string ConnectionString = "localhost:6379"; // Modifiez avec l'IP/Port de votre instance Valkey
        private static readonly string ChannelName = "mon_canal_notifications";

        public async Task Test_Basics()
        {
            Logger.SetLoggerConfig(Level.Debug);

            // Create client configuration
            var config = new StandaloneClientConfigurationBuilder()
                .WithAddress("localhost", 6379)
                .WithRequestTimeout(TimeSpan.FromSeconds(5))
                .WithConnectionTimeout(TimeSpan.FromSeconds(10))
                .WithDatabaseId(0)
                .Build();

            await using var client = await GlideClient.CreateClient(config);

            try
            {
                // Test the connection
                var result1 = await client.PingAsync();

                // Valkey responds with PONG
                Console.WriteLine($"Connected! Server responded: {result1}");

                // Set a key-value pair
                await client.SetAsync("user:1000", "john_doe");
                Console.WriteLine("✓ Key set successfully");

                // Get the value
                ValkeyValue value = await client.GetAsync("user:1000");
                Console.WriteLine($"✓ Retrieved value: {value}");


                // Set multiple keys at once
                await client.SetAsync(new KeyValuePair<ValkeyKey, ValkeyValue>[]
                {
                    new("user:1001", "alice"),
                    new("user:1002", "bob"),
                    new("user:1003", "charlie"),
                });
                Console.WriteLine("✓ Multiple keys set");

                // Get multiple keys at once
                var values = await client.GetAsync(["user:1001", "user:1002", "user:1003"]);
                Console.WriteLine($"✓ Retrieved values: {string.Join(", ", values)}");

                var count = await client.DeleteAsync(["user:1001", "user:1002", "user:1003"]);
                Console.WriteLine($"✓ Deleted {count} keys");

                // Create a simple script
                using var script1 = new Script("return 'Hello, Valkey!'");

                // Execute the script
                var result2 = await client.ScriptInvokeAsync(script1);
                Console.WriteLine(result2); // Hello, Valkey!


                var productKey = "product:shoe:stock";
                var buyQuantity = "3";

                using var script = new Script("return redis.call('SET', KEYS[1], ARGV[1])");

                var options = new ScriptOptions()
                  .WithKeys(productKey)    // Maps to KEYS[1] in Lua
                  .WithArgs(buyQuantity);  // Maps to ARGV[1] in Lua

                var result3 = await client.ScriptInvokeAsync(script, options);
                Console.WriteLine(result3);

            }
            catch (Exception e)
            {
                Console.WriteLine($"Connection failed: {e.Message}");
            }
        }


        public async Task TestPublishSubscribe()
        {
            // 1. Connexion à Valkey
            ConnectionMultiplexer valkey = await ConnectionMultiplexer.ConnectAsync(ConnectionString);
            ISubscriber subscriber = valkey.GetSubscriber();

            Console.WriteLine("=== Démarrage du Pub/Sub Valkey ===");

            // 2. ABONNEMENT (Subscriber)
            // Cette méthode s'exécute de manière asynchrone à chaque fois qu'un message arrive
            await subscriber.SubscribeAsync(ValkeyChannel.Literal(ChannelName), (channel, message) =>
            {
                Console.WriteLine($"[Abonné] Message reçu sur {channel} : {message}");
            });
            Console.WriteLine($"Abonné écoutant sur le canal : '{ChannelName}'");

            // 3. PUBLICATION (Publisher)
            IDatabase db = valkey.GetDatabase();

            Console.WriteLine("\n[Éditeur] Envoi de messages de test (Appuyez sur une touche pour quitter)...");

            for (int i = 1; i <= 3; i++)
            {
                string messageToSend = $"Notification Alerte #{i} générée à {DateTime.Now.ToLongTimeString()}";

                // Publie le message et retourne le nombre d'abonnés l'ayant reçu
                long recipients = await subscriber.PublishAsync(ValkeyChannel.Literal(ChannelName), messageToSend);

                Console.WriteLine($"[Éditeur] Message #{i} envoyé à {recipients} abonné(s).");
                await Task.Delay(1500); // Pause de 1,5 seconde
            }

            Console.ReadKey();

            // Nettoyage : Se désabonner proprement avant de quitter
            await subscriber.UnsubscribeAsync(ValkeyChannel.Literal(ChannelName));
        }

        public void Test3()
        {
            // Create an atomic batch (transaction)
            var transaction = new Batch(true);
            transaction.SetAsync("key", "value");

            // Create a non-atomic batch (pipeline)
            var pipeline = new Batch(false);
            pipeline.SetAsync("key", "value");
        }

        public async Task Test4()
        {
            var config = new StandaloneClientConfigurationBuilder().Build();
            await using var client = await GlideClient.CreateClient(config);

            var options = new BatchOptions(timeout: 2000); // 2 seconds

            var batch = new Batch(true)
                  .SetAsync("key", "hello")
                  .ListLeftPop("key")
                  .Delete("key")
                  .Rename("key", "key2");

            try
            {
                await client.Exec(batch, raiseOnError: true);
            }
            catch (RequestException e)
            {
                Console.WriteLine($"Batch execution aborted: {e.Message}");
            }
        }

        public void Test5()
        {
            var config = new StandaloneClientConfigurationBuilder().Build();
            using var client = GlideClient.CreateClient(config).Result;
            var options = new BatchOptions(timeout: 2000); // 2 seconds
            var batch = new Batch(true)
                  .SetAsync("key", "hello")
                  .ListLeftPop("key")
                  .Delete("key")
                  .Rename("key", "key2");
            try
            {
                client.Exec(batch, raiseOnError: true).Wait();
            }
            catch (AggregateException ae)
            {
                foreach (var e in ae.InnerExceptions)
                {
                    if (e is RequestException re)
                    {
                        Console.WriteLine($"Batch execution aborted: {re.Message}");
                    }
                    else
                    {
                        Console.WriteLine($"Unexpected error: {e.Message}");
                    }
                }
            }
        }

        public async Task Test6()
        {
            // Create a cache configuration
            var cache = new ClientSideCacheConfig(maxCacheKb: 1024, entryTtl: TimeSpan.FromSeconds(60))
                .WithEvictionPolicy(EvictionPolicy.LRU)   // LRU or LFU
                .WithMetrics(true);                       // Enable hit/miss tracking

            // Standalone client
            var config = new StandaloneClientConfigurationBuilder()
                .WithAddress("localhost", 6379)
                .WithClientSideCache(cache)
                .Build();
            var client = await GlideClient.CreateClient(config);


            // Get metrics
            double hitRate = await client.GetCacheHitRateAsync();       // 0.0–1.0
            double missRate = await client.GetCacheMissRateAsync();     // 0.0–1.0
            long entryCount = await client.GetCacheEntryCountAsync();
            long evictions = await client.GetCacheEvictionsAsync();
            long expirations = await client.GetCacheExpirationsAsync();
            long total = await client.GetCacheTotalLookupsAsync();

            Console.WriteLine($"Hit rate: {hitRate:P2}");
            Console.WriteLine($"Entries: {entryCount}, Evictions: {evictions}, Expirations: {expirations}");
        }


        public async Task TestJson()
        {
            // Both standalone and cluster mode are supported
            var config = new StandaloneClientConfigurationBuilder()
                  .WithAddress("localhost", 6379)
                  .WithRequestTimeout(TimeSpan.FromSeconds(5))
                  .WithConnectionTimeout(TimeSpan.FromSeconds(10))
                  .WithDatabaseId(0)
                  .Build();

            await using var client = await GlideClient.CreateClient(config);

            var key = "testKey";
            var path = "$";
            var jsonValue = """{"a": 1.0, "b": 2}""";

            // JSON.SET
            await GlideJson.SetAsync(client, key, path, jsonValue);

            // JSON.GET
            var response = await GlideJson.GetAsync(client, key);
            Console.WriteLine($"Output: {response}");  // Output: {"a":1.0,"b":2}

        }

        public async Task TestJson2()
        {
            // Both standalone and cluster mode are supported
            var config = new ConnectionConfiguration.StandaloneClientConfigurationBuilder()
                .WithAddress("localhost", 6379)
                .Build();
            await using var client = await GlideClient.CreateClient(config);

            var prefix = "{hash}:";
            var index = prefix + "index";

            // FT.CREATE a Hash index with a vector field
            var fields = new Ft.CreateField[]
            {
                new Ft.CreateVectorFieldHnsw
                {
                    Identifier = "vec",
                    Alias = "VEC",
                    Dimensions = 2,
                    DistanceMetric = Ft.DistanceMetric.Euclidean,
                },
            };

            var createOptions = new Ft.CreateOptions
            {
                DataType = Ft.DataType.Hash,
                Prefixes = [prefix],
            };

            await Ft.CreateAsync(client, index, fields, createOptions);

            // Insert hash data with vector fields (2-dimensional float32 vectors = 8 bytes each)
            var vec0 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
            var vec1 = new byte[] { 0, 0, 0, 0, 0, 0, 0x80, 0xBF };
            await client.HashSetAsync($"{prefix}0", "vec", vec0);
            await client.HashSetAsync($"{prefix}1", "vec", vec1);

            // Let server digest the data and update index
            await Task.Delay(TimeSpan.FromSeconds(1));

            // FT.SEARCH
            var query = "*=>[KNN 2 @VEC $query_vec]";
            var searchOptions = new Ft.SearchOptions
            {
                Params = new Dictionary<ValkeyValue, ValkeyValue>
                {
                    ["query_vec"] = vec0,
                },
            };
            var result = await Ft.SearchAsync(client, index, query, searchOptions);
            var vectors = result.Documents.Select(d => Convert.ToHexString((byte[])d.Fields["vec"]));

            Console.WriteLine($"Total results: {result.TotalResults}");     // Total results: 2
            Console.WriteLine($"Vectors: [{string.Join(", ", vectors)}]");  // Vectors: [0000000000000000, 00000000000080BF]

        }
    }
}
