//using StackExchange.Redis;
using Valkey.Glide;
using static Valkey.Glide.ConnectionConfiguration;

//  ping -4 metl00599

namespace ValkeyTest
{
    public class Program
    {

        public static async Task Main(string[] args)
        {
            var manager = new ValkeyManager();
            await manager.Test_Basics();
            //await manager.TestPublishSubscribe();
            await manager.TestJson();
        }

        
    }
}
