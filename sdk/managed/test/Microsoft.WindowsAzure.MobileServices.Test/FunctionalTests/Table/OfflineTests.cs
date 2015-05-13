using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.MobileServices.SQLiteStore;
using Microsoft.WindowsAzure.MobileServices.TestFramework;
using Microsoft.WindowsAzure.MobileServices.Sync;
using System.Threading;

namespace Microsoft.WindowsAzure.MobileServices.Test.FunctionalTests.Table
{
    [Tag("Offline")]
    public class OfflineTests : FunctionalTestBase
    {
        [AsyncTestMethod]
        public async Task SimplePull()
        {
            var client = await this.InitializeClient();
            var table = client.GetSyncTable<Movie>();
            await table.PurgeAsync();
            var results = await table.ReadAsync();
            var query = table.CreateQuery();
            
            await table.PullAsync(null, query);
            results = await table.ReadAsync();        
        }


        [AsyncTestMethod]
        public async Task SimpleIncrementalPull()
        {
            var client = await this.InitializeClient();
            var table = client.GetSyncTable<Movie>();
            await table.PurgeAsync("movies", null, true, CancellationToken.None);
            var results = await table.ReadAsync();
            var query = table.CreateQuery().OrderBy(m => m.Duration);           
            await table.PullAsync("movies", query);
            results = await table.ReadAsync();
        }

        private async Task<MobileServiceClient> InitializeClient()
        {
            var store = new MobileServiceSQLiteStore("localsync12.db");
            store.DefineTable<Movie>();
            var client = this.GetClient();            
            await client.SyncContext.InitializeAsync(store);
            return client;
        }
    }
}
