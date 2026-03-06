using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Blinq.Tests
{
	[Trait("Category", "Integration")]
	[Collection("Integration")]
	public class BlobItemQueryTests(QueryTestsFixture fixture)
	{
		private readonly QueryTestsFixture _fixture = fixture;

		[Fact]
		public async Task CanQueryBlobItemOnly()
		{
			Assert.SkipUnless(_fixture.IsConfigured, "AZURE_STORAGE_BLOB_URI not configured. Set in user secrets or environment variables.");
			var containerClient = _fixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var results = await (
				from x in containerClient.AsBlobItemQueryable()
				where x.Properties.ContentType == "application/json"
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			Assert.NotEmpty(results);
			foreach (var result in results)
			{
				Assert.NotNull(result); // BlobItem should be present
				Debug.WriteLine($"{result.Name}: {result.Properties.ContentType}");
			}
		}
	}
}
