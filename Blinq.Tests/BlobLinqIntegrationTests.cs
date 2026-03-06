using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Xunit;
using Blinq;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Blinq.Tests
{
	[Trait("Category", "Integration")]
	[Collection("Integration")]
	public class BlobLinqIntegrationTests(QueryTestsFixture fixture)
	{
		private readonly QueryTestsFixture _fixture = fixture;

		private const string SkipReason = "AZURE_STORAGE_BLOB_URI not configured. Set in user secrets or environment variables.";

		[Fact]
		public async Task CanQueryAllJsonBlobs_ByContentType()
		{
			Assert.SkipUnless(_fixture.IsConfigured, SkipReason);
			var _containerClient = _fixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var results = await (
			from x in _containerClient.AsQueryable<string>(deserializer: StringBlobDeserializer.Default)
			where x.Metadata.Properties.ContentType == "application/json"
			select x
			).ToListAsync();

			Assert.True(results.Count >=100);
			Assert.All(results, r => Assert.EndsWith(".json", r.BlobName));
		}

		[Fact]
		public async Task CanFilterJsonBlobs_ByActiveField()
		{
			Assert.SkipUnless(_fixture.IsConfigured, SkipReason);
			var _containerClient = _fixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var results = await (
			from x in _containerClient.AsQueryable<string>(deserializer: StringBlobDeserializer.Default)
			where x.Metadata.Properties.ContentType == "application/json"
			select x
			).ToListAsync();

			var activeCount = results.Count(r => r.Content?.Contains("\"active\": true") ?? false);
			var inactiveCount = results.Count(r => r.Content?.Contains("\"active\": false") ?? false);
			Assert.True(activeCount >0);
			Assert.True(inactiveCount >0);
		}

		[Fact]
		public async Task CanPaginateJsonBlobs()
		{
			Assert.SkipUnless(_fixture.IsConfigured, SkipReason);
			var _containerClient = _fixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var results = await (
			from x in _containerClient.AsQueryable<string>(deserializer: StringBlobDeserializer.Default)
			where x.Metadata.Properties.ContentType == "application/json"
			select x
			).TakeAsync(10);

			Assert.Equal(10, results.Count);
		}
	}
}
