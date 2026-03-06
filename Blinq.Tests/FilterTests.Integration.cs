using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

namespace Blinq.Tests
{
	[CollectionDefinition("Integration")]
	public class IntegrationTestCollection : ICollectionFixture<QueryTestsFixture>
	{
	}

	[Trait("Category", "Integration")]
	[Collection("Integration")]
	public class FilterTests(QueryTestsFixture queryTestsFixture)
	{
		private readonly QueryTestsFixture _queryTestsFixture = queryTestsFixture;

		private const string SkipReason = "AZURE_STORAGE_BLOB_URI not configured. Set in user secrets or environment variables.";

		[Fact]
		public async Task SimpleQuery_AsyncEnumerated()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var results =
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.Metadata.Properties.ContentType == "application/json"
				select x
			);

			Assert.NotEmpty(results);

			await foreach (var result in results.AsAsyncEnumerable())
			{
				System.Diagnostics.Trace.WriteLine($"{result.BlobName}: {result.Content?.Id}/{result.Content?.Name}");
				Console.WriteLine($"{result.BlobName}: {result.Content?.Id}/{result.Content?.Name}");
			}
		}

		[Fact]
		public async Task SimpleQuery()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var sw = new Stopwatch();
			sw.Start();

			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName == "TestDocument1.json"
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			sw.Stop();
			Console.WriteLine(sw.Elapsed);
			foreach (var result in results)
			{
				System.Diagnostics.Trace.WriteLine($"{result.BlobName}: {result.Content?.Id}/{result.Content?.Name}");
				Console.WriteLine($"{result.BlobName}: {result.Content?.Id}/{result.Content?.Name}");
			}
			Assert.Single(results);
			Assert.Contains(results, (b) => b.Content?.Name == "Test Document 1");
		}

		[Fact]
		public async Task CompoundFilterQuery()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var sw = new Stopwatch();
			sw.Start();

			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName == "TestDocument1.json"
						&& x.Content != null && x.Content.Name == "Test Document 1"
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			sw.Stop();
			Console.WriteLine(sw.Elapsed);
			foreach (var result in results)
			{
				System.Diagnostics.Trace.WriteLine($"{result.BlobName}: {result.Content?.Id}/{result.Content?.Name}");
				Console.WriteLine($"{result.BlobName}: {result.Content?.Id}/{result.Content?.Name}");
			}
			Assert.Single(results);
			Assert.Equal("Test Document 1", results.First().Content?.Name);
		}
		
		[Fact]
		public async Task QueryBySize()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			// Requires sample004-nz001.json (1109 bytes) to be uploaded via Upload-TestBlobs.ps1
			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.Metadata.Properties.ContentLength >= 1000
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			Assert.SkipWhen(results.Count == 0, "No blobs >= 1000 bytes found. Re-run Upload-TestBlobs.ps1 to upload sample004-nz001.json.");
			Assert.Contains(results, r => r.Content?.Name == "NZ001");
		}


		[Fact]
		public async Task Filters_ByPrefix()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var sw = new Stopwatch();
			sw.Start();

			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName.StartsWith("TestDocument")
						&& x.Content != null && x.Content.Name == "Test Document 2"
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			sw.Stop();
			Console.WriteLine(sw.Elapsed);
			foreach (var result in results)
			{
				System.Diagnostics.Trace.WriteLine($"{result.Content?.Id}: {result.Content?.Name}");
				Console.WriteLine($"{result.Content?.Id}: {result.Content?.Name}");
			}
			Assert.Single(results);
			Assert.Equal("Test Document 2", results.First().Content?.Name);
		}

		[Fact]
		public async Task Filters_ByPrefix_AndBlobNameExpressions()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var sw = new Stopwatch();
			sw.Start();

			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName.StartsWith("TestDocument")
					 && x.BlobName.EndsWith(".json")
						&& x.Content != null && x.Content.Name == "Test Document 1"
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			sw.Stop();
			Console.WriteLine(sw.Elapsed);
			foreach (var result in results)
			{
				System.Diagnostics.Trace.WriteLine($"{result.Content?.Id}: {result.Content?.Name}");
				Console.WriteLine($"{result.Content?.Id}: {result.Content?.Name}");
			}

			Assert.Single(results);
			Assert.Equal("Test Document 1", results.First().Content?.Name);
		}

		[Fact]
		public void NonFilters_Any()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var query =
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName.StartsWith("TestDocument")
					&& x.BlobName.EndsWith(".json")
					&& x.Content != null && x.Content.Name != null
				select x
			);

			Assert.True(query.Any());
		}

		[Fact]
		public void NonFilters_Single()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var query =
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName.StartsWith("TestDocument")
					&& x.BlobName.EndsWith(".json")
					&& x.Content != null && x.Content.Name == "Test Document 2"
				select x
			);

			Assert.NotNull(query.Single());
		}

		[Fact]
		public async Task NonFilters_AnyAsync()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var result = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName.StartsWith("TestDocument")
					&& x.BlobName.EndsWith(".json")
					&& x.Content != null && x.Content.Name == "Test Document 1"
				select x
			).AnyAsync(TestContext.Current.CancellationToken);

			Assert.True(result);
		}
		[Fact]
		public async Task NonFilters_SkipThenTakeAync()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var result = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName.StartsWith("TestDocument")
				select x
			).Skip(1).TakeAsync(1, TestContext.Current.CancellationToken);

			Assert.NotNull(result);
			Assert.Equal("Test Document 1", result.First().Content?.Name);
		}

		[Fact]
		public void NonFilters_First()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var query =
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName.StartsWith("TestDocument")
					&& x.BlobName.EndsWith(".json")
					&& x.Content != null && x.Content.Name == "Test Document 1"
				select x
			);

			Assert.NotNull(query.First());
		}

		[Fact]
		public void NonFilters_TakeOne()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var query =
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.BlobName.StartsWith("TestDocument")
				select x
			);

			Assert.Equal(1, query.Take(1).Count());
		}

		[Fact]
		public async Task QueryWithProjection()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var sw = new Stopwatch();
			sw.Start();

			var results = (await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where
					x.Metadata.Properties.ContentType == "application/json"
						&& x.Content != null && x.Content.Name == "Test Document 2"
				select x
			).ToListAsync(TestContext.Current.CancellationToken)).Select(x => x.Content);

			sw.Stop();
			Console.WriteLine(sw.Elapsed);
			foreach (var result in results)
			{
				System.Diagnostics.Trace.WriteLine($"{result?.Id}: {result?.Name}");
				Console.WriteLine($"{result?.Id}: {result?.Name}");
			}

			Assert.Single(results);
			Assert.Equal("Test Document 2", results.First()?.Name);
		}

		[Fact]
		public async Task AsyncAny_ReturnsTrue()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var result = await (
				from x in containerClient.AsQueryable<TestDocument>(15)
				where x.BlobName.StartsWith("sample004") && x.Content != null && x.Content.Id == 4
				select x
			).AnyAsync(TestContext.Current.CancellationToken);
			Assert.True(result);
		}

		[Fact]
		public async Task AsyncFirstOrDefault_ReturnsExpected()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var result = await (
				from x in containerClient.AsQueryable<TestDocument>(15)
				where x.BlobName.StartsWith("TestDocument") && x.Content != null
				select x
			).FirstOrDefaultAsync(TestContext.Current.CancellationToken);
			Assert.NotNull(result);
			Assert.True(result.Content?.Name == "Test Document 1" || result.Content?.Name == "Test Document 2");
		}

		[Fact]
		public async Task AsyncSingle_ReturnsExpected()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var result = await (
				from x in containerClient.AsQueryable<TestDocument>(15)
				where x.BlobName.StartsWith("TestDocument") && x.Content != null && x.Content.Name == "Test Document 1"
				select x
			).SingleAsync(TestContext.Current.CancellationToken);
			Assert.NotNull(result);
			Assert.Equal("Test Document 1", result.Content?.Name);
		}

		[Fact]
		public async Task AsyncTake_ReturnsExpectedCountAndContent()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");
			var results = await (
				from x in containerClient.AsQueryable<TestDocument>(15)
				where x.BlobName.StartsWith("TestDocument") && x.Content != null
				select x
			).TakeAsync(2, TestContext.Current.CancellationToken);
			Assert.NotNull(results);
			Assert.Equal(2, results.Count);
			Assert.Contains(results, r => r.Content?.Name == "Test Document 1");
			Assert.Contains(results, r => r.Content?.Name == "Test Document 2");
		}

		[Fact]
		public async Task Deserialization_String()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var result = await
			(
				from x in containerClient.AsQueryable<string>(deserializer: StringBlobDeserializer.Default)
				where
					x.BlobName.StartsWith("TestDocument")
					&& x.BlobName.EndsWith(".json") 
					&& x.Content != null && x.Content.Contains("Test Document 2", StringComparison.OrdinalIgnoreCase)
				select x
			).FirstAsync(TestContext.Current.CancellationToken);

			Assert.Equal(typeof(string), result.Content?.GetType());
			Assert.False(string.IsNullOrEmpty(result.Content));
			Assert.True(result.Content.Contains("Test Document 2", StringComparison.OrdinalIgnoreCase));
		}

		[Fact]
		public async Task Deserialization_ByteArray()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			var result = await
			(
				from x in containerClient.AsQueryable<byte[]>(deserializer: ByteArrayBlobDeserializer.Default)
				where
					x.BlobName.StartsWith("TestDocument")
					&& x.BlobName.EndsWith(".json")
				select x
			).FirstAsync(TestContext.Current.CancellationToken);

			Assert.Equal(typeof(byte[]), result.Content?.GetType());
			Assert.NotNull(result.Content);
			Assert.False(result.Content?.Length <= 0);
		}

		#region Closure capture regression (fix #1)

		[Fact]
		public async Task MetadataFilter_WithClosureCapturedVariable()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			// Closure-captured variable — previously crashed MetadataAccessRewriter
			string expectedContentType = "application/json";
			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where x.Metadata.Properties.ContentType == expectedContentType
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			Assert.NotEmpty(results);
			Assert.All(results, r => Assert.Equal("application/json", r.Metadata.Properties.ContentType));
		}

		[Fact]
		public async Task ContentFilter_WithClosureCapturedVariable()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			// Closure-captured variable used in content filter
			string expectedName = "Test Document 1";
			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where x.BlobName.StartsWith("TestDocument")
					&& x.Content != null && x.Content.Name == expectedName
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			Assert.Single(results);
			Assert.Equal("Test Document 1", results.First().Content?.Name);
		}

		[Fact]
		public async Task MixedFilter_WithClosureCapturedVariables()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			// Both metadata and content filters using closure captures
			string contentType = "application/json";
			string docName = "Test Document 2";
			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where x.Metadata.Properties.ContentType == contentType
					&& x.Content != null && x.Content.Name == docName
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			Assert.Single(results);
			Assert.Equal("Test Document 2", results.First().Content?.Name);
		}

		#endregion

		#region BlobItem metadata dictionary (fix #6)

		[Fact]
		public async Task BlobItemQuery_MetadataDictionary_IsAccessible()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			// BlobTraits.Metadata is now passed to GetBlobsAsync, so the Metadata
			// dictionary should be non-null (though empty if no user-defined tags exist).
			var results = await
			(
				from x in containerClient.AsBlobItemQueryable()
				where x.Properties.ContentType == "application/json"
				select x
			).TakeAsync(3, TestContext.Current.CancellationToken);

			Assert.NotEmpty(results);
			Assert.All(results, r =>
			{
				Assert.NotNull(r.Metadata);
			});
		}

		[Fact]
		public async Task BlobItemQuery_UserDefinedMetadata_FiltersByTag()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			// Requires blobs uploaded with Upload-TestBlobs.ps1 (which sets category/source tags).
			// Check if metadata is populated first — skip if blobs haven't been re-uploaded.
			var probe = await
			(
				from x in containerClient.AsBlobItemQueryable()
				where x.Name == "TestDocument1.json"
				select x
			).FirstOrDefaultAsync(TestContext.Current.CancellationToken);

			Assert.SkipWhen(
				probe == null || !probe.Metadata.ContainsKey("source"),
				"Test blobs do not have user-defined metadata. Re-run Upload-TestBlobs.ps1 to upload with metadata tags.");

			// Filter by user-defined metadata: only "test" category blobs
			var results = await
			(
				from x in containerClient.AsBlobItemQueryable()
				where x.Metadata.ContainsKey("category")
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			Assert.NotEmpty(results);
			Assert.All(results, r => Assert.True(r.Metadata.ContainsKey("category")));
		}

		[Fact]
		public async Task ContentQuery_UserDefinedMetadata_FiltersByTag()
		{
			Assert.SkipUnless(_queryTestsFixture.IsConfigured, SkipReason);
			var containerClient = _queryTestsFixture.BlobServiceClient.GetBlobContainerClient("blinqtestdata");

			// Check if metadata is populated first
			var probe = await
			(
				from x in containerClient.AsBlobItemQueryable()
				where x.Name == "TestDocument1.json"
				select x
			).FirstOrDefaultAsync(TestContext.Current.CancellationToken);

			Assert.SkipWhen(
				probe == null || !probe.Metadata.ContainsKey("category"),
				"Test blobs do not have user-defined metadata. Re-run Upload-TestBlobs.ps1 to upload with metadata tags.");

			// Use closure-captured variable to filter by user-defined metadata via the content path
			string expectedCategory = "test";
			var results = await
			(
				from x in containerClient.AsQueryable<TestDocument>(15)
				where x.Metadata.Metadata.ContainsKey("category")
					&& x.Metadata.Metadata["category"] == expectedCategory
				select x
			).ToListAsync(TestContext.Current.CancellationToken);

			Assert.NotEmpty(results);
			Assert.All(results, r =>
			{
				Assert.StartsWith("TestDocument", r.BlobName);
			});
		}

		#endregion

	}

	public class QueryTestsFixture
	{
		private readonly BlobServiceClient? _blobServiceClient;

		/// <summary>
		/// Returns true if Azure credentials are configured and integration tests can run.
		/// </summary>
		public bool IsConfigured { get; }

		public QueryTestsFixture()
		{
			// Build configuration: user secrets first, then environment variables
			var config = new ConfigurationBuilder()
					.AddUserSecrets<FilterTests>()
					.AddEnvironmentVariables()
					.Build();

			var blobUri = config["AZURE_STORAGE_BLOB_URI"];
			if (string.IsNullOrWhiteSpace(blobUri))
			{
				IsConfigured = false;
				return;
			}

			var chainedCredential = new ChainedTokenCredential(
					new DefaultAzureCredential(),
					new VisualStudioCodeCredential(),
					new InteractiveBrowserCredential()
			);
			_blobServiceClient = new BlobServiceClient(new Uri(blobUri), chainedCredential);

			// Warm up the connection to the blob account
			var containerClient = _blobServiceClient.GetBlobContainerClient("blinqtestdata");
			var blobs = containerClient.GetBlobs(prefix: "test" + Guid.NewGuid().ToString());
			_ = blobs.Count();

			IsConfigured = true;
		}

		public BlobServiceClient BlobServiceClient
		{
			get
			{
				if (!IsConfigured)
				{
					throw new InvalidOperationException(
						"Azure Storage is not configured. Set AZURE_STORAGE_BLOB_URI in user secrets or as an environment variable.");
				}

				return _blobServiceClient!;
			}
		}
	}

	public class TestDocument
	{
		[System.Text.Json.Serialization.JsonPropertyName("id")]
		public int Id { get; set; }

		[System.Text.Json.Serialization.JsonPropertyName("name")]
		public string? Name { get; set; }

		[System.Text.Json.Serialization.JsonPropertyName("active")]
		public bool Active { get; set; }
	}
}
