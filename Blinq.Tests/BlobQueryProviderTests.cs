using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Blinq.Tests
{
	[Trait("Category", "Unit")]
	public class BlobQueryProviderTests
	{
		/// <summary>
		/// Creates a BlobContainerClient that doesn't require an Azure connection.
		/// No network call is made until an actual blob operation.
		/// </summary>
		private static BlobContainerClient FakeContainerClient() =>
			new BlobContainerClient(new Uri("https://fake.blob.core.windows.net/test"));

		[Fact]
		public void Constructor_NullContainer_ThrowsArgumentNullException()
		{
			Assert.Throws<ArgumentNullException>(() =>
				new BlobQueryProvider<string>(null!));
		}

		[Fact]
		public void Constructor_ZeroConcurrency_ThrowsArgumentException()
		{
			Assert.Throws<ArgumentException>(() =>
				new BlobQueryProvider<string>(FakeContainerClient(), maxConcurrency: 0));
		}

		[Fact]
		public void Constructor_NegativeConcurrency_ThrowsArgumentException()
		{
			Assert.Throws<ArgumentException>(() =>
				new BlobQueryProvider<string>(FakeContainerClient(), maxConcurrency: -1));
		}

		[Fact]
		public void CreateQuery_WrongElementType_ThrowsBlinqProjectionNotSupportedException()
		{
			var provider = new BlobQueryProvider<string>(FakeContainerClient());

			// Try to create a query with a different element type (int instead of BlobDocument<string>)
			var expr = Expression.Constant(Array.Empty<BlobDocument<string>>().AsQueryable());

			Assert.Throws<BlinqProjectionNotSupportedException>(() =>
				provider.CreateQuery<int>(expr));
		}

		[Fact]
		public async Task AnyAsync_NullSource_ThrowsArgumentNullException()
		{
			await Assert.ThrowsAsync<ArgumentNullException>(() =>
				BlobQueryableAsyncExtensions.AnyAsync<string>(null!));
		}

		[Fact]
		public async Task FirstOrDefaultAsync_NullSource_ThrowsArgumentNullException()
		{
			await Assert.ThrowsAsync<ArgumentNullException>(() =>
				BlobQueryableAsyncExtensions.FirstOrDefaultAsync<string>(null!));
		}

		[Fact]
		public async Task TakeAsync_NullSource_ThrowsArgumentNullException()
		{
			await Assert.ThrowsAsync<ArgumentNullException>(() =>
				BlobQueryableAsyncExtensions.TakeAsync<string>(null!, 5));
		}

		[Fact]
		public async Task ToAsyncEnumerable_BlobItem_CancelledToken_ThrowsOperationCancelled()
		{
			var cts = new CancellationTokenSource();
			cts.Cancel();

			var source = new[] {
				BlobsModelFactory.BlobItem("a", false, BlobsModelFactory.BlobItemProperties(false))
			}.AsQueryable();

			await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			{
				await foreach (var item in source.ToAsyncEnumerable(cts.Token))
				{
					// Should not reach here
				}
			});
		}

		[Fact]
		public void AsAsyncEnumerable_NonBlinqQueryable_ThrowsBlinqQueryException()
		{
			var plainQueryable = Array.Empty<BlobDocument<string>>().AsQueryable();
			Assert.Throws<BlinqQueryException>(() =>
				BlobQueryableExtensions.AsAsyncEnumerable(plainQueryable));
		}

		[Fact]
		public void AsBlobItemQueryable_NullContainer_ThrowsArgumentNullException()
		{
			Assert.Throws<ArgumentNullException>(() =>
				BlobQueryableExtensions.AsBlobItemQueryable(null!));
		}

		[Fact]
		public void CreateQuery_CorrectElementType_ReturnsBlobQueryable()
		{
			var provider = new BlobQueryProvider<string>(FakeContainerClient());

			var source = Array.Empty<BlobDocument<string>>().AsQueryable();
			var expr = source.Expression;

			var result = provider.CreateQuery<BlobDocument<string>>(expr);

			Assert.NotNull(result);
			Assert.IsAssignableFrom<IQueryable<BlobDocument<string>>>(result);
		}
	}
}
