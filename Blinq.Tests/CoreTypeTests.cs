using Azure.Storage.Blobs.Models;

namespace Blinq.Tests
{
	[Trait("Category", "Unit")]
	public class CoreTypeTests
	{
		#region BlobDocument

		[Fact]
		public void BlobDocument_ConstructorAssignsAllProperties()
		{
			var metadata = BlobsModelFactory.BlobItem("test.json", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 42));
			var content = "hello";

			var doc = new BlobDocument<string>("test.json", metadata, content);

			Assert.Equal("test.json", doc.BlobName);
			Assert.Same(metadata, doc.Metadata);
			Assert.Equal("hello", doc.Content);
		}

		[Fact]
		public void BlobDocument_NullContent_AllowedForMetadataOnly()
		{
			var metadata = BlobsModelFactory.BlobItem("meta-only.json", false, BlobsModelFactory.BlobItemProperties(false));

			var doc = new BlobDocument<string>("meta-only.json", metadata, default!);

			Assert.Equal("meta-only.json", doc.BlobName);
			Assert.Same(metadata, doc.Metadata);
			Assert.Null(doc.Content);
		}

		#endregion

		#region BlobQueryException

		[Fact]
		public void BlobQueryException_StoresContainerAndBlobName()
		{
			var inner = new InvalidOperationException("download failed");
			var ex = new BlobQueryException("Error reading blob", "my-container", "path/to/blob.json", inner);

			Assert.Equal("Error reading blob", ex.Message);
			Assert.Equal("my-container", ex.ContainerName);
			Assert.Equal("path/to/blob.json", ex.BlobName);
			Assert.Same(inner, ex.InnerException);
		}

		#endregion

		#region BlinqQueryException

		[Fact]
		public void BlinqProjectionNotSupportedException_InheritsFromBlinqQueryException()
		{
			var ex = new BlinqProjectionNotSupportedException("no projections");

			Assert.IsAssignableFrom<BlinqQueryException>(ex);
			Assert.Equal("no projections", ex.Message);
		}

		[Fact]
		public void BlinqQueryException_StoresMessageAndInnerException()
		{
			var inner = new FormatException("bad format");
			var ex = new BlinqQueryException("query failed", inner);

			Assert.Equal("query failed", ex.Message);
			Assert.Same(inner, ex.InnerException);
		}

		#endregion
	}
}
