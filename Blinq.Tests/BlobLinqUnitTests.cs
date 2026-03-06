using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Blinq;

namespace Blinq.Tests
{
	[Trait("Category", "Unit")]
	public class BlobLinqUnitTests
	{
		[Fact]
		public void StringBlobDeserializer_DeserializesStringCorrectly()
		{
			var deserializer = new StringBlobDeserializer();
			var testString = "Hello, Blinq!";
			using var ms = new MemoryStream(Encoding.UTF8.GetBytes(testString));
			var result = deserializer.Deserialize<string>(ms);
			Assert.Equal(testString, result);
		}

		[Fact]
		public async Task StringBlobDeserializer_DeserializesStringAsyncCorrectly()
		{
			var deserializer = new StringBlobDeserializer();
			var testString = "Async Hello, Blinq!";
			using var ms = new MemoryStream(Encoding.UTF8.GetBytes(testString));
			var result = await deserializer.DeserializeAsync<string>(ms);
			Assert.Equal(testString, result);
		}

		[Fact]
		public void StringBlobDeserializer_ThrowsOnUnsupportedType()
		{
			var deserializer = new StringBlobDeserializer();
			using var ms = new MemoryStream(Encoding.UTF8.GetBytes("irrelevant"));
			Assert.Throws<NotSupportedException>(() => deserializer.Deserialize<int>(ms));
		}

		[Fact]
		public async Task StringBlobDeserializer_ThrowsOnUnsupportedTypeAsync()
		{
			var deserializer = new StringBlobDeserializer();
			using var ms = new MemoryStream(Encoding.UTF8.GetBytes("irrelevant"));
			await Assert.ThrowsAsync<NotSupportedException>(async () => await deserializer.DeserializeAsync<int>(ms));
		}

		[Fact]
		public void ByteArrayBlobDeserializer_DeserializesByteArrayCorrectly()
		{
			var deserializer = new ByteArrayBlobDeserializer();
			var testBytes = new byte[] {1,2,3,4,5 };
			using var ms = new MemoryStream(testBytes);
			var result = deserializer.Deserialize<byte[]>(ms);
			Assert.Equal(testBytes, result);
		}

		[Fact]
		public async Task ByteArrayBlobDeserializer_DeserializesByteArrayAsyncCorrectly()
		{
			var deserializer = new ByteArrayBlobDeserializer();
			var testBytes = new byte[] {10,20,30,40,50 };
			using var ms = new MemoryStream(testBytes);
			var result = await deserializer.DeserializeAsync<byte[]>(ms);
			Assert.Equal(testBytes, result);
		}

		[Fact]
		public void ByteArrayBlobDeserializer_ThrowsOnUnsupportedType()
		{
			var deserializer = new ByteArrayBlobDeserializer();
			using var ms = new MemoryStream(new byte[] {1 });
			Assert.Throws<NotSupportedException>(() => deserializer.Deserialize<string>(ms));
		}

		[Fact]
		public async Task ByteArrayBlobDeserializer_ThrowsOnUnsupportedTypeAsync()
		{
			var deserializer = new ByteArrayBlobDeserializer();
			using var ms = new MemoryStream(new byte[] {1 });
			await Assert.ThrowsAsync<NotSupportedException>(async () => await deserializer.DeserializeAsync<string>(ms));
		}

		[Fact]
		public void StringBlobDeserializer_DeserializesEmptyStreamToEmptyString()
		{
			var deserializer = new StringBlobDeserializer();
			using var ms = new MemoryStream();
			var result = deserializer.Deserialize<string>(ms);
			Assert.Equal(string.Empty, result);
		}

		[Fact]
		public void ByteArrayBlobDeserializer_DeserializesEmptyStreamToEmptyArray()
		{
			var deserializer = new ByteArrayBlobDeserializer();
			using var ms = new MemoryStream();
			var result = deserializer.Deserialize<byte[]>(ms);
			Assert.NotNull(result);
			Assert.Empty(result);
		}
	}
}
