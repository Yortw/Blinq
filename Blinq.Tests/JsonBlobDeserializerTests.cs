using System.Text;
using System.Text.Json;

namespace Blinq.Tests
{
	[Trait("Category", "Unit")]
	public class JsonBlobDeserializerTests
	{
		private static MemoryStream ToStream(string json) =>
			new MemoryStream(Encoding.UTF8.GetBytes(json));

		[Fact]
		public void Deserialize_ValidJson_ReturnsObject()
		{
			var deserializer = new JsonBlobDeserializer();
			using var stream = ToStream("""{"id":1,"name":"Test","active":true}""");

			var result = deserializer.Deserialize<TestDocument>(stream);

			Assert.NotNull(result);
			Assert.Equal(1, result.Id);
			Assert.Equal("Test", result.Name);
			Assert.True(result.Active);
		}

		[Fact]
		public async Task DeserializeAsync_ValidJson_ReturnsObject()
		{
			var deserializer = new JsonBlobDeserializer();
			using var stream = ToStream("""{"id":2,"name":"Async","active":false}""");

			var result = await deserializer.DeserializeAsync<TestDocument>(stream, TestContext.Current.CancellationToken);

			Assert.NotNull(result);
			Assert.Equal(2, result.Id);
			Assert.Equal("Async", result.Name);
			Assert.False(result.Active);
		}

		[Fact]
		public void Deserialize_EmptyStream_ThrowsJsonException()
		{
			var deserializer = new JsonBlobDeserializer();
			using var stream = new MemoryStream();

			Assert.Throws<JsonException>(() => deserializer.Deserialize<TestDocument>(stream));
		}

		[Fact]
		public void Deserialize_MalformedJson_ThrowsJsonException()
		{
			var deserializer = new JsonBlobDeserializer();
			using var stream = ToStream("{not valid json!!}");

			Assert.Throws<JsonException>(() => deserializer.Deserialize<TestDocument>(stream));
		}

		[Fact]
		public void Deserialize_TrailingCommasAllowed()
		{
			// Default options set AllowTrailingCommas = true
			var deserializer = new JsonBlobDeserializer();
			using var stream = ToStream("""{"id":3,"name":"Trailing",}""");

			var result = deserializer.Deserialize<TestDocument>(stream);

			Assert.NotNull(result);
			Assert.Equal(3, result.Id);
			Assert.Equal("Trailing", result.Name);
		}

		[Fact]
		public void Deserialize_CommentsSkipped()
		{
			// Default options set ReadCommentHandling = Skip
			var deserializer = new JsonBlobDeserializer();
			using var stream = ToStream("""
				{
					// This is a comment
					"id": 4,
					"name": "Commented"
				}
				""");

			var result = deserializer.Deserialize<TestDocument>(stream);

			Assert.NotNull(result);
			Assert.Equal(4, result.Id);
			Assert.Equal("Commented", result.Name);
		}

		[Fact]
		public void Default_ReturnsSameInstance()
		{
			var a = JsonBlobDeserializer.Default;
			var b = JsonBlobDeserializer.Default;

			Assert.Same(a, b);
		}
	}
}
