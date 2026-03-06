using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Blinq
{
	/// <summary>
	/// Deserializes blob content from a stream using System.Text.Json.
	/// Supports flexible JSON options and is the default deserializer for JSON-formatted blobs.
	/// </summary>
	/// <remarks>
	/// Allows customization of <see cref="JsonSerializerOptions"/> for advanced scenarios.
	/// </remarks>
	public sealed class JsonBlobDeserializer : IBlobContentDeserializer
	{
		private static JsonBlobDeserializer? _Default;

		/// <summary>
		/// Returns a singleton instance of <see cref="JsonBlobDeserializer"/>.
		/// </summary>
		public static JsonBlobDeserializer Default { get { return _Default ??= new(); } }

		private readonly JsonSerializerOptions _options;

		/// <summary>
		/// Initializes a new instance of <see cref="JsonBlobDeserializer"/> with optional JSON serializer options.
		/// </summary>
		/// <param name="options">The <see cref="JsonSerializerOptions"/> to use. If null, sensible defaults are applied.</param>
		public JsonBlobDeserializer(JsonSerializerOptions? options = null)
		{
			_options = options ?? new JsonSerializerOptions
			{
				AllowTrailingCommas = true,
				ReadCommentHandling = JsonCommentHandling.Skip
			};
		}

		/// <summary>
		/// Deserializes the provided stream into an object of type <typeparamref name="T"/> using System.Text.Json.
		/// </summary>
		/// <typeparam name="T">The target type to deserialize to.</typeparam>
		/// <param name="stream">The input stream containing JSON blob content.</param>
		/// <returns>The deserialized object of type <typeparamref name="T"/>.</returns>
		public T? Deserialize<T>(Stream stream)
		{
			return JsonSerializer.Deserialize<T>(stream, _options);
		}

		/// <summary>
		/// Asynchronously deserializes the provided stream into an object of type <typeparamref name="T"/> using System.Text.Json.
		/// </summary>
		/// <typeparam name="T">The target type to deserialize to.</typeparam>
		/// <param name="stream">The input stream containing JSON blob content.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task representing the asynchronous operation, with the deserialized object as its result.</returns>
		public async Task<T?> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default)
		{
			return await JsonSerializer.DeserializeAsync<T>(stream, _options, cancellationToken).ConfigureAwait(false);
		}
	}
}
