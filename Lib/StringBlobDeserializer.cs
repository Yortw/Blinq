using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Blinq
{
	/// <summary>
	/// Deserializes blob content from a stream as a string using the specified encoding.
	/// Efficiently reads the stream directly to a string, minimizing memory usage.
	/// </summary>
	/// <remarks>
	/// Defaults to UTF-8 encoding if none is specified.
	/// </remarks>
	public sealed class StringBlobDeserializer : IBlobContentDeserializer
	{
		private static StringBlobDeserializer? _Default;

		/// <summary>
		/// Returns a singleton instance of <see cref="StringBlobDeserializer"/>.
		/// </summary>
		public static StringBlobDeserializer Default { get { return _Default ??= new(); } }

		private readonly Encoding _encoding;

		/// <summary>
		/// Initializes a new instance of <see cref="StringBlobDeserializer"/> using UTF-8 encoding.
		/// </summary>
		public StringBlobDeserializer() : this(null)
		{
		}

		/// <summary>
		/// Initializes a new instance of <see cref="StringBlobDeserializer"/> using the specified encoding.
		/// </summary>
		/// <param name="encoding">The encoding to use when reading the stream. If null, UTF-8 is used.</param>
		public StringBlobDeserializer(Encoding? encoding)
		{
			_encoding = encoding ?? Encoding.UTF8;
		}

		/// <summary>
		/// Deserializes the provided stream into a string using the configured encoding.
		/// </summary>
		/// <typeparam name="T">The target type. Must be <see cref="string"/>.</typeparam>
		/// <param name="stream">The input stream containing blob content.</param>
		/// <returns>The deserialized string.</returns>
		/// <exception cref="NotSupportedException">Thrown if <typeparamref name="T"/> is not <see cref="string"/>.</exception>
		public T? Deserialize<T>(Stream stream)
		{
			if (typeof(T) != typeof(string))
			{
				throw new NotSupportedException("StringBlobDeserializer only supports string type.");
			}

			using (var reader = new StreamReader(stream, _encoding, detectEncodingFromByteOrderMarks: true, bufferSize: 8192, leaveOpen: true))
			{
				return (T)(object)reader.ReadToEnd();
			}
		}

		/// <summary>
		/// Asynchronously deserializes the provided stream into a string using the configured encoding.
		/// </summary>
		/// <typeparam name="T">The target type. Must be <see cref="string"/>.</typeparam>
		/// <param name="stream">The input stream containing blob content.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task representing the asynchronous operation, with the deserialized string as its result.</returns>
		/// <exception cref="NotSupportedException">Thrown if <typeparamref name="T"/> is not <see cref="string"/>.</exception>
		public async Task<T?> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default)
		{
			if (typeof(T) != typeof(string))
			{
				throw new NotSupportedException("StringBlobDeserializer only supports string type.");
			}

			using (var reader = new StreamReader(stream, _encoding, detectEncodingFromByteOrderMarks: true, bufferSize: 8192, leaveOpen: true))
			{
				var result = await reader.ReadToEndAsync().ConfigureAwait(false);
				return (T)(object)result;
			}
		}
	}
}
