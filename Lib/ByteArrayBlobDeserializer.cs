using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Blinq
{
	/// <summary>
	/// Deserializes blob content from a stream as a byte array.
	/// Efficiently copies the stream to a buffer and returns the result as a <c>byte[]</c>.
	/// </summary>
	/// <remarks>
	/// Only supports deserialization to <c>byte[]</c>. Throws <see cref="NotSupportedException"/> for other types.
	/// </remarks>
	public sealed class ByteArrayBlobDeserializer : IBlobContentDeserializer
	{
		private static ByteArrayBlobDeserializer? _Default;

		/// <summary>
		/// Returns a singleton instance of <see cref="ByteArrayBlobDeserializer"/>.
		/// </summary>
		public static ByteArrayBlobDeserializer Default { get { return _Default ??= new(); } }

		/// <summary>
		/// Deserializes the provided stream into a <c>byte[]</c>.
		/// </summary>
		/// <typeparam name="T">The target type. Must be <c>byte[]</c>.</typeparam>
		/// <param name="stream">The input stream containing blob content.</param>
		/// <returns>The deserialized byte array.</returns>
		/// <exception cref="NotSupportedException">Thrown if <typeparamref name="T"/> is not <c>byte[]</c>.</exception>
		public T? Deserialize<T>(Stream stream)
		{
			if (typeof(T) != typeof(byte[]))
			{
				throw new NotSupportedException("ByteArrayBlobDeserializer only supports byte[] type.");
			}

			if (stream.CanSeek && stream.Length > 0)
			{
				var buffer = new byte[stream.Length];
				int read, totalRead = 0;
				while (totalRead < buffer.Length &&
					 (read = stream.Read(buffer, totalRead, buffer.Length - totalRead)) > 0)
				{
					totalRead += read;
				}
				return (T)(object)buffer;
			}
			else
			{
				using (var ms = new MemoryStream())
				{
					stream.CopyTo(ms, 81920);
					return (T)(object)ms.ToArray();
				}
			}
		}

		/// <summary>
		/// Asynchronously deserializes the provided stream into a <c>byte[]</c>.
		/// </summary>
		/// <typeparam name="T">The target type. Must be <c>byte[]</c>.</typeparam>
		/// <param name="stream">The input stream containing blob content.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task representing the asynchronous operation, with the deserialized byte array as its result.</returns>
		/// <exception cref="NotSupportedException">Thrown if <typeparamref name="T"/> is not <c>byte[]</c>.</exception>
		public async Task<T?> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default)
		{
			if (typeof(T) != typeof(byte[]))
			{
				throw new NotSupportedException("ByteArrayBlobDeserializer only supports byte[] type.");
			}

			if (stream.CanSeek && stream.Length > 0)
			{
				var buffer = new byte[stream.Length];
				int read, totalRead = 0;
				while (totalRead < buffer.Length &&
					(read = await stream.ReadAsync(buffer, totalRead, buffer.Length - totalRead, cancellationToken).ConfigureAwait(false)) > 0)
				{
					totalRead += read;
				}
				return (T)(object)buffer;
			}
			else
			{
				using (var ms = new MemoryStream())
				{
					await stream.CopyToAsync(ms, 81920, cancellationToken).ConfigureAwait(false);
					return (T)(object)ms.ToArray();
				}
			}
		}
	}
}
