using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Blinq
{
	/// <summary>
	/// An interface for components that can deserialize a blob's contents from a stream into a specified type.
	/// </summary>
	/// <remarks>
	/// <para>Implementations of this component must be thread-safe. The same instance may be called to deserialise blobs on parallel threads.</para>
	/// </remarks>
	/// <seealso cref="JsonBlobDeserializer"/>
	/// <seealso cref="StringBlobDeserializer"/>
	/// <seealso cref="ByteArrayBlobDeserializer"/>
	public interface IBlobContentDeserializer
	{
		/// <summary>
		/// Reads the specified stream and deserializes its content into an instance of type <typeparamref name="T"/>.
		/// </summary>
		/// <typeparam name="T">The type to deserialize into.</typeparam>
		/// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
		/// <returns>
		/// The deserialized object of type <typeparamref name="T"/>, or <c>null</c> if the content cannot be deserialized.
		/// </returns>
		/// <exception cref="IOException">Thrown if reading from the stream fails.</exception>
		/// <exception cref="NotSupportedException">Thrown if the deserializer does not support the requested type.</exception>
		T? Deserialize<T>(Stream stream);

		/// <summary>
		/// Asynchronously reads the specified stream and deserializes its content into an instance of type <typeparamref name="T"/>.
		/// </summary>
		/// <typeparam name="T">The type to deserialize into.</typeparam>
		/// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>
		/// A task representing the asynchronous operation, with the deserialized object of type <typeparamref name="T"/> as its result, or <c>null</c> if the content cannot be deserialized.
		/// </returns>
		/// <exception cref="IOException">Thrown if reading from the stream fails.</exception>
		/// <exception cref="NotSupportedException">Thrown if the deserializer does not support the requested type.</exception>
		Task<T?> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default);
	}
}
