using Azure.Storage.Blobs.Models;

namespace Blinq
{
	/// <summary>
	/// Represents an immutable Azure Storage blob returned from a LINQ query, providing access to both blob metadata and deserialized content.
	/// This class is immutable; its properties are set only via the constructor and cannot be changed after creation.
	/// </summary>
	/// <typeparam name="T">The type to which the blob content is deserialized.</typeparam>
	public sealed class BlobDocument<T>
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="BlobDocument{T}"/> class with the specified blob name, metadata, and content.
		/// </summary>
		/// <param name="blobName">The name (path relative from the container) of this blob.</param>
		/// <param name="metadata">The metadata and properties of the blob.</param>
		/// <param name="content">The deserialized content of the blob.</param>
		public BlobDocument(string blobName, BlobItem metadata, T content)
		{
			BlobName = blobName;
			Metadata = metadata;
			Content = content;
		}

		/// <summary>
		/// Gets the name (path relative from the container) of this blob.
		/// </summary>
		public string BlobName { get; }

		/// <summary>
		/// Gets the metadata and properties of the blob.
		/// </summary>
		public BlobItem Metadata { get; }

		/// <summary>
		/// Gets the deserialized content of the blob. May be <c>null</c> for metadata-only queries.
		/// </summary>
		public T? Content { get; }
	}
}
