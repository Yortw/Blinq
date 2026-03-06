using System;

namespace Blinq
{
	/// <summary>
	/// Exception thrown when a blob-level operation (download, deserialization) fails during query execution.
	/// Includes the container and blob name to aid diagnosis.
	/// </summary>
	public sealed class BlobQueryException : BlinqQueryException
	{
		/// <summary>
		/// Gets the name of the Azure Blob container where the error occurred.
		/// </summary>
		public string ContainerName { get; }

		/// <summary>
		/// Gets the name of the blob where the error occurred.
		/// </summary>
		public string BlobName { get; }

		/// <summary>
		/// Initializes a new instance of the <see cref="BlobQueryException"/> class.
		/// </summary>
		/// <param name="message">The error message.</param>
		/// <param name="containerName">The name of the blob container.</param>
		/// <param name="blobName">The name of the blob.</param>
		/// <param name="innerException">The exception that is the cause of the current exception.</param>
		public BlobQueryException(string message, string containerName, string blobName, Exception innerException)
			: base(message, innerException)
		{
			ContainerName = containerName;
			BlobName = blobName;
		}
	}
}
