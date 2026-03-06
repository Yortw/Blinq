using System;
using System.Linq;
using System.Linq.Expressions;
using Azure.Storage.Blobs;

namespace Blinq
{
	/// <summary>
	/// Extension methods for Azure Blob Storage LINQ queries.
	/// </summary>
	public static class BlobContainerClientExtensions
	{
		/// <summary>
		/// Returns an <see cref="IQueryable"/> for LINQ queries over blobs in this container. Each element is a <see cref="BlobDocument{T}"/>.
		/// </summary>
		/// <typeparam name="T">The type to deserialize blob content to.</typeparam>
		/// <param name="containerClient">The Azure Blob container client.</param>
		/// <param name="maxConcurrency">The maximum number of blobs to download/process in parallel while running queries.</param>
		/// <param name="deserializer">Optional custom blob content deserializer.</param>
		/// <returns>An <see cref="IQueryable"/> of <see cref="BlobDocument{T}"/> for querying blobs.</returns>
		public static IQueryable<BlobDocument<T>> AsQueryable<T>(this BlobContainerClient containerClient, int maxConcurrency = 4, IBlobContentDeserializer? deserializer = null)
		{
			if (containerClient == null)
			{
				throw new ArgumentNullException(nameof(containerClient));
			}

			var provider = new BlobQueryProvider<T>(containerClient, maxConcurrency, deserializer);
			var expression = Expression.Constant(null, typeof(IQueryable<BlobDocument<T>>));
			return new BlobQueryable<T>(provider, expression);
		}
	}
}
