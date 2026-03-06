using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Blinq
{
	/// <summary>
	/// Provides asynchronous LINQ extension methods for <see cref="IQueryable"/> of <see cref="BlobDocument{T}"/> over Azure Blob Storage.
	/// These methods enable efficient, non-blocking queries and materialization of blob results.
	/// </summary>
	public static class BlobQueryableAsyncExtensions
	{
		private static IBlobQueryProvider<T> GetProvider<T>(IQueryable<BlobDocument<T>> source)
		{
			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			if (source.Provider is IBlobQueryProvider<T> provider)
			{
				return provider;
			}

			throw new BlinqQueryException(
				"Provider does not support async execution.\n" +
				BlinqQueryException.ProjectionLimitationMessage
			);
		}

		/// <summary>
		/// Asynchronously determines whether any blobs match the query.
		/// <b>Projection Limitation:</b> Only filtering is supported in the query. Projections (e.g., <c>select x.Content</c>) must be performed after materialization (e.g., after <c>ToListAsync()</c>).
		/// </summary>
		/// <typeparam name="T">The type of blob content.</typeparam>
		/// <param name="source">The queryable blob source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to <c>true</c> if any blobs match; otherwise, <c>false</c>.</returns>
		/// <exception cref="BlinqProjectionNotSupportedException">Thrown if a projection is attempted in query construction.</exception>
		public static async Task<bool> AnyAsync<T>(this IQueryable<BlobDocument<T>> source, CancellationToken cancellationToken = default)
		{
			var provider = GetProvider(source);
			await foreach (var _ in provider.ExecuteAsync(source.Expression, cancellationToken).ConfigureAwait(false))
			{
				return true;
			}

			return false;
		}

		/// <summary>
		/// Asynchronously returns the first blob matching the query, or <c>null</c> if none found.
		/// <b>Projection Limitation:</b> Only filtering is supported in the query. Projections must be performed after materialization.
		/// </summary>
		/// <typeparam name="T">The type of blob content.</typeparam>
		/// <param name="source">The queryable blob source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to the first matching <see cref="BlobDocument{T}"/>, or <c>null</c> if none found.</returns>
		/// <exception cref="BlinqProjectionNotSupportedException">Thrown if a projection is attempted in query construction.</exception>
		public static async Task<BlobDocument<T>?> FirstOrDefaultAsync<T>(this IQueryable<BlobDocument<T>> source, CancellationToken cancellationToken = default)
		{
			var provider = GetProvider(source);
			await foreach (var item in provider.ExecuteAsync(source.Expression, cancellationToken).ConfigureAwait(false))
			{
				return item;
			}

			return default;
		}

		/// <summary>
		/// Asynchronously returns the first blob matching the query.
		/// Throws if no blobs are found.
		/// <b>Projection Limitation:</b> Only filtering is supported in the query. Projections must be performed after materialization.
		/// </summary>
		/// <typeparam name="T">The type of blob content.</typeparam>
		/// <param name="source">The queryable blob source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to the first matching <see cref="BlobDocument{T}"/>.</returns>
		/// <exception cref="InvalidOperationException">Thrown if no blobs are found.</exception>
		/// <exception cref="BlinqProjectionNotSupportedException">Thrown if a projection is attempted in query construction.</exception>
		public static async Task<BlobDocument<T>> FirstAsync<T>(this IQueryable<BlobDocument<T>> source, CancellationToken cancellationToken = default)
		{
			var provider = GetProvider(source);
			await foreach (var item in provider.ExecuteAsync(source.Expression, cancellationToken).ConfigureAwait(false))
			{
				return item;
			}

			throw new InvalidOperationException("Sequence does not contain any elements.");
		}

		/// <summary>
		/// Asynchronously returns the single blob matching the query.
		/// Throws if no blobs or more than one blob is found.
		/// <b>Projection Limitation:</b> Only filtering is supported in the query. Projections must be performed after materialization.
		/// </summary>
		/// <typeparam name="T">The type of blob content.</typeparam>
		/// <param name="source">The queryable blob source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to the single matching <see cref="BlobDocument{T}"/>.</returns>
		/// <exception cref="InvalidOperationException">Thrown if no blobs or more than one blob is found.</exception>
		/// <exception cref="BlinqProjectionNotSupportedException">Thrown if a projection is attempted in query construction.</exception>
		public static async Task<BlobDocument<T>> SingleAsync<T>(this IQueryable<BlobDocument<T>> source, CancellationToken cancellationToken = default)
		{
			var provider = GetProvider(source);
			BlobDocument<T>? result = null;
			bool found = false;
			await foreach (var item in provider.ExecuteAsync(source.Expression, cancellationToken).ConfigureAwait(false))
			{
				if (found)
				{
					throw new InvalidOperationException("Sequence contains more than one element.");
				}

				result = item;
				found = true;
			}

			if (!found)
			{
				throw new InvalidOperationException("Sequence contains no elements.");
			}

			return result!;
		}

		/// <summary>
		/// Asynchronously takes up to <paramref name="count"/> blobs from the query results.
		/// <b>Projection Limitation:</b> Only filtering is supported in the query. Projections must be performed after materialization.
		/// </summary>
		/// <typeparam name="T">The type of blob content.</typeparam>
		/// <param name="source">The queryable blob source.</param>
		/// <param name="count">The maximum number of blobs to take.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to a list of up to <paramref name="count"/> <see cref="BlobDocument{T}"/> results.</returns>
		/// <exception cref="BlinqProjectionNotSupportedException">Thrown if a projection is attempted in query construction.</exception>
		public static async Task<List<BlobDocument<T>>> TakeAsync<T>(this IQueryable<BlobDocument<T>> source, int count, CancellationToken cancellationToken = default)
		{
			var provider = GetProvider(source);
			var list = new List<BlobDocument<T>>();
			await foreach (var item in provider.ExecuteAsync(source.Expression, cancellationToken).ConfigureAwait(false))
			{
				list.Add(item);
				if (list.Count >= count)
				{
					break;
				}
			}

			return list;
		}
	}
}
