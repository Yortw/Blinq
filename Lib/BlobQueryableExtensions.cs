using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Blinq
{
	/// <summary>
	/// Provides extension methods for querying Azure Blob Storage using LINQ, including metadata-only and content-based queries.
	/// </summary>
	public static class BlobQueryableExtensions
	{
		private static IBlobQueryProvider<T> GetProvider<T>(IQueryable<BlobDocument<T>> source)
		{
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
		/// Asynchronously enumerates all results of a query as <c>BlobDocument&lt;T&gt;</c>. For large containers, this can be expensive and memory-intensive. Prefer streaming or filtering for production use.
		/// </summary>
		/// <typeparam name="T">The type to which blob content is deserialized.</typeparam>
		/// <param name="source">The queryable blob source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to a list of <c>BlobDocument&lt;T&gt;</c> results.</returns>
		public static async Task<List<BlobDocument<T>>> ToListAsync<T>(this IQueryable<BlobDocument<T>> source, CancellationToken cancellationToken = default)
		{
			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			var provider = GetProvider(source);
			var asyncEnumerable = provider.ExecuteAsync(source.Expression, cancellationToken);
			var list = new List<BlobDocument<T>>();
			await foreach (var item in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
			{
				cancellationToken.ThrowIfCancellationRequested();
				if (item != null)
				{
					list.Add(item);
				}
			}

			return list;
		}

		/// <summary>
		/// Enables async enumeration (i.e. <c>await foreach</c>) over blob LINQ queries.
		/// </summary>
		/// <typeparam name="T">The type to which blob content is deserialized.</typeparam>
		/// <param name="query">The queryable blob source.</param>
		/// <returns>An async enumerable of <c>BlobDocument&lt;T&gt;</c> results.</returns>
		public static IAsyncEnumerable<BlobDocument<T>> AsAsyncEnumerable<T>(this IQueryable<BlobDocument<T>> query)
		{
			if (query is IAsyncEnumerable<BlobDocument<T>> asyncEnumerable)
			{
				return asyncEnumerable;
			}

			throw new BlinqQueryException("Query does not support async enumeration.");
		}

		/// <summary>
		/// Returns a metadata-only queryable for the blob container, enabling LINQ queries over <see cref="BlobItem"/> without downloading blob content.
		/// <b>Note:</b> Only metadata and properties are available; content is never downloaded.
		/// </summary>
		/// <param name="containerClient">The blob container client.</param>
		/// <param name="maxConcurrency">The maximum number of concurrent blob listing operations.</param>
		/// <returns>A queryable for LINQ queries over blob metadata.</returns>
		/// <exception cref="BlinqProjectionNotSupportedException">Thrown if a projection is attempted in query construction. Project to other types after materialization.</exception>
		public static IQueryable<BlobItem> AsBlobItemQueryable(this BlobContainerClient containerClient, int maxConcurrency = 4)
		{
			if (containerClient == null)
			{
				throw new ArgumentNullException(nameof(containerClient));
			}

			var provider = new BlobQueryProvider<object?>(containerClient, maxConcurrency, null, metadataOnly: true);
			var baseExpr = Expression.Constant(new EnumerableQuery<BlobItem>(Array.Empty<BlobItem>()));
			return new MetadataOnlyQueryable(provider, baseExpr);
		}

		/// <summary>
		/// Asynchronously enumerates all results of a metadata-only query as <see cref="BlobItem"/>. For large containers, this can be expensive and memory-intensive. Prefer streaming or filtering for production use.
		/// </summary>
		/// <param name="source">The queryable blob metadata source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to a list of <see cref="BlobItem"/> results.</returns>
		public static async Task<List<BlobItem>> ToListAsync(this IQueryable<BlobItem> source, CancellationToken cancellationToken = default)
		{
			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			var list = new List<BlobItem>();
			await foreach (var item in source.ToAsyncEnumerable().WithCancellation(cancellationToken).ConfigureAwait(false))
			{
				if (item != null)
				{
					list.Add(item);
				}
			}

			return list;
		}

		/// <summary>
		/// Enables async enumeration (i.e. <c>await foreach</c>) over BlobItem LINQ queries.
		/// If the underlying queryable supports <see cref="IAsyncEnumerable{BlobItem}"/>, this method will enumerate asynchronously; otherwise, it will enumerate synchronously.
		/// </summary>
		/// <param name="source">The queryable blob metadata source.</param>
		/// <returns>An async enumerable of <see cref="BlobItem"/> results.</returns>
		public static async IAsyncEnumerable<BlobItem> ToAsyncEnumerable(
			this IQueryable<BlobItem> source,
			[EnumeratorCancellation] CancellationToken cancellationToken = default)
		{
			if (source is IAsyncEnumerable<BlobItem> asyncEnumerable)
			{
				await foreach (var item in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
				{
					yield return item;
				}
			}
			else
			{
				foreach (var item in source)
				{
					cancellationToken.ThrowIfCancellationRequested();
					yield return item;
				}
			}
		}

		/// <summary>
		/// Asynchronously determines whether any <see cref="BlobItem"/> matches the query.
		/// </summary>
		/// <param name="source">The queryable blob metadata source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to <c>true</c> if any <see cref="BlobItem"/> matches; otherwise, <c>false</c>.</returns>
		public static async Task<bool> AnyAsync(this IQueryable<BlobItem> source, CancellationToken cancellationToken = default)
		{
			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			await foreach (var _ in source.ToAsyncEnumerable().WithCancellation(cancellationToken))
			{
				return true;
			}

			return false;
		}

		/// <summary>
		/// Asynchronously returns the first <see cref="BlobItem"/> matching the query, or <c>null</c> if none found.
		/// </summary>
		/// <param name="source">The queryable blob metadata source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to the first matching <see cref="BlobItem"/>, or <c>null</c> if none found.</returns>
		public static async Task<BlobItem?> FirstOrDefaultAsync(this IQueryable<BlobItem> source, CancellationToken cancellationToken = default)
		{
			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			await foreach (var item in source.ToAsyncEnumerable().WithCancellation(cancellationToken))
			{
				return item;
			}

			return default;
		}

		/// <summary>
		/// Asynchronously returns the first <see cref="BlobItem"/> matching the query. Throws if no <see cref="BlobItem"/> is found.
		/// </summary>
		/// <param name="source">The queryable blob metadata source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to the first matching <see cref="BlobItem"/>.</returns>
		/// <exception cref="InvalidOperationException">Thrown if no <see cref="BlobItem"/> is found.</exception>
		public static async Task<BlobItem> FirstAsync(this IQueryable<BlobItem> source, CancellationToken cancellationToken = default)
		{
			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			await foreach (var item in source.ToAsyncEnumerable().WithCancellation(cancellationToken))
			{
				return item;
			}

			throw new InvalidOperationException("Sequence does not contain any elements.");
		}

		/// <summary>
		/// Asynchronously returns the single <see cref="BlobItem"/> matching the query. Throws if no <see cref="BlobItem"/> or more than one <see cref="BlobItem"/> is found.
		/// </summary>
		/// <param name="source">The queryable blob metadata source.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to the single matching <see cref="BlobItem"/>.</returns>
		/// <exception cref="InvalidOperationException">Thrown if no <see cref="BlobItem"/> or more than one <see cref="BlobItem"/> is found.</exception>
		public static async Task<BlobItem> SingleAsync(this IQueryable<BlobItem> source, CancellationToken cancellationToken = default)
		{
			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			BlobItem? result = null;
			bool found = false;
			await foreach (var item in source.ToAsyncEnumerable().WithCancellation(cancellationToken))
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
		/// Asynchronously takes up to <paramref name="count"/> <see cref="BlobItem"/> from the query results.
		/// </summary>
		/// <param name="source">The queryable blob metadata source.</param>
		/// <param name="count">The maximum number of <see cref="BlobItem"/> to take.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <returns>A task that resolves to a list of up to <paramref name="count"/> <see cref="BlobItem"/> results.</returns>
		public static async Task<List<BlobItem>> TakeAsync(this IQueryable<BlobItem> source, int count, CancellationToken cancellationToken = default)
		{
			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			var list = new List<BlobItem>();
			await foreach (var item in source.ToAsyncEnumerable().WithCancellation(cancellationToken))
			{
				list.Add(item);
				if (list.Count >= count)
				{
					break;
				}
			}

			return list;
		}

		private sealed class MetadataOnlyQueryable : IQueryable<BlobItem>, IAsyncEnumerable<BlobItem>
		{
			private readonly BlobQueryProvider<object?> _innerProvider;

			public MetadataOnlyQueryable(BlobQueryProvider<object?> provider, Expression baseExpr)
			{
				_innerProvider = provider;
				Provider = new MetadataOnlyQueryProvider(provider);
				Expression = baseExpr;
			}

			public Type ElementType => typeof(BlobItem);
			public Expression Expression { get; }
			public IQueryProvider Provider { get; }

			public System.Collections.IEnumerator GetEnumerator()
			{
				var result = (IEnumerable<BlobItem>)(Provider.Execute(Expression)
					?? throw new InvalidOperationException("Query execution returned null."));
				return result.GetEnumerator();
			}

			IEnumerator<BlobItem> IEnumerable<BlobItem>.GetEnumerator()
			{
				var result = (IEnumerable<BlobItem>)(Provider.Execute(Expression)
					?? throw new InvalidOperationException("Query execution returned null."));
				return result.GetEnumerator();
			}

			public async IAsyncEnumerator<BlobItem> GetAsyncEnumerator(CancellationToken cancellationToken = default)
			{
				await foreach (var doc in _innerProvider.ExecuteAsync(Expression, cancellationToken).ConfigureAwait(false))
				{
					yield return doc.Metadata;
				}
			}
		}

		private sealed class MetadataOnlyQueryProvider : IQueryProvider
		{
			private readonly BlobQueryProvider<object?> _inner;

			public MetadataOnlyQueryProvider(BlobQueryProvider<object?> inner)
			{
				_inner = inner;
			}

			public IQueryable CreateQuery(Expression expression)
			{
				return new MetadataOnlyQueryable(_inner, expression);
			}

			public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
			{
				if (typeof(TElement) != typeof(BlobItem))
				{
					throw new BlinqProjectionNotSupportedException(
						"Only BlobItem is supported for metadata-only queries. " +
						"Project to other types after materialization, e.g., after calling ToListAsync()."
					);
				}

				return (IQueryable<TElement>)((object)new MetadataOnlyQueryable(_inner, expression));
			}

			public object Execute(Expression expression)
			{
				var docs = _inner.Execute(expression) as IEnumerable<BlobDocument<object?>>;
				return docs?.Select(d => d.Metadata) ?? Enumerable.Empty<BlobItem>();
			}

			public TResult Execute<TResult>(Expression expression)
			{
				if (typeof(TResult).IsAssignableFrom(typeof(IEnumerable<BlobItem>)))
				{
					var docs = _inner.Execute<IEnumerable<BlobDocument<object?>>>(expression);
					var result = docs.Select(d => d.Metadata);
					return (TResult)result;
				}

				// For scalar operators (Count, Any, First, etc.), execute via the
				// inner provider and let it handle the expression tree evaluation.
				var enumerable = (IEnumerable<BlobItem>)Execute(expression);
				var queryable = enumerable.AsQueryable();

				// Rebuild the expression tree with our materialized queryable as the source
				var originalSource = ExpressionTreeHelpers.FindQueryableSource(expression);
				if (originalSource == null)
				{
					throw new BlinqQueryException("Could not locate queryable source in expression tree for scalar execution.");
				}

				var rewritten = new SourceReplacingVisitor(originalSource, Expression.Constant(queryable)).Visit(expression);
				try
				{
					return (TResult)Expression.Lambda(rewritten).Compile().DynamicInvoke()!;
				}
				catch (TargetInvocationException tie) when (tie.InnerException != null)
				{
					ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
					throw; // unreachable
				}
			}

		}
	}

	/// <summary>
	/// Replaces a specific <see cref="IQueryable"/> constant in an expression tree with a new source,
	/// matched by reference identity.
	/// </summary>
	internal sealed class SourceReplacingVisitor : ExpressionVisitor
	{
		private readonly IQueryable _originalSource;
		private readonly ConstantExpression _replacement;

		public SourceReplacingVisitor(IQueryable originalSource, ConstantExpression replacement)
		{
			_originalSource = originalSource;
			_replacement = replacement;
		}

		protected override Expression VisitConstant(ConstantExpression node)
		{
			if (node.Value is IQueryable queryable && ReferenceEquals(queryable, _originalSource))
			{
				return _replacement;
			}

			return base.VisitConstant(node);
		}
	}
}
