using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace Blinq
{
	/// <summary>
	/// Represents a LINQ queryable for Azure Blob Storage, returning <see cref="BlobDocument{T}"/> results.
	/// </summary>
	/// <typeparam name="T">The type to deserialize blob content to.</typeparam>
	public class BlobQueryable<T> : IQueryable<BlobDocument<T>>, IAsyncEnumerable<BlobDocument<T>>
	{
		/// <summary>
		/// Gets the LINQ expression tree associated with this query.
		/// </summary>
		public Expression Expression { get; }

		/// <summary>
		/// Gets the type of elements returned by the query.
		/// </summary>
		public Type ElementType => typeof(BlobDocument<T>);

		/// <summary>
		/// Gets the query provider that executes this query.
		/// </summary>
		public IQueryProvider Provider { get; }

		/// <summary>
		/// Initializes a new instance of <see cref="BlobQueryable{T}"/> with the specified provider and expression.
		/// </summary>
		/// <param name="provider">The query provider.</param>
		/// <param name="expression">The LINQ expression tree.</param>
		internal BlobQueryable(IQueryProvider provider, Expression expression)
		{
			Provider = provider ?? throw new ArgumentNullException(nameof(provider));
			Expression = expression ?? throw new ArgumentNullException(nameof(expression));
		}

		/// <summary>
		/// Returns an enumerator that iterates through the results of the query.
		/// </summary>
		public IEnumerator<BlobDocument<T>> GetEnumerator()
		{
			var result = Provider.Execute(Expression)
				?? throw new InvalidOperationException("Query execution returned null. This is unexpected.");
			return ((IEnumerable<BlobDocument<T>>)result).GetEnumerator();
		}

		/// <summary>
		/// Returns an enumerator that iterates through the results of the query.
		/// </summary>
		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

		/// <summary>
		/// Returns an async enumerator for asynchronous iteration over the query results.
		/// </summary>
		public IAsyncEnumerator<BlobDocument<T>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
		{
			if (Provider is BlobQueryProvider<T> asyncProvider)
			{
				return asyncProvider.ExecuteAsync(Expression, cancellationToken).GetAsyncEnumerator(cancellationToken);
			}
			throw new NotSupportedException("Async enumeration is only supported with BlobQueryProvider<T>.");
		}
	}
}
