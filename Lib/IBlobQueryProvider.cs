using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace Blinq
{
	/// <summary>
	/// Defines the interface for a LINQ query provider that executes queries over Azure Blob Storage and returns <see cref="BlobDocument{T}"/> results.
	/// </summary>
	/// <typeparam name="T">The type to deserialize blob content to.</typeparam>
	internal interface IBlobQueryProvider<T> : IQueryProvider
	{
		/// <summary>
		/// Asynchronously executes the specified LINQ expression and returns an async sequence of <see cref="BlobDocument{T}"/> results.
		/// </summary>
		/// <param name="expression">The LINQ expression tree representing the query.</param>
		/// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
		/// <returns>An async sequence of <see cref="BlobDocument{T}"/> results.</returns>
		IAsyncEnumerable<BlobDocument<T>> ExecuteAsync(Expression expression, CancellationToken cancellationToken = default);
	}
}
