using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using Azure.Storage.Blobs;

namespace Blinq
{
	/// <summary>
	/// Provides LINQ query execution over Azure Blob Storage.
	/// </summary>
	internal sealed class BlobQueryProvider<T> : IBlobQueryProvider<T>
	{
		private readonly BlobContainerClient _containerClient;
		private readonly int _maxConcurrency;
		private readonly IBlobContentDeserializer? _deserializer;
		private readonly bool _metadataOnly;

		public BlobQueryProvider(BlobContainerClient containerClient, int maxConcurrency = 4, IBlobContentDeserializer? deserializer = null, bool metadataOnly = false)
		{
			if (maxConcurrency <= 0)
			{
				throw new ArgumentException("Max concurrency must be positive.", nameof(maxConcurrency));
			}

			_containerClient = containerClient ?? throw new ArgumentNullException(nameof(containerClient));
			_maxConcurrency = maxConcurrency;
			_deserializer = deserializer;
			_metadataOnly = metadataOnly;
		}

		public IQueryable CreateQuery(Expression expression)
		{
			if (expression == null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			return new BlobQueryable<T>(this, expression);
		}

		public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
		{
			if (typeof(TElement) != typeof(BlobDocument<T>))
			{
				throw new BlinqProjectionNotSupportedException(
					"Projections to types other than BlobDocument<T> are not supported in query construction. " +
					"Project to your desired type after materialization, e.g., after calling ToListAsync()."
				);
			}

			return (IQueryable<TElement>)new BlobQueryable<T>(this, expression);
		}

		public object Execute(Expression expression)
		{
			if (expression == null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			return BlobQueryExecutor.ExecuteSync<T>(_containerClient, expression, _maxConcurrency, _deserializer, _metadataOnly);
		}

		public IAsyncEnumerable<BlobDocument<T>> ExecuteAsync(Expression expression, CancellationToken cancellationToken = default)
		{
			return BlobQueryExecutor.ExecuteAsync<T>(_containerClient, expression, _maxConcurrency, _deserializer, _metadataOnly, cancellationToken);
		}

		public TResult Execute<TResult>(Expression expression)
		{
			if (typeof(TResult).IsAssignableFrom(typeof(IEnumerable<BlobDocument<T>>)))
			{
				return (TResult)BlobQueryExecutor.ExecuteSync<T>(_containerClient, expression, _maxConcurrency, _deserializer, _metadataOnly);
			}

			object Evaluate(Expression expr)
			{
				if (expr is ConstantExpression ce)
				{
					if (ce.Value is IQueryable<BlobDocument<T>> queryable)
					{
						return BlobQueryExecutor.ExecuteSync<T>(_containerClient, queryable.Expression, _maxConcurrency, _deserializer, _metadataOnly);
					}

					if (ce.Value == null)
					{
						return BlobQueryExecutor.ExecuteSync<T>(_containerClient, expression, _maxConcurrency, _deserializer, _metadataOnly);
					}

					return ce.Value;
				}

				if (expr is MethodCallExpression mce)
				{
					var method = mce.Method;
					var args = mce.Arguments.Select(Evaluate).ToArray();
					if (args.Length > 0 && method.DeclaringType == typeof(Queryable) && args[0] is IEnumerable<BlobDocument<T>> enumerable)
					{
						args[0] = enumerable.AsQueryable();
					}

					try
					{
						return method.Invoke(null, args);
					}
					catch (TargetInvocationException tie) when (tie.InnerException != null)
					{
						ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
						throw; // unreachable
					}
				}

				if (expr is ParameterExpression || expr is MemberExpression)
				{
					return BlobQueryExecutor.ExecuteSync<T>(_containerClient, expr, _maxConcurrency, _deserializer, _metadataOnly);
				}

				var lambda = Expression.Lambda(expr);
				return lambda.Compile().DynamicInvoke();
			}

			var result = Evaluate(expression);
			return (TResult)result;
		}
	}
}
