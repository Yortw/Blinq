using System;

namespace Blinq
{
	/// <summary>
	/// Base exception for Blinq query errors.
	/// Thrown for errors encountered during query construction, execution, or materialization in the Blinq LINQ provider.
	/// This exception is the base for all Blinq-specific query exceptions, allowing consumers to catch and handle Blinq query errors distinctly from other exceptions.
	/// </summary>
	public class BlinqQueryException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="BlinqQueryException"/> class with a specified error message.
		/// </summary>
		/// <param name="message">The error message that explains the reason for the exception.</param>
		public BlinqQueryException(string message) : base(message) { }

		/// <summary>
		/// Initializes a new instance of the <see cref="BlinqQueryException"/> class with a specified error message and inner exception.
		/// </summary>
		/// <param name="message">The error message that explains the reason for the exception.</param>
		/// <param name="innerException">The exception that is the cause of the current exception.</param>
		public BlinqQueryException(string message, Exception innerException) : base(message, innerException) { }

		internal const string ProjectionLimitationMessage =
			"Only filtering is supported in the query. " +
			"Project to your desired type after materialization, e.g., after calling ToListAsync().";
	}

	/// <summary>
	/// Exception thrown when a projection is attempted in query construction.
	/// Use projections (e.g., <c>select x.Content</c>) only after materialization (e.g., after <c>ToListAsync()</c>).
	/// </summary>
	public sealed class BlinqProjectionNotSupportedException : BlinqQueryException
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="BlinqProjectionNotSupportedException"/> class with a specified error message.
		/// </summary>
		/// <param name="message">The error message that explains the reason for the exception.</param>
		public BlinqProjectionNotSupportedException(string message) : base(message) { }
	}
}
