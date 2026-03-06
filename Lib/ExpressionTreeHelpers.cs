using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Blinq
{
	/// <summary>
	/// Pure helper methods for analyzing LINQ expression trees.
	/// </summary>
	internal static class ExpressionTreeHelpers
	{
		/// <summary>
		/// Finds the first <see cref="MethodCallExpression"/> with the given method name
		/// by walking the <c>Arguments[0]</c> chain of a LINQ expression tree.
		/// </summary>
		internal static MethodCallExpression? FindMethodCall(Expression expression, string methodName)
		{
			while (expression is MethodCallExpression mce)
			{
				if (mce.Method.Name == methodName)
				{
					return mce;
				}

				expression = mce.Arguments[0];
			}

			return null;
		}

		/// <summary>
		/// Finds all <see cref="MethodCallExpression"/> nodes with the given method name
		/// by walking the full <c>Arguments[0]</c> chain of a LINQ expression tree.
		/// Returns them in tree order (outermost first).
		/// </summary>
		internal static List<MethodCallExpression> FindAllMethodCalls(Expression expression, string methodName)
		{
			var results = new List<MethodCallExpression>();
			while (expression is MethodCallExpression mce)
			{
				if (mce.Method.Name == methodName)
				{
					results.Add(mce);
				}

				expression = mce.Arguments[0];
			}

			return results;
		}

		/// <summary>
		/// Strips a <see cref="ExpressionType.Quote"/> wrapper if present, returning the inner operand.
		/// </summary>
		internal static Expression StripQuote(Expression e) =>
			e is UnaryExpression ue && ue.NodeType == ExpressionType.Quote ? ue.Operand : e;

		/// <summary>
		/// Returns <c>true</c> if the expression references the <c>BlobName</c> property anywhere in its tree.
		/// </summary>
		internal static bool ReferencesBlobName(Expression? expr)
		{
			if (expr == null)
			{
				return false;
			}

			if (expr is MemberExpression memberExpr && memberExpr.Member.Name == "BlobName")
			{
				return true;
			}

			if (expr is MethodCallExpression mce)
			{
				return ReferencesBlobName(mce.Object) || mce.Arguments.Any(ReferencesBlobName);
			}

			if (expr is BinaryExpression binExpr)
			{
				return ReferencesBlobName(binExpr.Left) || ReferencesBlobName(binExpr.Right);
			}

			if (expr is UnaryExpression ue)
			{
				return ReferencesBlobName(ue.Operand);
			}

			return false;
		}

		/// <summary>
		/// Returns <c>true</c> if the expression references <c>BlobName</c> but does NOT reference <c>Content</c> or <c>Metadata</c>.
		/// </summary>
		internal static bool ReferencesBlobNameOnly(Expression expr)
		{
			if (!ReferencesBlobName(expr))
			{
				return false;
			}

			if (ReferencesContentOrMetadata(expr))
			{
				return false;
			}

			return true;
		}

		/// <summary>
		/// Returns <c>true</c> if the expression references <c>Content</c> or <c>Metadata</c> anywhere in its tree.
		/// </summary>
		internal static bool ReferencesContentOrMetadata(Expression? expr)
		{
			if (expr == null)
			{
				return false;
			}

			if (expr is MemberExpression memberExpr)
			{
				if (memberExpr.Member.Name == "Content" || memberExpr.Member.Name == "Metadata")
				{
					return true;
				}

				if (memberExpr.Expression != null)
				{
					return ReferencesContentOrMetadata(memberExpr.Expression);
				}
			}

			if (expr is MethodCallExpression mce)
			{
				return ReferencesContentOrMetadata(mce.Object) || mce.Arguments.Any(ReferencesContentOrMetadata);
			}

			if (expr is BinaryExpression binExpr)
			{
				return ReferencesContentOrMetadata(binExpr.Left) || ReferencesContentOrMetadata(binExpr.Right);
			}

			if (expr is UnaryExpression ue)
			{
				return ReferencesContentOrMetadata(ue.Operand);
			}

			return false;
		}

		/// <summary>
		/// Returns <c>true</c> if the expression is <c>Expression.Constant(true)</c>.
		/// </summary>
		internal static bool IsConstantTrue(Expression expr)
		{
			return expr is ConstantExpression ce && ce.Value is bool b && b;
		}

		/// <summary>
		/// Walks the expression tree to find the first <see cref="IQueryable"/> constant (the root source).
		/// </summary>
		internal static IQueryable? FindQueryableSource(Expression expression)
		{
			if (expression is ConstantExpression ce && ce.Value is IQueryable q)
			{
				return q;
			}

			if (expression is MethodCallExpression mce)
			{
				foreach (var arg in mce.Arguments)
				{
					var result = FindQueryableSource(arg);
					if (result != null)
					{
						return result;
					}
				}
			}

			return null;
		}
	}
}
