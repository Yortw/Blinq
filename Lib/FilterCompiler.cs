using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Azure.Storage.Blobs.Models;

namespace Blinq
{
	/// <summary>
	/// Compiles metadata and content filter predicates from LINQ expression trees.
	/// </summary>
	internal static class FilterCompiler
	{
		internal static Func<BlobItem, bool> CompileMetadataFilter(Expression expression)
		{
			var whereCalls = ExpressionTreeHelpers.FindAllMethodCalls(expression, "Where");
			Expression? combined = null;
			ParameterExpression? metadataParam = null;

			foreach (var whereCall in whereCalls)
			{
				if (whereCall.Arguments.Count != 2)
				{
					continue;
				}

				if (ExpressionTreeHelpers.StripQuote(whereCall.Arguments[1]) is LambdaExpression lambda)
				{
					metadataParam ??= Expression.Parameter(typeof(BlobItem), "metadata");
					var body = new MetadataAccessRewriter(lambda.Parameters[0], metadataParam).Visit(lambda.Body);

					if (!ExpressionTreeHelpers.IsConstantTrue(body))
					{
						combined = combined == null
							? body
							: Expression.AndAlso(combined, body);
					}
				}
			}

			if (combined != null && metadataParam != null)
			{
				var metadataLambda = Expression.Lambda<Func<BlobItem, bool>>(combined, metadataParam);
				return metadataLambda.Compile();
			}

			return _ => true;
		}

		internal static Func<T, BlobItem, bool> CompileContentFilter<T>(Expression expression)
		{
			var whereCalls = ExpressionTreeHelpers.FindAllMethodCalls(expression, "Where");
			var contentPredicates = new List<Expression<Func<BlobDocument<T>, bool>>>();

			foreach (var whereCall in whereCalls)
			{
				if (whereCall.Arguments.Count != 2)
				{
					continue;
				}

				if (ExpressionTreeHelpers.StripQuote(whereCall.Arguments[1]) is LambdaExpression lambda)
				{
					// Classify each conjunct: skip blob-name-only and metadata-only parts
					var contentParts = ExtractContentConjuncts(lambda);
					if (contentParts.Count > 0)
					{
						var combinedBody = contentParts.Aggregate((Expression)Expression.Constant(true),
							(acc, next) => Expression.AndAlso(acc, next));

						// Simplify leading true &&
						if (combinedBody is BinaryExpression bin
							&& bin.NodeType == ExpressionType.AndAlso
							&& ExpressionTreeHelpers.IsConstantTrue(bin.Left))
						{
							combinedBody = bin.Right;
						}

						var contentLambda = Expression.Lambda<Func<BlobDocument<T>, bool>>(combinedBody, lambda.Parameters[0]);
						contentPredicates.Add(contentLambda);
					}
				}
			}

			if (contentPredicates.Count == 0)
			{
				return (_, _) => true;
			}

			var compiledPredicates = contentPredicates.Select(p => p.Compile()).ToList();
			return (content, metadata) =>
			{
				var doc = new BlobDocument<T>(metadata.Name, metadata, content);
				foreach (var predicate in compiledPredicates)
				{
					if (!predicate(doc))
					{
						return false;
					}
				}
				return true;
			};
		}

		/// <summary>
		/// Splits a lambda body into AND-ed conjuncts and returns only those that reference Content.
		/// Blob-name-only and metadata-only conjuncts are dropped.
		/// </summary>
		private static List<Expression> ExtractContentConjuncts(LambdaExpression lambda)
		{
			var conjuncts = new List<Expression>();
			FlattenAndAlso(lambda.Body, conjuncts);

			var contentParts = new List<Expression>();
			foreach (var conjunct in conjuncts)
			{
				// Skip blob-name-only predicates (handled by BlobNamePrefixExtractor)
				if (ExpressionTreeHelpers.ReferencesBlobNameOnly(conjunct))
				{
					continue;
				}

				// Keep predicates that reference Content (or Content + anything else)
				if (ReferencesContent(conjunct))
				{
					contentParts.Add(conjunct);
				}
				// Metadata-only predicates are dropped (handled by CompileMetadataFilter)
			}

			return contentParts;
		}

		private static void FlattenAndAlso(Expression expr, List<Expression> results)
		{
			if (expr is BinaryExpression bin && bin.NodeType == ExpressionType.AndAlso)
			{
				FlattenAndAlso(bin.Left, results);
				FlattenAndAlso(bin.Right, results);
			}
			else
			{
				results.Add(expr);
			}
		}

		private static bool ReferencesContent(Expression? expr)
		{
			if (expr == null)
			{
				return false;
			}

			if (expr is MemberExpression memberExpr)
			{
				if (memberExpr.Member.Name == "Content")
				{
					return true;
				}

				return ReferencesContent(memberExpr.Expression);
			}

			if (expr is MethodCallExpression mce)
			{
				return ReferencesContent(mce.Object) || mce.Arguments.Any(ReferencesContent);
			}

			if (expr is BinaryExpression binExpr)
			{
				return ReferencesContent(binExpr.Left) || ReferencesContent(binExpr.Right);
			}

			if (expr is UnaryExpression ue)
			{
				return ReferencesContent(ue.Operand);
			}

			if (expr is ConditionalExpression cond)
			{
				return ReferencesContent(cond.Test) || ReferencesContent(cond.IfTrue) || ReferencesContent(cond.IfFalse);
			}

			return false;
		}

		internal sealed class MetadataAccessRewriter : ExpressionVisitor
		{
			private readonly ParameterExpression _oldParam;
			private readonly ParameterExpression _newParam;

			public MetadataAccessRewriter(ParameterExpression oldParam, ParameterExpression newParam)
			{
				_oldParam = oldParam;
				_newParam = newParam;
			}

			protected override Expression VisitParameter(ParameterExpression node)
			{
				return node == _oldParam ? _newParam : base.VisitParameter(node);
			}

			protected override Expression VisitMember(MemberExpression node)
			{
				if (IsMetadataChain(node))
				{
					return RebuildFromMetadata(node);
				}

				return base.VisitMember(node);
			}

			protected override Expression VisitBinary(BinaryExpression node)
			{
				bool leftIsNonMetadata = IsNonMetadataExpression(node.Left);
				bool rightIsNonMetadata = IsNonMetadataExpression(node.Right);

				if (node.NodeType == ExpressionType.AndAlso || node.NodeType == ExpressionType.OrElse)
				{
					var left = Visit(node.Left);
					var right = Visit(node.Right);

					if (node.NodeType == ExpressionType.AndAlso)
					{
						if (ExpressionTreeHelpers.IsConstantTrue(left))
						{
							return right;
						}

						if (ExpressionTreeHelpers.IsConstantTrue(right))
						{
							return left;
						}

						if (left.Type == typeof(bool) && right.Type == typeof(bool))
						{
							return Expression.AndAlso(left, right);
						}

						return Expression.Constant(true);
					}
					else // OrElse
					{
						if (ExpressionTreeHelpers.IsConstantTrue(left) || ExpressionTreeHelpers.IsConstantTrue(right))
						{
							return Expression.Constant(true);
						}

						if (left.Type == typeof(bool) && right.Type == typeof(bool))
						{
							return Expression.OrElse(left, right);
						}

						return Expression.Constant(true);
					}
				}
				else
				{
					if (leftIsNonMetadata || rightIsNonMetadata)
					{
						return Expression.Constant(true);
					}

					var left = Visit(node.Left);
					var right = Visit(node.Right);

					return Expression.MakeBinary(node.NodeType, left, right);
				}
			}

			private bool IsNonMetadataExpression(Expression expr)
			{
				if (expr is ConstantExpression)
				{
					return false;
				}

				if (expr is MemberExpression memberExpr)
				{
					var current = memberExpr;
					while (current != null)
					{
						if (current.Expression == _oldParam)
						{
							if (current.Member.Name == "Content" || current.Member.Name == "BlobName")
							{
								return true;
							}

							if (current.Member.Name == "Metadata")
							{
								return false;
							}

							// BlobDocument<T> has Content, BlobName, and Metadata.
							// Any other member off BlobDocument<T> is non-metadata.
							// But for BlobItem parameters (metadata-only path), ALL
							// properties (Name, Properties, etc.) are metadata-accessible.
							return _oldParam.Type.IsGenericType
								&& _oldParam.Type.GetGenericTypeDefinition() == typeof(BlobDocument<>);
						}
						current = current.Expression as MemberExpression;
					}

					return false;
				}

				if (expr is MethodCallExpression mce)
				{
					if (mce.Object != null)
					{
						return IsNonMetadataExpression(mce.Object);
					}
				}

				if (expr is UnaryExpression unary)
				{
					return IsNonMetadataExpression(unary.Operand);
				}

				return false;
			}

			protected override Expression VisitMethodCall(MethodCallExpression node)
			{
				if (node.Object != null)
				{
					if (IsNonMetadataExpression(node.Object))
					{
						return Expression.Constant(true);
					}

					return base.VisitMethodCall(node);
				}

				// Static method call (e.g. string.IsNullOrEmpty) — check arguments
				if (node.Arguments.Any(IsNonMetadataExpression))
				{
					return Expression.Constant(true);
				}

				return base.VisitMethodCall(node);
			}

			private bool IsMetadataChain(MemberExpression? node)
			{
				while (node != null)
				{
					if (node.Member.Name == "Metadata" && node.Expression == _oldParam)
					{
						// Only match BlobDocument<T>.Metadata (which returns BlobItem).
						// BlobItem.Metadata (returns IDictionary) is a regular property access.
						return node.Type == typeof(BlobItem);
					}

					node = node.Expression as MemberExpression;
				}

				return false;
			}

			private Expression RebuildFromMetadata(MemberExpression? node)
			{
				var members = new Stack<string>();
				while (node != null)
				{
					if (node.Member.Name == "Metadata" && node.Expression == _oldParam)
					{
						break;
					}

					members.Push(node.Member.Name);
					node = node.Expression as MemberExpression;
				}

				Expression expr = _newParam;
				while (members.Count > 0)
				{
					expr = Expression.PropertyOrField(expr, members.Pop());
				}

				return expr;
			}
		}
	}
}
