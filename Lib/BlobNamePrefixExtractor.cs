using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Blinq
{
	/// <summary>
	/// Extracts blob name prefix (from <c>StartsWith</c>) and compiles blob-name predicates from a LINQ expression tree.
	/// </summary>
	internal static class BlobNamePrefixExtractor
	{
		internal static (string? prefix, Func<BlobDocument<T>, bool>? predicate) GetBlobNamePrefixAndPredicate<T>(Expression expression)
		{
			string? prefix = null;
			int startsWithCount = 0;
			List<Expression> blobNamePredicates = new();

			void FindWhereLambdas(Expression expr)
			{
				if (expr is MethodCallExpression mce)
				{
					if (mce.Method.Name == "Where" && mce.Arguments.Count == 2)
					{
						if (ExpressionTreeHelpers.StripQuote(mce.Arguments[1]) is LambdaExpression lambda)
						{
							var visitor = new RemoveBlobNameStartsWithVisitor();
							var withoutPrefix = visitor.Visit(lambda.Body);
							if (visitor.Prefix != null)
							{
								if (prefix != null && prefix != visitor.Prefix)
								{
									throw new BlinqQueryException(
										$"Conflicting BlobName.StartsWith prefixes: '{prefix}' and '{visitor.Prefix}'. Only one prefix is supported.");
								}
								prefix = visitor.Prefix;
							}
							startsWithCount += visitor.StartsWithCount;
							if (startsWithCount > 1)
							{
								throw new BlinqQueryException("Only one filter on BlobName.StartsWith is supported.");
							}

							blobNamePredicates.AddRange(ExtractBlobNamePredicates(withoutPrefix));
						}
					}
					FindWhereLambdas(mce.Arguments[0]);
				}
			}

			FindWhereLambdas(expression);

			Func<BlobDocument<T>, bool>? predicate = null;
			if (blobNamePredicates.Count > 0)
			{
				var param = Expression.Parameter(typeof(BlobDocument<T>), "doc");
				var rewrittenPredicates = blobNamePredicates
					.Select(expr => new ParameterReplaceVisitor(param).Visit(expr));
				var combined = rewrittenPredicates
					.Aggregate((Expression)Expression.Constant(true), (acc, next) => Expression.AndAlso(acc, next));
				var lambda = Expression.Lambda<Func<BlobDocument<T>, bool>>(combined, param);
				predicate = lambda.Compile();
			}

			return (prefix, predicate);
		}

		internal static List<Expression> ExtractBlobNamePredicates(Expression expr)
		{
			var result = new List<Expression>();
			void Visit(Expression e)
			{
				if (e is MethodCallExpression mce)
				{
					if (mce.Method.Name == "StartsWith" && mce.Object is MemberExpression member && member.Member.Name == "BlobName")
					{
						// skip, handled by prefix
					}
					else if (ExpressionTreeHelpers.ReferencesBlobNameOnly(mce))
					{
						result.Add(e);
					}
				}
				else if (e is BinaryExpression bin)
				{
					if (bin.NodeType == ExpressionType.AndAlso || bin.NodeType == ExpressionType.OrElse)
					{
						Visit(bin.Left);
						Visit(bin.Right);
					}
					else if (ExpressionTreeHelpers.ReferencesBlobNameOnly(e))
					{
						result.Add(e);
					}
				}
				else if (e is MemberExpression || e is UnaryExpression)
				{
					if (ExpressionTreeHelpers.ReferencesBlobNameOnly(e))
					{
						result.Add(e);
					}
				}
			}
			Visit(expr);
			return result;
		}

		internal sealed class RemoveBlobNameStartsWithVisitor : ExpressionVisitor
		{
			public string? Prefix { get; private set; }
			public int StartsWithCount { get; private set; }
			private readonly string _blobNameProperty = "BlobName";
			private readonly string _startsWithMethod = "StartsWith";

			protected override Expression VisitMethodCall(MethodCallExpression node)
			{
				if (node.Method.Name == _startsWithMethod &&
					node.Object is MemberExpression member &&
					member.Member.Name == _blobNameProperty &&
					node.Arguments[0] is ConstantExpression constExpr)
				{
					if (constExpr.Value == null)
					{
						throw new BlinqQueryException("BlobName.StartsWith argument must not be null.");
					}

					StartsWithCount++;
					Prefix = (string)constExpr.Value;
					return Expression.Constant(true);
				}

				return base.VisitMethodCall(node);
			}
		}

		internal sealed class ParameterReplaceVisitor : ExpressionVisitor
		{
			private readonly ParameterExpression _newParam;
			public ParameterReplaceVisitor(ParameterExpression newParam) => _newParam = newParam;

			protected override Expression VisitParameter(ParameterExpression node) => _newParam;

			protected override Expression VisitMember(MemberExpression node)
			{
				if (node.Expression == null)
				{
					// Static member access (e.g. string.Empty, MyConfig.Prefix) — preserve as-is
					return base.VisitMember(node);
				}

				var expr = Visit(node.Expression);
				return Expression.PropertyOrField(expr, node.Member.Name);
			}

			protected override Expression VisitMethodCall(MethodCallExpression node)
			{
				var args = node.Arguments.Select(Visit).ToArray();

				// Static method calls (e.g. string.IsNullOrEmpty) have null Object
				if (node.Object == null)
				{
					return Expression.Call(node.Method, args);
				}

				var obj = Visit(node.Object);

				if (node.Method.Name == "EndsWith" && obj.Type == typeof(string) && args.Length == 1 && args[0].Type == typeof(string))
				{
					var method = typeof(string).GetMethod("EndsWith", new[] { typeof(string) });
					return Expression.Call(obj, method, args);
				}

				return Expression.Call(obj, node.Method, args);
			}
		}
	}
}
