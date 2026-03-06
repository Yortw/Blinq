using System;
using System.Linq;
using System.Linq.Expressions;
using Azure.Storage.Blobs.Models;
using Blinq;
using Xunit;

namespace Blinq.Tests
{
	public class ExpressionAnalysisTests
	{
		/// <summary>
		/// Builds a LINQ expression tree from a queryable builder function.
		/// This produces the same expression tree structure that BlobQueryProvider would receive.
		/// </summary>
		private static Expression BuildExpr<T>(Func<IQueryable<BlobDocument<T>>, IQueryable<BlobDocument<T>>> qb)
			=> qb(Array.Empty<BlobDocument<T>>().AsQueryable()).Expression;

		/// <summary>
		/// Builds a LINQ expression tree from a BlobItem queryable builder function.
		/// Simulates the metadata-only path where the lambda parameter is BlobItem, not BlobDocument.
		/// </summary>
		private static Expression BuildBlobItemExpr(Func<IQueryable<BlobItem>, IQueryable<BlobItem>> qb)
			=> qb(Array.Empty<BlobItem>().AsQueryable()).Expression;

		#region FindAllMethodCalls

		[Fact]
		[Trait("Category", "Unit")]
		public void FindAllMethodCalls_NoWhere_ReturnsEmpty()
		{
			var expr = BuildExpr<string>(q => q);
			var result = ExpressionTreeHelpers.FindAllMethodCalls(expr, "Where");
			Assert.Empty(result);
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void FindAllMethodCalls_SingleWhere_ReturnsOne()
		{
			var expr = BuildExpr<string>(q => q.Where(d => d.BlobName == "x"));
			var result = ExpressionTreeHelpers.FindAllMethodCalls(expr, "Where");
			Assert.Single(result);
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void FindAllMethodCalls_TwoWheres_ReturnsBoth()
		{
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName == "a")
				 .Where(d => d.BlobName == "b"));
			var result = ExpressionTreeHelpers.FindAllMethodCalls(expr, "Where");
			Assert.Equal(2, result.Count);
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void FindAllMethodCalls_ThreeWheres_ReturnsAll()
		{
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName == "a")
				 .Where(d => d.BlobName == "b")
				 .Where(d => d.BlobName == "c"));
			var result = ExpressionTreeHelpers.FindAllMethodCalls(expr, "Where");
			Assert.Equal(3, result.Count);
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void FindAllMethodCalls_MixedMethods_OnlyFindsTarget()
		{
			// Where + Select + Where — but Select changes the type, so we can't chain
			// Instead test Where + OrderBy + Where pattern (which preserves type for BlobDocument)
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName == "a")
				 .Take(10)
				 .Where(d => d.BlobName == "b"));
			var result = ExpressionTreeHelpers.FindAllMethodCalls(expr, "Where");
			Assert.Equal(2, result.Count);
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void FindMethodCall_ReturnsFirstMatch()
		{
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName == "a")
				 .Where(d => d.BlobName == "b"));
			var result = ExpressionTreeHelpers.FindMethodCall(expr, "Where");
			Assert.NotNull(result);
			// The first match found is the outermost Where (the second .Where in code)
			// because the tree is structured as Where(Where(source, a), b)
			var lambda = ExpressionTreeHelpers.StripQuote(result.Arguments[1]) as LambdaExpression;
			Assert.NotNull(lambda);
			var binary = lambda.Body as BinaryExpression;
			Assert.NotNull(binary);
			var constant = binary.Right as ConstantExpression;
			Assert.Equal("b", constant?.Value);
		}

		#endregion

		#region GetBlobNamePrefixAndPredicate

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_SingleStartsWith_ExtractsPrefix()
		{
			var expr = BuildExpr<string>(q => q.Where(d => d.BlobName.StartsWith("folder/")));
			var (prefix, predicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr);
			Assert.Equal("folder/", prefix);
			Assert.Null(predicate); // no other blob-name conditions
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_NoStartsWith_ReturnsNullPrefix()
		{
			var expr = BuildExpr<string>(q => q.Where(d => d.BlobName.EndsWith(".json")));
			var (prefix, predicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr);
			Assert.Null(prefix);
			Assert.NotNull(predicate); // EndsWith becomes a blob-name predicate
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_StartsWith_PlusEndsWith()
		{
			var expr = BuildExpr<string>(q => q.Where(d => d.BlobName.StartsWith("prefix/") && d.BlobName.EndsWith(".json")));
			var (prefix, predicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr);
			Assert.Equal("prefix/", prefix);
			Assert.NotNull(predicate); // EndsWith remains as a predicate
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_StartsWithInFirstWhereOnly_BugRegression()
		{
			// Bug 4 regression: if StartsWith is in the first Where and a second Where
			// has no StartsWith, the prefix should NOT be overwritten to null
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName.StartsWith("data/"))
				 .Where(d => d.BlobName.EndsWith(".csv")));
			var (prefix, predicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr);
			Assert.Equal("data/", prefix);
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_ConflictingPrefixes_Throws()
		{
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName.StartsWith("a/"))
				 .Where(d => d.BlobName.StartsWith("b/")));
			Assert.Throws<BlinqQueryException>(() =>
				BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_SamePrefixInTwoWheres_Throws()
		{
			// Even the same prefix twice means 2 StartsWith calls, which we disallow
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName.StartsWith("same/"))
				 .Where(d => d.BlobName.StartsWith("same/")));
			Assert.Throws<BlinqQueryException>(() =>
				BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_StaticMemberInPredicate_DoesNotCrash()
		{
			// Bug: ParameterReplaceVisitor.VisitMember passed null to
			// Expression.PropertyOrField for static member accesses (node.Expression == null).
			// This crashed with ArgumentNullException.
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName == string.Empty));
			var (prefix, predicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr);

			Assert.Null(prefix);
			Assert.NotNull(predicate);

			var matchBlob = BlobsModelFactory.BlobItem("", false, BlobsModelFactory.BlobItemProperties(false));
			var noMatchBlob = BlobsModelFactory.BlobItem("notempty", false, BlobsModelFactory.BlobItemProperties(false));

			Assert.True(predicate(new BlobDocument<string>("", matchBlob, "")));
			Assert.False(predicate(new BlobDocument<string>("notempty", noMatchBlob, "")));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_StartsWithNull_Throws()
		{
			// Bug: StartsWith(null) was silently erased (constExpr.Value as string returns null),
			// causing the predicate to be dropped. The query would return all blobs
			// instead of throwing. String.StartsWith(null) throws ArgumentNullException,
			// so Blinq should fail at query time rather than silently return wrong results.
			// Use a literal null to produce a ConstantExpression (not a closure capture).
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type
			var expr = BuildExpr<string>(q => q.Where(d => d.BlobName.StartsWith(null)));
#pragma warning restore CS8625
			Assert.Throws<BlinqQueryException>(() =>
				BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_NoWhereAtAll_ReturnsNulls()
		{
			var expr = BuildExpr<string>(q => q);
			var (prefix, predicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr);
			Assert.Null(prefix);
			Assert.Null(predicate);
		}

		#endregion

		#region CompileMetadataFilter

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_MetadataOnlyCondition_FiltersCorrectly()
		{
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Metadata.Properties.ContentLength > 100));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var bigBlob = BlobsModelFactory.BlobItem("big", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 200));
			var smallBlob = BlobsModelFactory.BlobItem("small", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 50));

			Assert.True(filter(bigBlob));
			Assert.False(filter(smallBlob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_ContentOnlyCondition_ReturnsTrue()
		{
			// Content-only predicates should be rewritten to 'true' by MetadataAccessRewriter
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Content == "hello"));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var blob = BlobsModelFactory.BlobItem("any", false, BlobsModelFactory.BlobItemProperties(false));
			Assert.True(filter(blob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_MixedCondition_OnlyAppliesMetadataPart()
		{
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Metadata.Properties.ContentLength > 100 && d.Content == "hello"));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var bigBlob = BlobsModelFactory.BlobItem("big", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 200));
			var smallBlob = BlobsModelFactory.BlobItem("small", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 50));

			Assert.True(filter(bigBlob));   // metadata part passes
			Assert.False(filter(smallBlob)); // metadata part fails
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_TwoWheresWithMetadata_BothApplied_BugRegression()
		{
			// Bug 1+2 regression: both Where clauses should be processed
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Metadata.Properties.ContentLength > 100)
				 .Where(d => d.Metadata.Properties.ContentLength < 500));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var tooSmall = BlobsModelFactory.BlobItem("small", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 50));
			var justRight = BlobsModelFactory.BlobItem("mid", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 200));
			var tooBig = BlobsModelFactory.BlobItem("big", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 1000));

			Assert.False(filter(tooSmall));
			Assert.True(filter(justRight));
			Assert.False(filter(tooBig));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_NoWhere_ReturnsAlwaysTrue()
		{
			var expr = BuildExpr<string>(q => q);
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var blob = BlobsModelFactory.BlobItem("any", false, BlobsModelFactory.BlobItemProperties(false));
			Assert.True(filter(blob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_ClosureCapturedVariable_FilterStillWorks()
		{
			// Bug regression: closure-captured variables on the RHS of comparisons
			// were replaced with Expression.Constant(true) by MetadataAccessRewriter,
			// causing a type mismatch (string == bool) crash.
			long threshold = 100;
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Metadata.Properties.ContentLength > threshold));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var bigBlob = BlobsModelFactory.BlobItem("big", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 200));
			var smallBlob = BlobsModelFactory.BlobItem("small", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 50));

			Assert.True(filter(bigBlob));
			Assert.False(filter(smallBlob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_BlobItemProperties_NotSilentlyDropped()
		{
			// Bug: IsNonMetadataExpression treated BlobItem.Properties as "non-metadata"
			// because "Properties" isn't in the recognised list (Content/BlobName/Metadata).
			// This silently dropped the filter, returning all blobs unfiltered.
			var expr = BuildBlobItemExpr(q =>
				q.Where(x => x.Properties.ContentType == "application/json"));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var match = BlobsModelFactory.BlobItem("a", false,
				BlobsModelFactory.BlobItemProperties(false, contentType: "application/json"));
			var noMatch = BlobsModelFactory.BlobItem("b", false,
				BlobsModelFactory.BlobItemProperties(false, contentType: "text/plain"));

			Assert.True(filter(match));
			Assert.False(filter(noMatch));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_BlobItemName_NotSilentlyDropped()
		{
			// Same bug: BlobItem.Name is a direct property, not "BlobName".
			var expr = BuildBlobItemExpr(q =>
				q.Where(x => x.Name == "specific.json"));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var match = BlobsModelFactory.BlobItem("specific.json", false,
				BlobsModelFactory.BlobItemProperties(false));
			var noMatch = BlobsModelFactory.BlobItem("other.json", false,
				BlobsModelFactory.BlobItemProperties(false));

			Assert.True(filter(match));
			Assert.False(filter(noMatch));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_BlobItemMetadataDictionary_ContainsKey()
		{
			// Bug regression: on the metadata-only path, the lambda parameter is BlobItem
			// (not BlobDocument<T>). BlobItem.Metadata is the user-defined dictionary,
			// not BlobDocument.Metadata (which is a BlobItem). The rewriter incorrectly
			// treated BlobItem.Metadata as the same kind of metadata chain, stripping it
			// down to just the BlobItem, causing a type mismatch when ContainsKey was called.
			var expr = BuildBlobItemExpr(q =>
				q.Where(x => x.Metadata.ContainsKey("category")));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var withTag = BlobsModelFactory.BlobItem(
				"tagged", false, BlobsModelFactory.BlobItemProperties(false),
				metadata: new System.Collections.Generic.Dictionary<string, string> { ["category"] = "test" });
			var withoutTag = BlobsModelFactory.BlobItem(
				"untagged", false, BlobsModelFactory.BlobItemProperties(false),
				metadata: new System.Collections.Generic.Dictionary<string, string>());

			Assert.True(filter(withTag));
			Assert.False(filter(withoutTag));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_BlobItemMetadataDictionary_ValueEquals()
		{
			// Same bug: BlobItem.Metadata["key"] == "value" should work
			var expr = BuildBlobItemExpr(q =>
				q.Where(x => x.Metadata["category"] == "test"));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var match = BlobsModelFactory.BlobItem(
				"match", false, BlobsModelFactory.BlobItemProperties(false),
				metadata: new System.Collections.Generic.Dictionary<string, string> { ["category"] = "test" });
			var noMatch = BlobsModelFactory.BlobItem(
				"nomatch", false, BlobsModelFactory.BlobItemProperties(false),
				metadata: new System.Collections.Generic.Dictionary<string, string> { ["category"] = "other" });

			Assert.True(filter(match));
			Assert.False(filter(noMatch));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_StaticMethodCall_IsPreserved()
		{
			// Bug: static method calls (node.Object == null) like string.IsNullOrEmpty
			// were unconditionally replaced with Constant(true), silently dropping the filter.
			var expr = BuildExpr<string>(q =>
				q.Where(d => !string.IsNullOrEmpty(d.Metadata.Properties.ContentType)));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var withType = BlobsModelFactory.BlobItem("a", false,
				BlobsModelFactory.BlobItemProperties(false, contentType: "application/json"));
			var withoutType = BlobsModelFactory.BlobItem("b", false,
				BlobsModelFactory.BlobItemProperties(false, contentType: null));

			Assert.True(filter(withType));
			Assert.False(filter(withoutType));
		}

		#endregion

		#region CompileContentFilter

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileContentFilter_ContentCondition_FiltersCorrectly()
		{
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Content == "hello"));
			var filter = FilterCompiler.CompileContentFilter<string>(expr);

			var matchBlob = BlobsModelFactory.BlobItem("match", false, BlobsModelFactory.BlobItemProperties(false));
			var noMatchBlob = BlobsModelFactory.BlobItem("nomatch", false, BlobsModelFactory.BlobItemProperties(false));

			Assert.True(filter("hello", matchBlob));
			Assert.False(filter("world", noMatchBlob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileContentFilter_TwoWheresWithContent_BothApplied_BugRegression()
		{
			// Bug 1+2 regression: both Where clauses should be processed
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Content!.StartsWith("hello"))
				 .Where(d => d.Content!.EndsWith("world")));
			var filter = FilterCompiler.CompileContentFilter<string>(expr);

			var blob = BlobsModelFactory.BlobItem("any", false, BlobsModelFactory.BlobItemProperties(false));

			Assert.True(filter("hello world", blob));
			Assert.False(filter("hello there", blob));
			Assert.False(filter("goodbye world", blob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileContentFilter_MetadataOnlyWhere_ReturnsAlwaysTrue()
		{
			// Metadata-only predicates should be skipped by the content filter
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Metadata.Properties.ContentLength > 100));
			var filter = FilterCompiler.CompileContentFilter<string>(expr);

			var blob = BlobsModelFactory.BlobItem("any", false, BlobsModelFactory.BlobItemProperties(false));
			Assert.True(filter("anything", blob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileContentFilter_BlobNameOnlyWhere_ReturnsAlwaysTrue()
		{
			// BlobName-only predicates should be skipped by the content filter
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName.EndsWith(".json")));
			var filter = FilterCompiler.CompileContentFilter<string>(expr);

			var blob = BlobsModelFactory.BlobItem("any", false, BlobsModelFactory.BlobItemProperties(false));
			Assert.True(filter("anything", blob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileContentFilter_ConditionalExpression_ContentPredicateNotDropped()
		{
			// Bug: ReferencesContent didn't walk ConditionalExpression branches,
			// so a ternary referencing Content was silently dropped from the content filter.
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.BlobName.EndsWith(".json") ? d.Content == "match" : false));
			var filter = FilterCompiler.CompileContentFilter<string>(expr);

			var blob = BlobsModelFactory.BlobItem("test.json", false, BlobsModelFactory.BlobItemProperties(false));
			Assert.True(filter("match", blob));
			Assert.False(filter("nomatch", blob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileContentFilter_NoWhere_ReturnsAlwaysTrue()
		{
			var expr = BuildExpr<string>(q => q);
			var filter = FilterCompiler.CompileContentFilter<string>(expr);

			var blob = BlobsModelFactory.BlobItem("any", false, BlobsModelFactory.BlobItemProperties(false));
			Assert.True(filter("anything", blob));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileContentFilter_MixedContentAndMetadata_OnlyAppliesContentPart()
		{
			// Bug 3: the content filter should NOT re-evaluate metadata or blob-name parts
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Content == "match" && d.Metadata.Properties.ContentLength > 100));
			var filter = FilterCompiler.CompileContentFilter<string>(expr);

			var blob = BlobsModelFactory.BlobItem("any", false, BlobsModelFactory.BlobItemProperties(false));

			// Should only test the Content == "match" part
			Assert.True(filter("match", blob));
			Assert.False(filter("nomatch", blob));
		}

		#endregion

		#region ExpressionTreeHelpers misc

		[Fact]
		[Trait("Category", "Unit")]
		public void StripQuote_UnquotedExpression_ReturnsSameExpression()
		{
			var expr = Expression.Constant(42);
			Assert.Same(expr, ExpressionTreeHelpers.StripQuote(expr));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void IsConstantTrue_TrueConstant_ReturnsTrue()
		{
			Assert.True(ExpressionTreeHelpers.IsConstantTrue(Expression.Constant(true)));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void IsConstantTrue_FalseConstant_ReturnsFalse()
		{
			Assert.False(ExpressionTreeHelpers.IsConstantTrue(Expression.Constant(false)));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void IsConstantTrue_NonBoolConstant_ReturnsFalse()
		{
			Assert.False(ExpressionTreeHelpers.IsConstantTrue(Expression.Constant(42)));
		}

		#endregion

		#region ReferencesBlobName / ReferencesContentOrMetadata classification

		[Fact]
		[Trait("Category", "Unit")]
		public void ReferencesBlobName_BlobNameExpression_ReturnsTrue()
		{
			Expression<Func<BlobDocument<string>, bool>> lambda = d => d.BlobName == "test";
			Assert.True(ExpressionTreeHelpers.ReferencesBlobName(lambda.Body));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void ReferencesBlobName_ContentExpression_ReturnsFalse()
		{
			Expression<Func<BlobDocument<string>, bool>> lambda = d => d.Content == "test";
			Assert.False(ExpressionTreeHelpers.ReferencesBlobName(lambda.Body));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void ReferencesBlobName_Null_ReturnsFalse()
		{
			Assert.False(ExpressionTreeHelpers.ReferencesBlobName(null));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void ReferencesBlobNameOnly_BlobNamePlusContent_ReturnsFalse()
		{
			// References both BlobName and Content — not "BlobName only"
			Expression<Func<BlobDocument<string>, bool>> lambda = d => d.BlobName == "x" && d.Content == "y";
			Assert.False(ExpressionTreeHelpers.ReferencesBlobNameOnly(lambda.Body));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void ReferencesContentOrMetadata_ContentExpression_ReturnsTrue()
		{
			Expression<Func<BlobDocument<string>, bool>> lambda = d => d.Content == "hello";
			Assert.True(ExpressionTreeHelpers.ReferencesContentOrMetadata(lambda.Body));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void ReferencesContentOrMetadata_MetadataExpression_ReturnsTrue()
		{
			Expression<Func<BlobDocument<string>, bool>> lambda = d => d.Metadata.Properties.ContentLength > 0;
			Assert.True(ExpressionTreeHelpers.ReferencesContentOrMetadata(lambda.Body));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void ReferencesContentOrMetadata_BlobNameExpression_ReturnsFalse()
		{
			Expression<Func<BlobDocument<string>, bool>> lambda = d => d.BlobName.EndsWith(".json");
			Assert.False(ExpressionTreeHelpers.ReferencesContentOrMetadata(lambda.Body));
		}

		#endregion

		#region Predicate invocation

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_EndsWith_PredicateFiltersCorrectly()
		{
			var expr = BuildExpr<string>(q => q.Where(d => d.BlobName.EndsWith(".json")));
			var (prefix, predicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr);

			Assert.Null(prefix);
			Assert.NotNull(predicate);

			var matchBlob = BlobsModelFactory.BlobItem("data.json", false, BlobsModelFactory.BlobItemProperties(false));
			var noMatchBlob = BlobsModelFactory.BlobItem("data.csv", false, BlobsModelFactory.BlobItemProperties(false));

			Assert.True(predicate(new BlobDocument<string>("data.json", matchBlob, "content")));
			Assert.False(predicate(new BlobDocument<string>("data.csv", noMatchBlob, "content")));
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void GetBlobNamePrefixAndPredicate_StartsWithPlusEndsWith_PredicateOnlyTestsEndsWith()
		{
			var expr = BuildExpr<string>(q => q.Where(d => d.BlobName.StartsWith("folder/") && d.BlobName.EndsWith(".json")));
			var (prefix, predicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<string>(expr);

			Assert.Equal("folder/", prefix);
			Assert.NotNull(predicate);

			// The predicate should test only the EndsWith part (StartsWith was extracted as prefix)
			var matchBlob = BlobsModelFactory.BlobItem("folder/data.json", false, BlobsModelFactory.BlobItemProperties(false));
			var noMatchBlob = BlobsModelFactory.BlobItem("folder/data.csv", false, BlobsModelFactory.BlobItemProperties(false));

			Assert.True(predicate(new BlobDocument<string>("folder/data.json", matchBlob, "content")));
			Assert.False(predicate(new BlobDocument<string>("folder/data.csv", noMatchBlob, "content")));
		}

		#endregion

		#region OrElse path

		[Fact]
		[Trait("Category", "Unit")]
		public void CompileMetadataFilter_OrCondition_EitherSidePasses()
		{
			// OrElse path in MetadataAccessRewriter — both sides are metadata conditions
			var expr = BuildExpr<string>(q =>
				q.Where(d => d.Metadata.Properties.ContentLength > 100 || d.Metadata.Properties.ContentLength < 10));
			var filter = FilterCompiler.CompileMetadataFilter(expr);

			var small = BlobsModelFactory.BlobItem("small", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 5));
			var mid = BlobsModelFactory.BlobItem("mid", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 50));
			var big = BlobsModelFactory.BlobItem("big", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 200));

			Assert.True(filter(small));   // < 10 passes
			Assert.False(filter(mid));    // neither passes
			Assert.True(filter(big));     // > 100 passes
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void FindQueryableSource_NoQueryableConstant_ReturnsNull()
		{
			// FindQueryableSource should return null for expressions without an IQueryable constant.
			var expr = Expression.Constant(42);
			var result = ExpressionTreeHelpers.FindQueryableSource(expr);
			Assert.Null(result);
		}

		#endregion

		#region SourceReplacingVisitor

		[Fact]
		[Trait("Category", "Unit")]
		public void SourceReplacingVisitor_OnlyReplacesMatchingSource()
		{
			// Bug regression: SourceReplacingVisitor replaced ALL IQueryable constants
			// instead of only the specific source. This caused captured IQueryable variables
			// in user predicates to be silently replaced with the materialized result.
			var source = Array.Empty<BlobItem>().AsQueryable();
			var other = Array.Empty<BlobItem>().AsQueryable();
			var replacement = new[] { BlobsModelFactory.BlobItem("replaced", false, BlobsModelFactory.BlobItemProperties(false)) }.AsQueryable();

			// Build: source.Concat(other) — two different IQueryable constants
			var expr = Expression.Call(
				typeof(Queryable),
				nameof(Queryable.Concat),
				new[] { typeof(BlobItem) },
				Expression.Constant(source),
				Expression.Constant(other)
			);

			var visitor = new SourceReplacingVisitor(source, Expression.Constant(replacement));
			var rewritten = visitor.Visit(expr);

			var mce = (MethodCallExpression)rewritten;
			var arg0 = (ConstantExpression)mce.Arguments[0];
			var arg1 = (ConstantExpression)mce.Arguments[1];

			// Only the source constant should be replaced; the other should remain
			Assert.Same(replacement, arg0.Value);
			Assert.Same(other, arg1.Value);
		}

		#endregion
	}
}
