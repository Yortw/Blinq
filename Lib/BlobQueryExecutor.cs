using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Blinq
{
	/// <summary>
	/// Executes LINQ queries over Azure Blob Storage. For large containers, prefer async APIs and streaming to avoid loading all blobs into memory.
	/// </summary>
	internal static class BlobQueryExecutor
	{
		private static async Task CreateBlobTasksAsync<T>(
			BlobContainerClient containerClient,
			Expression expression,
			SemaphoreSlim semaphore,
			List<Task<BlobDocument<T>?>> tasks,
			IBlobContentDeserializer? blobDeserializer = null,
			bool metadataOnly = false,
			CancellationToken cancellationToken = default)
		{
			var (prefix, blobNamePredicate) = BlobNamePrefixExtractor.GetBlobNamePrefixAndPredicate<T>(expression);
			var metadataFilter = FilterCompiler.CompileMetadataFilter(expression);
			var contentFilter = FilterCompiler.CompileContentFilter<T>(expression);

			var deserializer = blobDeserializer ?? GetDefaultDeserializer<T>();

			await foreach (var blobItem in containerClient.GetBlobsAsync(traits: BlobTraits.Metadata, prefix: prefix, cancellationToken: cancellationToken).ConfigureAwait(false))
			{
				var docForFilter = new BlobDocument<T>(blobItem.Name, blobItem, default!);
				if (blobNamePredicate != null && !blobNamePredicate(docForFilter))
				{
					continue;
				}

				if (!metadataFilter(blobItem))
				{
					continue;
				}

				if (metadataOnly)
				{
					// Only return metadata, skip content download
					tasks.Add(Task.FromResult<BlobDocument<T>?>(new BlobDocument<T>(blobItem.Name, blobItem, default!)));
					continue;
				}

				await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

				var task = Task.Run(async () =>
				{
					try
					{
						var blobClient = containerClient.GetBlobClient(blobItem.Name);
						var response = await blobClient.DownloadContentAsync(cancellationToken).ConfigureAwait(false);
						using (var stream = response.Value.Content.ToStream())
						{
							T? content;
							try
							{
								content = deserializer.Deserialize<T>(stream);
							}
							catch (Exception ex)
							{
								throw new BlobQueryException(
									$"Failed to deserialize blob '{blobItem.Name}' in container '{containerClient.Name}' as {typeof(T).Name}.",
									containerClient.Name,
									blobItem.Name,
									ex
								);
							}

							if (content != null && contentFilter(content, blobItem))
							{
								return new BlobDocument<T>(blobItem.Name, blobItem, content);
							}

							return null;
						}
					}
					catch (BlobQueryException)
					{
						throw;
					}
					catch (OperationCanceledException)
					{
						throw;
					}
					catch (Exception ex)
					{
						throw new BlobQueryException(
							$"Failed to download blob '{blobItem.Name}' from container '{containerClient.Name}'.",
							containerClient.Name,
							blobItem.Name,
							ex
						);
					}
					finally
					{
						semaphore.Release();
					}
				}, cancellationToken);

				tasks.Add(task);
			}

		}

		private static IBlobContentDeserializer GetDefaultDeserializer<T>()
		{
			if (typeof(T) == typeof(string))
			{
				return StringBlobDeserializer.Default;
			}
			else if (typeof(T) == typeof(byte[]))
			{
				return ByteArrayBlobDeserializer.Default;
			}

			return JsonBlobDeserializer.Default;
		}

		/// <summary>
		/// Synchronously executes a query over blobs. For large containers, this can be expensive and memory-intensive. Prefer async APIs for production use.
		/// </summary>
		/// <remarks>
		/// Uses <see cref="Task.Run(System.Func{Task})"/> internally to avoid deadlocks
		/// in environments with a <see cref="SynchronizationContext"/> (WPF, WinForms, legacy ASP.NET).
		/// </remarks>
		internal static IEnumerable<BlobDocument<T>> ExecuteSync<T>(BlobContainerClient containerClient, Expression expression, int maxConcurrency = 4, IBlobContentDeserializer? blobDeserializer = null, bool metadataOnly = false)
		{
			var semaphore = new SemaphoreSlim(maxConcurrency);
			var tasks = new List<Task<BlobDocument<T>?>>();
			try
			{
				Task.Run(() => CreateBlobTasksAsync<T>(containerClient, expression, semaphore, tasks, blobDeserializer, metadataOnly, default)).GetAwaiter().GetResult();
				foreach (var task in tasks)
				{
					var result = task.GetAwaiter().GetResult();
					if (result != null)
					{
						yield return result;
					}
				}
			}
			finally
			{
				// Ensure all tasks complete before disposing the semaphore
				// to avoid ObjectDisposedException from Release() in task finally blocks.
				// tasks is always non-null — even if CreateBlobTasksAsync threw mid-iteration,
				// any tasks already queued are in the list.
				try
				{
					Task.WhenAll(tasks).GetAwaiter().GetResult();
				}
				catch
				{
					// Exceptions from individual tasks are already observed
					// via the foreach above, or are not actionable here.
				}

				semaphore.Dispose();
			}
		}

		internal static async IAsyncEnumerable<BlobDocument<T>> ExecuteAsync<T>(BlobContainerClient containerClient, Expression expression, int maxConcurrency = 4, IBlobContentDeserializer? blobDeserializer = null, bool metadataOnly = false, [EnumeratorCancellation] CancellationToken cancellationToken = default)
		{
			var semaphore = new SemaphoreSlim(maxConcurrency);
			var tasks = new List<Task<BlobDocument<T>?>>();
			try
			{
				await CreateBlobTasksAsync<T>(containerClient, expression, semaphore, tasks, blobDeserializer, metadataOnly, cancellationToken).ConfigureAwait(false);
				foreach (var task in tasks)
				{
					var result = await task.ConfigureAwait(false);
					if (result != null)
					{
						yield return result;
					}
				}
			}
			finally
			{
				// Ensure all tasks complete before disposing the semaphore
				// to avoid ObjectDisposedException from Release() in task finally blocks.
				// tasks is always non-null — even if CreateBlobTasksAsync threw mid-iteration,
				// any tasks already queued are in the list.
				try
				{
					await Task.WhenAll(tasks).ConfigureAwait(false);
				}
				catch
				{
					// Exceptions from individual tasks are already observed
					// via the foreach above, or are not actionable here.
				}

				semaphore.Dispose();
			}
		}
	}
}
