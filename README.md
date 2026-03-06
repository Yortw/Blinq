# Blinq

**Blinq** (pronounced "Blink") is a LINQ provider for Azure Blob Storage, enabling basic, type-safe queries over blobs using standard LINQ syntax.

## Features

- Query Azure Blob Storage containers with LINQ
- Filter by blob metadata and/or content
- Asynchronous streaming and materialization
- Customizable concurrency
- Strongly-typed document mapping

## Getting Started

### 1. Install NuGet Package

```
dotnet add package Yort.Blinq
```

### 2. Configure Azure Blob Storage

In production code create `BlobServiceClient` from the Azure SDK (using a connection string or SAS), and use it to get a `BlobContainerClient`.
You can then call `AsQueryable<T>()` on the container client to start querying.

To run tests in this solution, set your connection info in environment variables or user secrets:

- `AZURE_STORAGE_BLOB_URI` (e.g. `https://<account>.blob.core.windows.net/`)
- Use Azure AD, Visual Studio, or Interactive login for authentication.

Upload test blobs using the provided script (requires the `Az.Storage` PowerShell module):

```powershell
./Upload-TestBlobs.ps1 -StorageAccountName <your-account> -ContainerName blinqtestdata
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on manual upload and required metadata.

### 3. Basic Usage
NB: Both query and method syntax for LINQ are supported; examples use query syntax for clarity.

```
using Azure.Storage.Blobs; 
using Blinq; 
using System.Linq;

// Define your document type 
public class MyDocument 
{ 
    public string Name { get; set; } 
    public int Value { get; set; } 
}

// Create a BlobServiceClient and get a container 
var blobServiceClient = new BlobServiceClient("<your-blob-uri>", credential); 
var containerClient = blobServiceClient.GetBlobContainerClient("mycontainer");

// Query blobs 
var results = await 
(
  from x in containerClient.AsQueryable<MyDocument>(maxConcurrency: 8) 
  where x.Metadata.Properties.ContentType == "application/json" 
    && x.Content.Value > 10 
  select x
).ToListAsync();

// Projections are not supported on the initial query, project content in-memory after materialising the first result (calling ToListAsync)
var docs = results.Select(x => x.Content).ToList();

```

### 4. Advanced Filtering

#### Query with metadata and content filter:
```
var results = await (
    from x in containerClient.AsQueryable<MyType>()
    where x.Metadata.Properties.ContentType == "application/json"
      && x.Content.SomeProperty == "foo"
    select x
).ToListAsync();
```

#### Project to anonymous type in-memory:
```
var docs = 
(
    await (
        from x in containerClient.AsQueryable<MyType>()
        where x.Content.SomeProperty == "foo"
        select x
    ).ToListAsync()
).Select
(
    x => new { x.Content.SomeProperty, x.Content.OtherProperty }
).ToList();
```

### 5. Notes

- Projections (e.g. `select x.Content`) are supported at materialization, not in query construction.
- For anonymous types, project in-memory after calling `ToListAsync()`.

## Supported LINQ Operators

- `Where` (multiple clauses supported)
- `Select` (at materialization)
- Others (`OrderBy`, `Skip`, etc.) are not supported natively; use in-memory after materialization.

## Performance
Yeah, nah, no one is going to be blown away by performance here. Blinq doesn't have any magic, it doesn't 
index your blobs or spin up any special services. It just helps you write LINQ queries that get translated into blob listing and filtering operations.
Blinq shouldn't add significant overhead compared to writing equivalent code manually, but the performance will ultimately depend on your blob storage structure, number and size of blobs, and the complexity of your queries.
That said, Blinq does support concurrent fetching of blob contents to help speed up queries that need to read blob data, and uses async I/O to avoid blocking threads for scalability.

It is also possible to optimize your queries to improve performance:

* A StartsWith filter on blob names is passed to the Blob StorageService so Azure can filter results, which can significantly reduce the number of blobs that need to be examined.
* Metadata & property filters are applied before fetching blob contents, so try to filter on metadata first to reduce the number of blobs that need to be read.

## License
This project is licensed under the MIT License.

## Contributing

PRs and issues welcome!