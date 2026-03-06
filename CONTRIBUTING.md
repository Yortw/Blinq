# Contributing to Blinq

Thank you for your interest in contributing to Blinq!

## Setting Up Development Environment

### Prerequisites
- .NET 8 SDK or later
- Visual Studio 2022 or VS Code
- Azure Storage Account for integration tests (optional — unit tests run without Azure)

### Running Tests

**Unit tests** run without any configuration:

```
dotnet test --filter "Category!=Integration"
```

**Integration tests** require Azure Blob Storage access:

1. Set up user secrets for the test project:

```
dotnet user-secrets set "AZURE_STORAGE_BLOB_URI" "https://<your-account>.blob.core.windows.net/" --project Blinq.Tests/Blinq.Tests.csproj
```

2. Upload test blobs using the provided PowerShell script (requires the [Az.Storage](https://learn.microsoft.com/en-us/powershell/azure/install-azure-powershell) module):

```powershell
./Upload-TestBlobs.ps1 -StorageAccountName <your-account> -ContainerName blinqtestdata
```

This uploads all files from `BlinqTestData/Blobs/` into the container with the correct `ContentType` and user-defined metadata (`category`, `source`) needed by integration tests. The script authenticates via Azure AD (`Connect-AzAccount`), matching how the integration tests authenticate.

You can also upload manually, but you must ensure:
- Blob names match the filenames in `BlinqTestData/Blobs/` (no folder prefix)
- `ContentType` is set appropriately (e.g. `application/json` for `.json` files)
- User-defined metadata tags are set: `category` = `test` or `sample`, `source` = `blinq-test-upload`

3. Run all tests (integration tests will be skipped if Azure is not configured):

```
dotnet test
```

### Code Style
- Follow existing code conventions
- Use tabs for indentation (matching project style)
- Enable nullable reference types
- Add XML documentation comments for public APIs

### Pull Request Process
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Reporting Issues
When reporting issues, please include:
- Blinq version
- .NET version
- Azure Storage SDK version
- Minimal reproducible example
- Expected vs actual behavior
