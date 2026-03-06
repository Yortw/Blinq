param(
 [Parameter(Mandatory=$true)]
 [string]$StorageAccountName,
 [Parameter(Mandatory=$true)]
 [string]$ContainerName,
 [Parameter(Mandatory=$false)]
 [string]$BlobFolder = "BlinqTestData/Blobs"
)

# Login interactively if needed
if (-not (Get-AzContext)) {
 Connect-AzAccount | Out-Null
}

# Create storage context using Azure AD (OAuth) auth — matches how integration tests authenticate
$context = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

function Get-ContentType {
 param([string]$FileName)
 switch -Regex ($FileName) {
 ".*\.json$" { return "application/json" }
 ".*\.txt$" { return "text/plain" }
 ".*\.csv$" { return "text/csv" }
 ".*\.xml$" { return "application/xml" }
 ".*\.bin$" { return "application/octet-stream" }
 default { return "application/octet-stream" }
 }
}

function Get-BlobMetadata {
 param([string]$FileName)
 $metadata = @{}
 # Tag TestDocument blobs as "category=test", sample blobs as "category=sample"
 if ($FileName -match "^TestDocument") {
  $metadata["category"] = "test"
 } elseif ($FileName -match "^sample") {
  $metadata["category"] = "sample"
 }
 # Add a source tag to all blobs for general metadata filtering tests
 $metadata["source"] = "blinq-test-upload"
 return $metadata
}

# Get all files in the blob folder
$files = Get-ChildItem -Path $BlobFolder -File

foreach ($file in $files) {
 $contentType = Get-ContentType $file.Name
 $metadata = Get-BlobMetadata $file.Name
 Write-Host "Uploading $($file.Name) to container $ContainerName with content-type $contentType, metadata: $($metadata | ConvertTo-Json -Compress)..."
 Set-AzStorageBlobContent -File $file.FullName -Container $ContainerName -Blob $file.Name -Context $context -Properties @{ "ContentType" = $contentType } -Metadata $metadata -Force | Out-Null
}

Write-Host "Upload complete."
