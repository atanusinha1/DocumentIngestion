# DocumentIngestion — Local RAG Pipeline

A production-grade document ingestion and retrieval-augmented generation (RAG) pipeline built with **.NET 10 C# microservices**, running entirely on your local machine with no cloud dependencies.

Drop a PDF (or connect SharePoint, Confluence, Jira, SQL, Email, or a website) and ask questions about it in a chat UI — powered by local LLMs via Ollama.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CONNECTOR LAYER                          │
│   PDF  │  SharePoint  │  Confluence  │  Jira  │  SQL  │  Email │
└───────────────────────────┬─────────────────────────────────────┘
                            │ DocumentIngestedEvent
                            ▼
                    ┌───────────────┐
                    │   Azurite     │  ← blob staging (raw document JSON)
                    └───────┬───────┘
                            │
                    ┌───────────────┐
                    │   RabbitMQ    │  ← message broker
                    └───────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
    ┌──────────────┐               ┌──────────────┐
    │  Chunking    │               │  Dead Letter  │
    │  Service     │               │  Queue        │
    └──────┬───────┘               └──────────────┘
           │ ChunksReadyEvent
           ▼
    ┌──────────────┐
    │  Embedding   │  ← Ollama nomic-embed-text (768 dims)
    │  Service     │
    └──────┬───────┘
           │ EmbeddingsReadyEvent
           ▼
    ┌──────────────┐
    │  VectorDB    │  ← Qdrant (REST upsert)
    │  Writer      │
    └──────────────┘

    ┌──────────────────────────────┐
    │       Query Service          │
    │  Question → embed → search   │
    │  → mistral → answer + sources│
    └──────────────────────────────┘
```

---

## Tech Stack

### Local Development (Zero Cost)

| Component | Technology | Port |
|---|---|---|
| Message Queue | RabbitMQ 3.13 | 5672 / 15672 (UI) |
| Blob Storage | Azurite (Azure emulator) | 10000 |
| Vector Database | Qdrant | 6333 (REST) / 6334 (gRPC) |
| Embedding Model | Ollama + `nomic-embed-text` (768 dims) | 11434 |
| LLM | Ollama + `mistral` | 11434 |
| Chat UI | ASP.NET Core + HTML/JS | 5100 |

### Production (Azure)

| Component | Technology |
|---|---|
| Message Queue | Azure Service Bus |
| Blob Storage | Azure Blob Storage |
| Vector Database | Azure AI Search or Qdrant |
| Embedding Model | Azure OpenAI `text-embedding-3-large` |
| LLM | Azure OpenAI `gpt-4o` |

---

## Project Structure

```
DocumentIngestion/
├── docker-compose.dev.yml              ← Local infrastructure (RabbitMQ, Azurite, Qdrant)
├── src/
│   ├── DocumentIngestion.Contracts/    ← Shared events, models, interfaces
│   │   ├── Events/                     ← DocumentIngestedEvent, ChunksReadyEvent, etc.
│   │   ├── Interfaces/                 ← IMessagePublisher, IMessageConsumer
│   │   └── Models/                     ← RawDocument, Chunk, ChunkEmbedding
│   │
│   ├── DocumentIngestion.Messaging/    ← RabbitMQ publisher & consumer
│   │   └── RabbitMQ/
│   │       ├── RabbitMqOptions.cs
│   │       ├── RabbitMqConnectionFactory.cs
│   │       ├── RabbitMqPublisher.cs    ← IMessagePublisher implementation
│   │       └── RabbitMqConsumer.cs     ← IMessageConsumer implementation
│   │
│   ├── DocumentIngestion.Connectors/   ← Document source connectors
│   │   ├── Base/ConnectorBase.cs       ← Shared polling + staging logic
│   │   ├── Pdf/PdfConnector.cs         ← Hot-folder PDF watcher (PdfPig)
│   │   ├── SharePoint/                 ← Microsoft Graph SDK v6 + delta feed
│   │   ├── Confluence/                 ← REST API + HTML stripping
│   │   ├── Jira/                       ← JQL polling
│   │   ├── SqlDatabase/                ← Dapper table mapping
│   │   ├── Email/                      ← MailKit IMAP
│   │   └── WebCrawler/                 ← BFS crawler + HtmlAgilityPack
│   │
│   ├── DocumentIngestion.ChunkingService/
│   │   ├── Chunking/RecursiveTextChunker.cs  ← 512-token chunks, 64 overlap
│   │   └── Workers/ChunkingWorker.cs
│   │
│   ├── DocumentIngestion.EmbeddingService/
│   │   └── Workers/OllamaEmbeddingWorker.cs  ← Batched Ollama /api/embed calls
│   │
│   ├── DocumentIngestion.VectorDbWriter/
│   │   └── Writers/QdrantWriter.cs     ← REST upsert to Qdrant
│   │
│   └── DocumentIngestion.QueryService/ ← RAG chat API + UI
│       ├── RagService.cs               ← Embed → Search → Generate
│       ├── Program.cs                  ← Minimal API endpoints
│       └── wwwroot/index.html          ← Chat UI
└── k8s/                                ← Kubernetes manifests
```

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| .NET SDK | 10.0+ | https://dotnet.microsoft.com/download |
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop |
| Ollama | Latest | `brew install ollama` |

---

## Step-by-Step Setup

### Step 1 — Clone and create the solution

```bash
# Create solution
dotnet new sln -n DocumentIngestion
mkdir -p src

# Core shared library
dotnet new classlib -n DocumentIngestion.Contracts   -o src/DocumentIngestion.Contracts
dotnet new classlib -n DocumentIngestion.Messaging   -o src/DocumentIngestion.Messaging

# Microservices
dotnet new worker  -n DocumentIngestion.Connectors       -o src/DocumentIngestion.Connectors
dotnet new worker  -n DocumentIngestion.ChunkingService  -o src/DocumentIngestion.ChunkingService
dotnet new worker  -n DocumentIngestion.EmbeddingService -o src/DocumentIngestion.EmbeddingService
dotnet new worker  -n DocumentIngestion.VectorDbWriter   -o src/DocumentIngestion.VectorDbWriter

# Query / RAG service
dotnet new web     -n DocumentIngestion.QueryService     -o src/DocumentIngestion.QueryService

# Add to solution
dotnet sln add src/**/*.csproj
```

### Step 2 — Add NuGet packages

```bash
# Messaging layer
dotnet add src/DocumentIngestion.Messaging package RabbitMQ.Client
dotnet add src/DocumentIngestion.Messaging package Newtonsoft.Json

# Connectors
cd src/DocumentIngestion.Connectors
dotnet add package PdfPig
dotnet add package HtmlAgilityPack
dotnet add package MailKit
dotnet add package RestSharp
dotnet add package Polly
dotnet add package Microsoft.Graph
dotnet add package Azure.Identity
dotnet add package Azure.Storage.Blobs
dotnet add package Microsoft.Extensions.Http
dotnet add package RabbitMQ.Client
cd ../..

# Chunking
dotnet add src/DocumentIngestion.ChunkingService package Azure.Storage.Blobs
dotnet add src/DocumentIngestion.ChunkingService package Newtonsoft.Json

# VectorDbWriter
dotnet add src/DocumentIngestion.VectorDbWriter package Qdrant.Client
dotnet add src/DocumentIngestion.VectorDbWriter package Newtonsoft.Json

# QueryService
dotnet add src/DocumentIngestion.QueryService package Newtonsoft.Json
dotnet add src/DocumentIngestion.QueryService package Microsoft.Extensions.Http

# Health checks (all services)
dotnet add src/DocumentIngestion.ChunkingService  package Microsoft.Extensions.Diagnostics.HealthChecks
dotnet add src/DocumentIngestion.EmbeddingService package Microsoft.Extensions.Diagnostics.HealthChecks
dotnet add src/DocumentIngestion.VectorDbWriter   package Microsoft.Extensions.Diagnostics.HealthChecks
```

### Step 3 — Add project references

```bash
# All services reference Contracts and Messaging
dotnet add src/DocumentIngestion.Connectors       reference src/DocumentIngestion.Contracts
dotnet add src/DocumentIngestion.Connectors       reference src/DocumentIngestion.Messaging
dotnet add src/DocumentIngestion.ChunkingService  reference src/DocumentIngestion.Contracts
dotnet add src/DocumentIngestion.ChunkingService  reference src/DocumentIngestion.Messaging
dotnet add src/DocumentIngestion.EmbeddingService reference src/DocumentIngestion.Contracts
dotnet add src/DocumentIngestion.EmbeddingService reference src/DocumentIngestion.Messaging
dotnet add src/DocumentIngestion.VectorDbWriter   reference src/DocumentIngestion.Contracts
dotnet add src/DocumentIngestion.VectorDbWriter   reference src/DocumentIngestion.Messaging
```

### Step 4 — Pull local AI models

```bash
# nomic-embed-text: 768-dim embeddings (274MB)
ollama pull nomic-embed-text

# mistral: LLM for answer generation (4.4GB)
ollama pull mistral

# Verify both are available
ollama list
```

### Step 5 — Start local infrastructure

**`docker-compose.dev.yml`** (place in solution root):

```yaml
services:

  rabbitmq:
    image: rabbitmq:3.13-management
    container_name: di_rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq

  azurite:
    image: mcr.microsoft.com/azure-storage/azurite
    container_name: di_azurite
    ports:
      - "10000:10000"
    command: "azurite --blobHost 0.0.0.0 --loose --skipApiVersionCheck"
    volumes:
      - azurite_data:/data

  qdrant:
    image: qdrant/qdrant:v1.9.1
    container_name: di_qdrant
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage

volumes:
  rabbitmq_data:
  azurite_data:
  qdrant_data:
```

```bash
docker-compose -f docker-compose.dev.yml up -d
```

### Step 6 — Configure each service

Create `appsettings.Development.json` in each service folder:

**`src/DocumentIngestion.Connectors/appsettings.Development.json`**
```json
{
  "BlobStorage": {
    "ConnectionString": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
    "ContainerName": "documents-dev"
  },
  "RabbitMQ": {
    "Host": "localhost",
    "Username": "admin",
    "Password": "admin",
    "ExchangeName": "document-ingestion"
  },
  "PdfConnector": {
    "WatchDirectory":      "/tmp/di-pdfs/incoming",
    "ProcessedDirectory":  "/tmp/di-pdfs/processed",
    "PollIntervalSeconds": 5
  }
}
```

**`src/DocumentIngestion.ChunkingService/appsettings.Development.json`**
```json
{
  "BlobStorage": {
    "ConnectionString": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
    "ContainerName": "documents-dev"
  },
  "RabbitMQ": {
    "Host": "localhost",
    "Username": "admin",
    "Password": "admin",
    "ExchangeName": "document-ingestion"
  }
}
```

**`src/DocumentIngestion.EmbeddingService/appsettings.Development.json`**
```json
{
  "RabbitMQ": {
    "Host": "localhost",
    "Username": "admin",
    "Password": "admin",
    "ExchangeName": "document-ingestion"
  },
  "Ollama": {
    "Endpoint":         "http://localhost:11434",
    "Model":            "nomic-embed-text",
    "VectorDimensions": 768,
    "BatchSize":        16
  }
}
```

**`src/DocumentIngestion.VectorDbWriter/appsettings.Development.json`**
```json
{
  "RabbitMQ": {
    "Host": "localhost",
    "Username": "admin",
    "Password": "admin",
    "ExchangeName": "document-ingestion"
  },
  "Qdrant": {
    "Host":             "localhost",
    "Port":             6334,
    "CollectionName":   "document_chunks",
    "VectorDimensions": 768
  }
}
```

**`src/DocumentIngestion.QueryService/appsettings.Development.json`**
```json
{
  "Qdrant": {
    "BaseUrl":        "http://localhost:6333",
    "CollectionName": "document_chunks",
    "TopK":           5
  },
  "Ollama": {
    "BaseUrl":    "http://localhost:11434",
    "EmbedModel": "nomic-embed-text:latest",
    "ChatModel":  "mistral:latest"
  }
}
```

### Step 7 — Create watch directories

```bash
mkdir -p /tmp/di-pdfs/incoming
mkdir -p /tmp/di-pdfs/processed
```

### Step 8 — Run all services

Open 5 terminals and run one command per terminal:

```bash
# Terminal 1 — Connectors (start first: creates the blob container)
cd src/DocumentIngestion.Connectors
DOTNET_ENVIRONMENT=Development dotnet run

# Terminal 2 — Chunking Service
cd src/DocumentIngestion.ChunkingService
DOTNET_ENVIRONMENT=Development dotnet run

# Terminal 3 — Embedding Service
cd src/DocumentIngestion.EmbeddingService
DOTNET_ENVIRONMENT=Development dotnet run

# Terminal 4 — Vector DB Writer
cd src/DocumentIngestion.VectorDbWriter
DOTNET_ENVIRONMENT=Development dotnet run

# Terminal 5 — Query Service (chat UI)
cd src/DocumentIngestion.QueryService
DOTNET_ENVIRONMENT=Development dotnet run
```

### Step 9 — Ingest a document

```bash
# Copy any PDF into the watch folder
cp ~/Downloads/your-document.pdf /tmp/di-pdfs/incoming/

# Verify it's a real PDF (first 4 bytes must be %PDF)
head -c 4 /tmp/di-pdfs/incoming/your-document.pdf
```

Watch the pipeline process it across all terminals:

```
Connectors:   Processing PDF: your-document.pdf (N pages) → blob staged ✅
Chunking:     Document split into N chunks ✅
Embedding:    Ollama: embeddings ready (3 batches × 200 OK) ✅
VectorWriter: QdrantWriter: ✅ saved N points ✅
```

### Step 10 — Ask questions

```bash
open http://localhost:5100
```

Try questions like:
- `"What is this document about?"`
- `"Summarise the key points"`
- `"What are the dates mentioned?"`

---

## Monitoring

| Dashboard | URL | Credentials |
|---|---|---|
| RabbitMQ queues | http://localhost:15672 | admin / admin |
| Qdrant collections | http://localhost:6333/dashboard | — |
| Chat UI | http://localhost:5100 | — |

---

## Connectors Reference

| Connector | Trigger | Config key |
|---|---|---|
| **PDF** | File dropped in watch folder | `PdfConnector` |
| **SharePoint** | Microsoft Graph delta feed | `SharePoint` |
| **Confluence** | REST API polling | `Confluence` |
| **Jira** | JQL query polling | `Jira` |
| **SQL Database** | Table polling by modified date | `SqlConnector` |
| **Email** | IMAP INBOX polling | `EmailConnector` |
| **Web Crawler** | BFS crawl from seed URLs | `WebCrawler` |

To enable a connector, register its worker in `Connectors/Program.cs`:

```csharp
// Example: enable SQL connector alongside PDF
builder.Services.Configure<SqlConnectorOptions>(
    builder.Configuration.GetSection("SqlConnector"));
builder.Services.AddSingleton<SqlConnector>();
builder.Services.AddHostedService<SqlWorker>();
```

---

## How the RAG Pipeline Works

### Ingestion (offline)

```
1. Connector pulls document from source
2. PdfPig / REST API / IMAP extracts plain text
3. Raw document JSON staged to Azurite blob storage
4. DocumentIngestedEvent published to RabbitMQ

5. ChunkingService downloads blob, splits text into 512-token chunks
   with 64-token overlap using recursive character splitting
6. ChunksReadyEvent published (chunks as JSON)

7. EmbeddingService sends each chunk to Ollama nomic-embed-text
   in batches of 16 → returns 768-dimensional float vectors
8. EmbeddingsReadyEvent published

9. VectorDbWriter upserts vectors + metadata to Qdrant via REST API
   Payload stored: documentId, text, title, url, source
```

### Query (online)

```
1. User types question in chat UI
2. QueryService sends question to Ollama nomic-embed-text → 768-dim vector
3. Vector searched against Qdrant (cosine similarity, top 5)
4. Retrieved chunks assembled as context
5. Context + question sent to Ollama mistral → streamed answer
6. Answer + source citations returned to UI
```

---

## Chunking Strategy

```
RecursiveTextChunker splits on: \n\n → \n → ". " → " " → ""
Chunk size:    512 tokens (~2048 chars)
Chunk overlap: 64 tokens  (~256 chars)
```

Each chunk stores metadata: `source`, `title`, `url`, `chunkOf` (total chunk count).

---

## Known Issues and Fixes

| Error | Cause | Fix |
|---|---|---|
| `Azurite API version not supported` | Azure SDK newer than Azurite | Add `--skipApiVersionCheck` to Azurite command |
| `Vector inserting error: expected dim: 768, got 0` | gRPC float serialization | Use REST API for Qdrant upsert |
| `model 'mistral' not found` | Docker Ollama has no models | Stop Docker Ollama, use native Ollama |
| `address already in use :11434` | Ollama already running as daemon | Don't run `ollama serve` — it auto-starts on Mac |
| `IAdditionalDataHolder not found` | Kiota 2.0 moved interface | Remove `PageIterator`, use manual pagination |
| `RootRequestBuilder has no Delta` | Graph SDK v6 path changed | Use `Drives[id].Items["root"].Delta` not `Root.Delta` |
| `GetAsync is obsolete` | Graph SDK v6 | Use `GetAsDeltaGetResponseAsync` |
| Messages dropped between services | Queue not bound before publish | Publisher pre-declares all queues on startup |
| `float[]` arrives empty after RabbitMQ | System.Text.Json + record init | Use Newtonsoft.Json for deserialization |

---

## Production Deployment

### Environment switching

Each service reads `appsettings.{ENVIRONMENT}.json`. Set the environment variable:

```bash
# Local development
DOTNET_ENVIRONMENT=Development dotnet run

# Production (uses appsettings.Production.json with Azure connection strings)
DOTNET_ENVIRONMENT=Production dotnet run
```

### Azure production config

```json
// appsettings.Production.json (each service)
{
  "ServiceBus": {
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;..."
  },
  "BlobStorage": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=...;",
    "ContainerName": "documents-prod"
  },
  "AzureSearch": {
    "Endpoint": "https://your-search.search.windows.net",
    "Key": "..."
  },
  "Embedding": {
    "AzureOpenAiEndpoint": "https://your-resource.openai.azure.com/",
    "AzureOpenAiKey": "...",
    "DeploymentName": "text-embedding-3-large"
  }
}
```

### Docker

```bash
# Build and run all services
docker-compose up --build

# Individual service
docker build -t di-chunking -f src/DocumentIngestion.ChunkingService/Dockerfile .
docker run -e DOTNET_ENVIRONMENT=Production di-chunking
```

### Kubernetes

```bash
# Apply all manifests
kubectl apply -f k8s/

# Scale chunking based on queue depth
kubectl scale deployment chunking-service --replicas=5
```

---

## Development Tips

```bash
# Remove the redundant System.Text.Json package warning
dotnet remove src/DocumentIngestion.Messaging package System.Text.Json

# Purge RabbitMQ queues (when testing fresh)
curl -X DELETE "http://localhost:15672/api/queues/%2F/document-ingested/contents" -u admin:admin
curl -X DELETE "http://localhost:15672/api/queues/%2F/chunks-ready/contents" -u admin:admin
curl -X DELETE "http://localhost:15672/api/queues/%2F/embeddings-ready/contents" -u admin:admin

# Delete and recreate Qdrant collection (when changing embedding model)
curl -X DELETE http://localhost:6333/collections/document_chunks

# Move processed PDFs back for reprocessing
mv /tmp/di-pdfs/processed/*.pdf /tmp/di-pdfs/incoming/

# Test Ollama embedding directly
curl http://localhost:11434/api/embed \
  -d '{"model":"nomic-embed-text","input":["test document"]}'

# Search Qdrant directly
curl -s http://localhost:6333/collections/document_chunks/points/search \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.1, 0.2, ...768 values...], "limit": 3, "with_payload": true}'
```

---

## Model Comparison

### Embedding models (Ollama)

| Model | Dimensions | Size | Speed (M2) |
|---|---|---|---|
| `nomic-embed-text` | 768 | 274MB | ~20ms/batch ✅ Recommended |
| `mxbai-embed-large` | 1024 | 670MB | ~40ms/batch |
| `all-minilm` | 384 | 45MB | ~5ms/batch (fastest) |

### LLM models (Ollama)

| Model | Size | Best for |
|---|---|---|
| `mistral` | 4.4GB | General Q&A, fast |
| `llama3` | 4.7GB | Better reasoning |
| `codellama` | 3.8GB | Code-related documents |

> **Changing models:** If you switch embedding model, delete the Qdrant collection first
> (`curl -X DELETE http://localhost:6333/collections/document_chunks`) and
> update `VectorDimensions` in all `appsettings.Development.json` files.