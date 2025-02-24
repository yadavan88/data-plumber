# data-plumber
Moving your data through pipes, with ease!


A sample implementation showcasing clean, type-safe data integration patterns in Scala. Data-Plumber demonstrates how to build intuitive interfaces for transferring data between different sources and sinks - whether it's CSV files, MongoDB, PostgreSQL, or beyond.


[Quick Start Guide](src/main/scala/examples)

## What's Inside?

- **Pattern Showcase**: Examples of clean source/sink implementations
- **Type-Safe Design**: Demonstrates leveraging Scala's type system
- **Simple Patterns**: Minimal, educational implementations
- **Extensible Examples**: Learn how to add your own sources and sinks
- **Reference Architecture**: Clean, consistent patterns you can adopt

This is an educational project showing how to:
- Build type-safe data transfer interfaces
- Implement clean CSV and MongoDB connectors
- Structure your own data pipeline code
- Avoid common integration pitfalls


It intentionally does not cover production concerns such as:
- Transaction management
- Retry mechanisms
- Error recovery
- Connection pooling
- Batch processing optimizations


# Integration Types
Data Plumber provides three different integration patterns to handle various data transfer scenarios.

Examples of different types and their sample implementation are [available here](src/main/scala/examples).
Before running the samples, you'll need to start the required services using Docker:
```
docker compose up
```


## 1. Simple Integration

The simplest form of data integration with synchronous execution and no additional features.

### Key Features:
- Synchronous execution
- Basic error handling
- Direct source to sink transfer
- No offset tracking or effect handling

### Example
```scala
class StarLogIntegrator extends DataPlumber[StarLogEntry, MongoStarLogEntry] {
  override def source: CsvSource[StarLogEntry] = new StarLogSource()
  override def sink: MongoSink[MongoStarLogEntry] = new StarLogSink()
  override def transform(list: List[StarLogEntry]): List[MongoStarLogEntry] = {
    list.map(value => MongoStarLogEntry(...))
  }
}
```


## 2. Effectful Integration

Adds support for effect handling using cats-effect IO, providing better error handling and resource management.

### Key Features:
- Effect-based execution (cats-effect IO)
- Advanced error handling
- Resource safety
- Composable operations



## 3. Offsetable Integration

More advanced integration pattern with support for offset tracking using Redis, enabling resumable transfers and progress tracking. This can also potentially support distributed locking to avoid processing the same source data simultaneously by multiple sources.

### Key Features:
- Offset tracking via Redis
- Resumable operations
- Batching Source
- Progress persistence
- Effect-based execution
- Named operations
- Locking mechanism on the Source (TBA)

### Example:

[Offsetable Sample](src/main/scala/examples#offsetable-data-plumber-framework)


## Common Components

All integration types share these common elements:
- Source definition
- Sink definition
- Transform logic
- Error handling (varies by type)

## Supported Source/Sink Types
- CSV files
- MongoDB
- PostgreSQL
- Custom implementations possible

Each integration type builds upon the previous one, adding more features and complexity as needed for your use case.


# How to run the sample
Before running the samples, you'll need to start the required services using Docker. Here's how to set up MongoDB, PostgreSQL, and Redis:

```
docker compose up
```
This starts MongoDB, PostgreSQL and Redis services required. 
