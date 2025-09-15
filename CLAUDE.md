# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Aspen is a distributed data platform for building higher-level distributed applications, written in Scala 3. It implements a distributed storage system with features like transactions, replication, and NFS server capabilities.

## Common Development Commands

### Building and Testing
- `sbt compile` - Compile the project
- `sbt test` - Run all tests
- `sbt 'testOnly *TestName -- -z "test substring"'` - Run a specific test by substring

### Running the Application
The project includes a main demo application with several modes:
- `./t bootstrap local/cfg.yaml` - Bootstrap a new Aspen system
- `./t node local/bootstrap_config.yaml local/node_a.yaml` - Start a storage node
- `./t nfs local/cfg.yaml local/log4j-conf.xml` - Launch NFS server
- `./t new-pool <log4j-conf> <bootstrap-config> <pool-name> <ida-type> <width> <read-threshold> <write-threshold> <hosts>` - Create new storage pool

### Development Setup Requirements
- Google ProtocolBuffers is used for serializstion/deserialization of many data types and messages
- JDK (configured for Homebrew OpenJDK at `/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home`)

## Architecture Overview

### Core Components
- **Client Layer** (`org.aspen_ddp.aspen.client`): AspenClient, object readers, allocators, transactions
- **Server Layer** (`org.aspen_ddp.aspen.server`): Store management, transaction processing, CRL (Commit Record Log)
- **Common Layer** (`org.aspen_ddp.aspen.common`): Shared data structures, network protocols, IDA (Information Dispersal Algorithms)
- **AmoebaFS** (`org.aspen_ddp.aspen.amoebafs`): Distributed filesystem implementation with NFS support

### Key Architectural Patterns
- **Distributed Storage**: Uses IDA (Replication or Reed-Solomon) for data distribution across storage nodes
- **Transactional**: ACID transactions with conflict detection and commit/abort semantics
- **Object-Oriented Storage**: Data objects, key-value objects, and metadata objects with versioning
- **Network Layer**: ZeroMQ-based networking with separate channels for client requests, server-to-server communication
- **Pluggable Backends**: RocksDB backend for persistent storage

### Storage Pools and Objects
- Storage pools contain distributed objects across multiple nodes
- Objects have UUIDs, revisions, reference counts, and timestamps
- Two object types: Data, KeyValue
- Objects are distributed using configurable IDA schemes

### Testing and Configuration
- ScalaTest framework with custom timeouts (`Tests.Argument(TestFrameworks.ScalaTest, "-W", "10", "5")`)
- YAML-based configuration for bootstrap, nodes, and logging
- Log4j2 with async logging configuration