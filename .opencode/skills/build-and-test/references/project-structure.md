# Project Structure

Dongting is a multi-module Maven project with the following structure:

## Modules

| Module | Java Version | Description |
|--------|--------------|-------------|
| `client` | Java 8 | Client module, has module-info.java, multiple compile profiles |
| `client-ex` | Java 8+ | Development only, copied into client module during package |
| `server` | Java 11+ | Server module, depends on client |
| `test-support` | Java 11+ | Shared test utilities |
| `dist` | Java 11+ | Packaging and scripts |
| `it-test` | Java 11+ | Integration tests |
| `demos` | Java 11+ | Example applications |

## Key Characteristics

- **High-performance, zero-dependency** raft/kv/mq engine
- **Fiber-based concurrency** framework (coroutines)
- **Project name**: Always use "dongting" (English), never Chinese name
- **Quality Standard**: Code as a top-tier expert; prioritize performance and strive for excellence
