# Twisp 101 gRPC Tutorial Runner (Kotlin)

This project reimplements the [`twisp/grpc101`](https://github.com/twisp/grpc101) tutorial client in Kotlin. It exercises the Twisp streaming `AnyService` gRPC API to replay every step of the Twisp 101 tutorial, from creating journals and accounts through posting transactions and inspecting balances.

## Prerequisites

* **Java 22+** (the Gradle toolchain targets Java 22 bytecode).
* **Gradle** – the repository includes a wrapper (`./gradlew`).
* **buf** 1.26+ – used to fetch the generated SDK from [buf.build](https://buf.build/twisp/api).
* **Docker** (optional) – required if you want to run the local Twisp environment container.

## Generate protobuf and gRPC stubs

All protobuf stubs are generated from the published Twisp API module on buf.build. The Gradle build invokes `buf generate buf.build/twisp/api` automatically via the `bufGenerate` task, but you can run it manually:

```sh
buf generate buf.build/twisp/api
```

Generated sources are written to `build/generated/source/buf/main/{java,kotlin}` and are added to the build classpath automatically.

## Running the tutorial

1. Ensure a Twisp gRPC endpoint is available. The official local container exposes `localhost:8081`:
   ```sh
   docker pull public.ecr.aws/twisp/local:latest
   docker run -p 3000:3000 -p 8080:8080 -p 8081:8081 public.ecr.aws/twisp/local:latest
   ```
2. Build and execute the Kotlin client:
   ```sh
   ./gradlew run
   ```

The program prints progress as it steps through each tutorial action:

1. Creates the general ledger journal and required accounts.
2. Defines ACH and bank transfer tran codes.
3. Posts the tutorial transactions and records ledger entries.
4. Builds an account set and fetches aggregate balances and history.

Authentication headers (`x-twisp-account-id` and `Authorization`) are applied in `Main.kt`. Update the bearer token if you are targeting a hosted Twisp environment.

## Project layout

```
README.md                      — Project overview and usage instructions
buf.gen.yaml                   — buf.generate template targeting the Twisp API module
build.gradle.kts               — Gradle build (Kotlin/JVM, gRPC, buf integration)
src/main/kotlin/.../Main.kt    — Tutorial runner implementation
build/generated/...            — Generated protobuf + gRPC sources (ignored by Git)
```

## Notes

* The client mirrors the behaviour of the Go reference implementation and maintains local state to track accounts, balances, and entries for reporting.
* Build artifacts (including generated sources) live under `build/` and are excluded via `.gitignore`.
* The Kotlin coroutine-based gRPC stub (`AnyServiceGrpcKt`) handles the bidirectional stream. A small `StreamClient` wrapper keeps request/response ordering straightforward for the sequential tutorial steps.
