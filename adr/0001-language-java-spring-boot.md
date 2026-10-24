# ADR 0001: Java 21 and Spring Boot 3

Status: accepted (2026-10-24)

## Context
The execution plan recommended Go but states that the language is the developer's choice (D1). The developer will write the project in Java.

## Decision
Use Java 21 with Spring Boot 3, Spring JDBC (plain SQL), Flyway, spring-kafka, Micrometer/OpenTelemetry, JUnit 5 and Testcontainers. One image runs the api, relay, consumer and cli roles through Spring profiles.

## Consequences
JVM memory is higher than Go, so containers get explicit limits and SerialGC. Library names in the plan (chi, pgx, sqlc, franz-go) are replaced by the Java equivalents in docs/pdf-review.md.
