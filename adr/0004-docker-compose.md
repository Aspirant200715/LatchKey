# ADR 0004: Docker and Compose

Status: accepted (2026-10-24)

## Context
The project must be dockerized and reproducible on any machine.

## Decision
Use Docker with Compose. One multi-stage Dockerfile builds the frontend and the jar and produces a small JRE runtime image. Compose profiles keep optional observability services off by default.

## Consequences
Requires the Docker daemon and group membership. Podman is not used.
