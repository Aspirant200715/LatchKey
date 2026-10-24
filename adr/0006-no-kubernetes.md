# ADR 0006: No Kubernetes

Status: accepted (2026-10-24)

## Context
The evaluator's expectation of Kubernetes is unknown (D6). The plan says to use Compose first.

## Decision
Do not use Kubernetes. Deployment is Docker Compose only.

## Consequences
If the department requires it, add manifests after M6 as an optional stretch; the images and health endpoints already support that.
