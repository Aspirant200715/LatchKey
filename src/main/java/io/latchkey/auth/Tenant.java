package io.latchkey.auth;

import java.util.UUID;

public record Tenant(UUID tenantId, String name) {}
