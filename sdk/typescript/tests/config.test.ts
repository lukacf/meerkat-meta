import { describe, it } from "node:test";
import assert from "node:assert/strict";

import { auth, memory, sessionStore } from "../dist/index.js";

// ---------------------------------------------------------------------------
// auth.google()
// ---------------------------------------------------------------------------

describe("auth.google", () => {
  it("creates a GoogleAuthConfig with defaults", () => {
    const config = auth.google("my-client-id");
    assert.equal(config.clientId, "my-client-id");
    assert.equal(
      config.discoveryUrl,
      "https://accounts.google.com/.well-known/openid-configuration",
    );
    assert.equal(config.audience, null);
    assert.equal(config.leewaySeconds, 60);
  });

  it("allows custom discoveryUrl", () => {
    const config = auth.google("cid", {
      discoveryUrl: "https://custom/.well-known/openid-configuration",
    });
    assert.equal(
      config.discoveryUrl,
      "https://custom/.well-known/openid-configuration",
    );
  });

  it("allows custom audience", () => {
    const config = auth.google("cid", { audience: "my-audience" });
    assert.equal(config.audience, "my-audience");
  });

  it("allows custom leewaySeconds", () => {
    const config = auth.google("cid", { leewaySeconds: 120 });
    assert.equal(config.leewaySeconds, 120);
  });

  it("allows all options combined", () => {
    const config = auth.google("cid", {
      discoveryUrl: "https://example.com",
      audience: "aud",
      leewaySeconds: 30,
    });
    assert.equal(config.clientId, "cid");
    assert.equal(config.discoveryUrl, "https://example.com");
    assert.equal(config.audience, "aud");
    assert.equal(config.leewaySeconds, 30);
  });
});

// ---------------------------------------------------------------------------
// auth.googleAuthConfigToDict()
// ---------------------------------------------------------------------------

describe("auth.googleAuthConfigToDict", () => {
  it("produces the correct wire dict", () => {
    const config = auth.google("cid-1");
    const dict = auth.googleAuthConfigToDict(config);
    assert.equal(dict.provider, "google");
    assert.equal(dict.client_id, "cid-1");
    assert.equal(
      dict.discovery_url,
      "https://accounts.google.com/.well-known/openid-configuration",
    );
    // When audience is null, it falls back to clientId
    assert.equal(dict.audience, "cid-1");
    assert.equal(dict.leeway_seconds, 60);
  });

  it("uses audience when explicitly set", () => {
    const config = auth.google("cid-2", { audience: "explicit-aud" });
    const dict = auth.googleAuthConfigToDict(config);
    assert.equal(dict.audience, "explicit-aud");
  });
});

// ---------------------------------------------------------------------------
// auth.jwt()
// ---------------------------------------------------------------------------

describe("auth.jwt", () => {
  it("creates a JwtAuthConfig with defaults", () => {
    const config = auth.jwt("secret-key");
    assert.equal(config.sharedSecret, "secret-key");
    assert.equal(config.issuer, null);
    assert.equal(config.audience, null);
    assert.equal(config.leewaySeconds, 60);
  });

  it("allows custom issuer", () => {
    const config = auth.jwt("secret", { issuer: "my-issuer" });
    assert.equal(config.issuer, "my-issuer");
  });

  it("allows custom audience", () => {
    const config = auth.jwt("secret", { audience: "my-aud" });
    assert.equal(config.audience, "my-aud");
  });

  it("allows custom leewaySeconds", () => {
    const config = auth.jwt("secret", { leewaySeconds: 10 });
    assert.equal(config.leewaySeconds, 10);
  });
});

// ---------------------------------------------------------------------------
// auth.jwtAuthConfigToDict()
// ---------------------------------------------------------------------------

describe("auth.jwtAuthConfigToDict", () => {
  it("produces the correct wire dict", () => {
    const config = auth.jwt("s3cr3t");
    const dict = auth.jwtAuthConfigToDict(config);
    assert.equal(dict.provider, "jwt");
    assert.equal(dict.shared_secret, "s3cr3t");
    assert.equal(dict.issuer, null);
    assert.equal(dict.audience, null);
    assert.equal(dict.leeway_seconds, 60);
  });

  it("includes issuer and audience when set", () => {
    const config = auth.jwt("key", { issuer: "iss", audience: "aud" });
    const dict = auth.jwtAuthConfigToDict(config);
    assert.equal(dict.issuer, "iss");
    assert.equal(dict.audience, "aud");
  });
});

// ---------------------------------------------------------------------------
// memory.localJson()
// ---------------------------------------------------------------------------

describe("memory.localJson", () => {
  it("produces minimal dict without a health endpoint", () => {
    const config = memory.localJson();
    assert.equal(config.healthCheckEndpoint, null);
    assert.deepEqual(config.toDict(), { backend: "local_json" });
  });

  it("includes health_check_endpoint when provided", () => {
    const config = memory.localJson({
      healthCheckEndpoint: "http://localhost:3000",
    });
    assert.equal(config.healthCheckEndpoint, "http://localhost:3000");
    assert.deepEqual(config.toDict(), {
      backend: "local_json",
      health_check_endpoint: "http://localhost:3000",
    });
  });
});

describe("memory module", () => {
  it("exports localJson as the only backend helper", () => {
    // The predecessor's backend helpers are removed with no alias; the
    // gateway refuses that backend value at init.
    assert.deepEqual(Object.keys(memory).sort(), ["localJson"]);
  });
});

// ---------------------------------------------------------------------------
// sessionStore.json()
// ---------------------------------------------------------------------------

describe("sessionStore.json", () => {
  it("creates JsonSessionStoreConfig with defaults", () => {
    const config = sessionStore.json("./sessions.json");
    assert.equal(config.path, "./sessions.json");
    assert.equal(config.staleLockThresholdSeconds, 30);
  });

  it("allows custom staleLockThresholdSeconds", () => {
    const config = sessionStore.json("/tmp/store.json", {
      staleLockThresholdSeconds: 120,
    });
    assert.equal(config.staleLockThresholdSeconds, 120);
  });
});

// ---------------------------------------------------------------------------
// sessionStore.jsonSessionStoreConfigToDict()
// ---------------------------------------------------------------------------

describe("sessionStore.jsonSessionStoreConfigToDict", () => {
  it("produces the correct wire dict", () => {
    const config = sessionStore.json("/data/sessions.json");
    const dict = sessionStore.jsonSessionStoreConfigToDict(config);
    assert.equal(dict.store, "json_file");
    assert.equal(dict.path, "/data/sessions.json");
    assert.equal(dict.stale_lock_threshold_seconds, 30);
  });

  it("includes custom staleLockThresholdSeconds", () => {
    const config = sessionStore.json("/data/s.json", {
      staleLockThresholdSeconds: 90,
    });
    const dict = sessionStore.jsonSessionStoreConfigToDict(config);
    assert.equal(dict.stale_lock_threshold_seconds, 90);
  });
});

// ---------------------------------------------------------------------------
// sessionStore.bigquery()
// ---------------------------------------------------------------------------

describe("sessionStore.bigquery", () => {
  it("creates BigQuerySessionStoreConfig with defaults", () => {
    const config = sessionStore.bigquery("my_dataset", "my_table");
    assert.equal(config.dataset, "my_dataset");
    assert.equal(config.table, "my_table");
    assert.equal(config.projectId, null);
    assert.equal(config.gcIntervalHours, 6);
  });

  it("allows custom projectId", () => {
    const config = sessionStore.bigquery("ds", "tbl", {
      projectId: "proj-123",
    });
    assert.equal(config.projectId, "proj-123");
  });

  it("allows custom gcIntervalHours", () => {
    const config = sessionStore.bigquery("ds", "tbl", {
      gcIntervalHours: 24,
    });
    assert.equal(config.gcIntervalHours, 24);
  });
});

// ---------------------------------------------------------------------------
// sessionStore.bigquerySessionStoreConfigToDict()
// ---------------------------------------------------------------------------

describe("sessionStore.bigquerySessionStoreConfigToDict", () => {
  it("produces the correct wire dict with defaults", () => {
    const config = sessionStore.bigquery("ds", "tbl");
    const dict = sessionStore.bigquerySessionStoreConfigToDict(config);
    assert.equal(dict.store, "bigquery");
    assert.equal(dict.dataset, "ds");
    assert.equal(dict.table, "tbl");
    assert.equal(dict.gc_interval_hours, 6);
    // projectId omitted when null
    assert.equal(dict.project_id, undefined);
  });

  it("includes projectId when set", () => {
    const config = sessionStore.bigquery("ds", "tbl", {
      projectId: "my-proj",
    });
    const dict = sessionStore.bigquerySessionStoreConfigToDict(config);
    assert.equal(dict.project_id, "my-proj");
  });

  it("includes custom gcIntervalHours", () => {
    const config = sessionStore.bigquery("ds", "tbl", {
      gcIntervalHours: 12,
    });
    const dict = sessionStore.bigquerySessionStoreConfigToDict(config);
    assert.equal(dict.gc_interval_hours, 12);
  });
});
