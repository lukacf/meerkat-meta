/**
 * Session store configuration for Meta runtime.
 */

// -- JSON file store ------------------------------------------------------

export interface JsonSessionStoreConfig {
  readonly path: string;
  readonly staleLockThresholdSeconds: number;
  toDict(): Record<string, unknown>;
}

export function json(
  path: string,
  options?: { staleLockThresholdSeconds?: number },
): JsonSessionStoreConfig {
  const config = {
    path,
    staleLockThresholdSeconds: options?.staleLockThresholdSeconds ?? 30,
    toDict() {
      return jsonSessionStoreConfigToDict(config);
    },
  };
  return config;
}

export function jsonSessionStoreConfigToDict(
  config: JsonSessionStoreConfig,
): Record<string, unknown> {
  return {
    store: "json_file",
    path: config.path,
    stale_lock_threshold_seconds: config.staleLockThresholdSeconds,
  };
}

// -- BigQuery store -------------------------------------------------------

export interface BigQuerySessionStoreConfig {
  readonly dataset: string;
  readonly table: string;
  readonly projectId: string | null;
  readonly gcIntervalHours: number;
  toDict(): Record<string, unknown>;
}

export function bigquery(
  dataset: string,
  table: string,
  options?: { projectId?: string; gcIntervalHours?: number },
): BigQuerySessionStoreConfig {
  const config = {
    dataset,
    table,
    projectId: options?.projectId ?? null,
    gcIntervalHours: options?.gcIntervalHours ?? 6,
    toDict() {
      return bigquerySessionStoreConfigToDict(config);
    },
  };
  return config;
}

export function bigquerySessionStoreConfigToDict(
  config: BigQuerySessionStoreConfig,
): Record<string, unknown> {
  const result: Record<string, unknown> = {
    store: "bigquery",
    dataset: config.dataset,
    table: config.table,
    gc_interval_hours: config.gcIntervalHours,
  };
  if (config.projectId) result.project_id = config.projectId;
  return result;
}
