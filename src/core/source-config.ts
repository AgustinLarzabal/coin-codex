import { readFile } from "node:fs/promises";

export type SourceRatePolicy = {
  minDelayMs?: number;
  backoffBaseMs?: number;
  attemptLimit?: number;
};

export type FakeSourceConfig = {
  adapter: "fake";
  fixtureId: string;
  name?: string;
  domain?: string;
  startUrl: string;
  ratePolicy?: SourceRatePolicy;
};

export type FirecrawlSourceConfig = {
  adapter: "firecrawl";
  apiKey: string;
  name?: string;
  domain?: string;
  startUrl: string;
  ratePolicy?: SourceRatePolicy;
};

export type SourceConfig = FakeSourceConfig | FirecrawlSourceConfig;

export type SeedSourceRecord = {
  id: string;
  config: SourceConfig;
};

function readConfigObject(value: unknown, message: string): Record<string, unknown> {
  if (!value || typeof value !== "object") {
    throw new Error(message);
  }

  return value as Record<string, unknown>;
}

function readRequiredString(
  record: Record<string, unknown>,
  key: string,
  message: string,
): string {
  const value = record[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(message);
  }

  return value;
}

function readOptionalString(
  record: Record<string, unknown>,
  key: string,
  message: string,
): string | undefined {
  const value = record[key];
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error(message);
  }

  return value;
}

export function parseSourceConfig(value: unknown): SourceConfig {
  const config = readConfigObject(value, "source config must be an object");
  const ratePolicy = readRatePolicy(config.ratePolicy);

  if (config.adapter === "fake") {
    return {
      adapter: "fake",
      fixtureId: readRequiredString(
        config,
        "fixtureId",
        "source config fixtureId must be a non-empty string",
      ),
      name: readOptionalString(config, "name", "source config name must be a string"),
      domain: readOptionalString(config, "domain", "source config domain must be a string"),
      startUrl: readRequiredString(
        config,
        "startUrl",
        "source config startUrl must be a non-empty string",
      ),
      ratePolicy,
    };
  }

  if (config.adapter === "firecrawl") {
    return {
      adapter: "firecrawl",
      apiKey: readRequiredString(
        config,
        "apiKey",
        "source config apiKey must be a non-empty string",
      ),
      name: readOptionalString(config, "name", "source config name must be a string"),
      domain: readOptionalString(config, "domain", "source config domain must be a string"),
      startUrl: readRequiredString(
        config,
        "startUrl",
        "source config startUrl must be a non-empty string",
      ),
      ratePolicy,
    };
  }

  throw new Error("source config adapter must be fake or firecrawl");
}

function readRatePolicy(value: unknown): SourceRatePolicy | undefined {
  if (value === undefined) {
    return undefined;
  }

  const record = readConfigObject(value, "source config ratePolicy must be an object");
  const minDelayMs = readOptionalInteger(
    record,
    "minDelayMs",
    "source config ratePolicy minDelayMs must be a non-negative integer",
  );
  const backoffBaseMs = readOptionalInteger(
    record,
    "backoffBaseMs",
    "source config ratePolicy backoffBaseMs must be a non-negative integer",
  );
  const attemptLimit = readOptionalInteger(
    record,
    "attemptLimit",
    "source config ratePolicy attemptLimit must be a positive integer",
    { allowZero: false },
  );

  return {
    minDelayMs,
    backoffBaseMs,
    attemptLimit,
  };
}

function readOptionalInteger(
  record: Record<string, unknown>,
  key: string,
  message: string,
  options: { allowZero?: boolean } = {},
): number | undefined {
  const value = record[key];
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new Error(message);
  }
  if (value < 0 || (value === 0 && options.allowZero === false)) {
    throw new Error(message);
  }

  return value as number;
}

export function readSourceAttemptLimit(sourceConfig: SourceConfig): number {
  return sourceConfig.ratePolicy?.attemptLimit ?? 3;
}

export function readSourceFetchDelayMs(sourceConfig: SourceConfig): number {
  return sourceConfig.ratePolicy?.minDelayMs ?? 0;
}

export function readSourceRetryBackoffMs(sourceConfig: SourceConfig, attemptNumber: number): number {
  const baseDelayMs = sourceConfig.ratePolicy?.backoffBaseMs ?? 1_000;
  if (baseDelayMs === 0) {
    return 0;
  }

  return baseDelayMs * 2 ** Math.max(0, attemptNumber - 1);
}

export function parseSeedSourceRecords(value: unknown): SeedSourceRecord[] {
  if (!Array.isArray(value)) {
    throw new Error("seed file must contain an array of sources");
  }

  return value.map((entry) => {
    const record = readConfigObject(entry, "seed source entry must be an object");

    return {
      id: readRequiredString(record, "id", "seed source id must be a non-empty string"),
      config: parseSourceConfig(record.config),
    };
  });
}

export async function readSeedSourceFile(path: string): Promise<SeedSourceRecord[]> {
  const content = await readFile(path, "utf8");
  return parseSeedSourceRecords(JSON.parse(content));
}
