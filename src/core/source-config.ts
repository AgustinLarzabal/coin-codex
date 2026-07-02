export type SourceConfig = {
  adapter: "fake";
  fixtureId: string;
  name?: string;
  domain?: string;
  startUrl: string;
};

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
  if (config.adapter !== "fake") {
    throw new Error("source config adapter must be fake");
  }

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
  };
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
