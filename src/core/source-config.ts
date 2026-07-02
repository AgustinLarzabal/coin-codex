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

export function parseSourceConfig(value: unknown): SourceConfig {
  if (!value || typeof value !== "object") {
    throw new Error("source config must be an object");
  }

  const config = value as Record<string, unknown>;
  if (config.adapter !== "fake") {
    throw new Error("source config adapter must be fake");
  }
  if (typeof config.fixtureId !== "string" || config.fixtureId.length === 0) {
    throw new Error("source config fixtureId must be a non-empty string");
  }
  if (typeof config.startUrl !== "string" || config.startUrl.length === 0) {
    throw new Error("source config startUrl must be a non-empty string");
  }
  if (config.name !== undefined && typeof config.name !== "string") {
    throw new Error("source config name must be a string");
  }
  if (config.domain !== undefined && typeof config.domain !== "string") {
    throw new Error("source config domain must be a string");
  }

  return {
    adapter: "fake",
    fixtureId: config.fixtureId,
    name: config.name as string | undefined,
    domain: config.domain as string | undefined,
    startUrl: config.startUrl,
  };
}

export function parseSeedSourceRecords(value: unknown): SeedSourceRecord[] {
  if (!Array.isArray(value)) {
    throw new Error("seed file must contain an array of sources");
  }

  return value.map((entry) => {
    if (!entry || typeof entry !== "object") {
      throw new Error("seed source entry must be an object");
    }

    const record = entry as Record<string, unknown>;
    if (typeof record.id !== "string" || record.id.length === 0) {
      throw new Error("seed source id must be a non-empty string");
    }

    return {
      id: record.id,
      config: parseSourceConfig(record.config),
    };
  });
}
