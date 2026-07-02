import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import process from "node:process";

import { createAppContext } from "./core/context.js";
import type { FirecrawlClient } from "./core/providers/firecrawl-provider.js";
import { parseSeedSourceRecords } from "./core/source-config.js";

const DEFAULT_SOURCE_ID = "src_fixture";
const DEFAULT_SCOPE = "default";

type CliDeps = {
  databaseUrl?: string;
  firecrawlClientFactory?: () => FirecrawlClient;
};

function readFlag(args: string[], name: string): string | undefined {
  const index = args.indexOf(name);
  if (index === -1) {
    return undefined;
  }

  return args[index + 1];
}

function readRequiredFlag(args: string[], name: string, command: string): string {
  const value = readFlag(args, name);
  if (!value) {
    throw new Error(`${command} requires ${name}`);
  }

  return value;
}

function readIntegerFlag(args: string[], name: string, fallback: number): number {
  const value = readFlag(args, name);
  if (value === undefined) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return parsed;
}

async function readSeedFile(path: string) {
  const content = await readFile(path, "utf8");
  return parseSeedSourceRecords(JSON.parse(content));
}

export async function executeCli(argv: string[], deps: CliDeps = {}): Promise<string> {
  const [command] = argv;
  if (!command) {
    throw new Error("missing command");
  }

  const context = await createAppContext({
    databaseUrl: deps.databaseUrl ?? process.env.DATABASE_URL,
    firecrawlClientFactory: deps.firecrawlClientFactory,
  });

  try {
    switch (command) {
      case "seed-sources": {
        const records = await readSeedFile(readRequiredFlag(argv, "--file", command));
        const output = await context.ingestionService.seedSources(records);
        return JSON.stringify(output, null, 2);
      }
      case "create-run": {
        const output = await context.ingestionService.createRun({
          runId: readFlag(argv, "--run-id") ?? randomUUID(),
          sourceId: readFlag(argv, "--source-id") ?? DEFAULT_SOURCE_ID,
          scope: readFlag(argv, "--scope") ?? DEFAULT_SCOPE,
          detailLimit: readIntegerFlag(argv, "--detail-limit", 10),
        });
        return JSON.stringify(output, null, 2);
      }
      case "run-worker": {
        const output = await context.worker.runOnce();
        return JSON.stringify(output, null, 2);
      }
      case "inspect-run":
        return context.inspector.inspectRun(
          readRequiredFlag(argv, "--run-id", command),
          { debugPrivate: argv.includes("--debug-private") },
        );
      default:
        throw new Error(`unknown command: ${command}`);
    }
  } finally {
    await context.close();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  executeCli(process.argv.slice(2))
    .then((output) => {
      process.stdout.write(`${output}\n`);
    })
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(`${message}\n`);
      process.exitCode = 1;
    });
}
