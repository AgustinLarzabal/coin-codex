import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline/promises";
import process from "node:process";

import { createAppContext } from "./core/context.js";
import {
  DEFAULT_OPERATOR_CONSOLE_SEED_FILE,
  runOperatorConsole,
  type OperatorConsolePrompt,
} from "./core/operator-console.js";
import type { FirecrawlClient } from "./core/providers/firecrawl-provider.js";
import type { ImageProvider } from "./core/providers/image-provider.js";
import { readSeedSourceFile } from "./core/source-config.js";

const DEFAULT_SOURCE_ID = "src_fixture";
const DEFAULT_SCOPE = "default";

type CliDeps = {
  databaseUrl?: string;
  firecrawlClientFactory?: () => FirecrawlClient;
  imageProviderFactory?: () => ImageProvider;
  operatorConsolePrompt?: OperatorConsolePrompt;
};

export function formatOperatorConsolePrompt(input: {
  label: string;
  defaultValue?: string;
  options?: string[];
}): string {
  const defaultSuffix = input.defaultValue ? ` [${input.defaultValue}]` : "";
  const optionsSuffix =
    input.options && input.options.length > 0
      ? ` (options: ${input.options.join(", ")})`
      : "";

  return `${input.label}${defaultSuffix}${optionsSuffix}: `;
}

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

function createReadlineOperatorConsolePrompt(): OperatorConsolePrompt {
  const readline = createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  return {
    async text(input) {
      return readline.question(formatOperatorConsolePrompt(input));
    },
    async close() {
      readline.close();
    },
  };
}

export async function executeCli(argv: string[], deps: CliDeps = {}): Promise<string> {
  const [command] = argv;
  if (!command) {
    throw new Error("missing command");
  }

  const context = await createAppContext({
    databaseUrl: deps.databaseUrl ?? process.env.DATABASE_URL,
    firecrawlClientFactory: deps.firecrawlClientFactory,
    imageProviderFactory: deps.imageProviderFactory,
  });

  try {
    switch (command) {
      case "seed-sources": {
        const records = await readSeedSourceFile(readRequiredFlag(argv, "--file", command));
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
      case "operator-console": {
        const prompt = deps.operatorConsolePrompt ?? createReadlineOperatorConsolePrompt();
        try {
          return await runOperatorConsole({
            ingestionService: context.ingestionService,
            worker: context.worker,
            inspector: context.inspector,
            prompt,
            initialSeedFilePath:
              readFlag(argv, "--seed-file") ?? DEFAULT_OPERATOR_CONSOLE_SEED_FILE,
          });
        } finally {
          await prompt.close?.();
        }
      }
      case "run-worker": {
        const output = await context.worker.runOnce();
        return JSON.stringify(output, null, 2);
      }
      case "inspect-run":
        return await context.inspector.inspectRun(
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
