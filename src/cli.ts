import { randomUUID } from "node:crypto";
import process from "node:process";

import { createAppContext } from "./core/context.js";

const DEFAULT_SOURCE_ID = "src_fixture";
const DEFAULT_SCOPE = "default";
const DEFAULT_FIXTURE_ID = "fixture-coin";

type CliDeps = {
  databaseUrl?: string;
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

export async function executeCli(argv: string[], deps: CliDeps = {}): Promise<string> {
  const [command] = argv;
  if (!command) {
    throw new Error("missing command");
  }

  const context = await createAppContext({
    databaseUrl: deps.databaseUrl ?? process.env.DATABASE_URL,
  });

  try {
    switch (command) {
      case "create-run": {
        const output = await context.ingestionService.createRun({
          runId: readFlag(argv, "--run-id") ?? randomUUID(),
          sourceId: readFlag(argv, "--source-id") ?? DEFAULT_SOURCE_ID,
          scope: readFlag(argv, "--scope") ?? DEFAULT_SCOPE,
          fixtureId: readFlag(argv, "--fixture") ?? DEFAULT_FIXTURE_ID,
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
