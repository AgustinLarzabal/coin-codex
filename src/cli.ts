import { randomUUID } from "node:crypto";
import process from "node:process";

import { createAppContext } from "./core/context.js";

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

export async function executeCli(argv: string[], deps: CliDeps = {}): Promise<string> {
  const [command] = argv;
  if (!command) {
    throw new Error("missing command");
  }

  const context = await createAppContext({
    databaseUrl: deps.databaseUrl ?? process.env.DATABASE_URL,
  });

  try {
    if (command === "create-run") {
      const sourceId = readFlag(argv, "--source-id") ?? "src_fixture";
      const scope = readFlag(argv, "--scope") ?? "default";
      const fixtureId = readFlag(argv, "--fixture") ?? "fixture-coin";
      const runId = readFlag(argv, "--run-id") ?? randomUUID();
      const output = await context.ingestionService.createRun({
        runId,
        sourceId,
        scope,
        fixtureId,
      });
      return JSON.stringify(output, null, 2);
    }

    if (command === "run-worker") {
      const output = await context.worker.runOnce();
      return JSON.stringify(output, null, 2);
    }

    if (command === "inspect-run") {
      const runId = readFlag(argv, "--run-id");
      if (!runId) {
        throw new Error("inspect-run requires --run-id");
      }
      return context.inspector.inspectRun(runId);
    }

    throw new Error(`unknown command: ${command}`);
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
