import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

type SqlExecutor = {
  exec(sql: string): Promise<unknown>;
};

const migrationsDir = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../drizzle",
);

export async function migrate(executor: SqlExecutor) {
  const migrationFiles = (await readdir(migrationsDir))
    .filter((entry) => entry.endsWith(".sql"))
    .sort();

  for (const file of migrationFiles) {
    const migrationSql = await readFile(path.join(migrationsDir, file), "utf8");
    await executor.exec(migrationSql);
  }
}
