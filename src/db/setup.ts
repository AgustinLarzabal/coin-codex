import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";

import * as schema from "./schema.js";

type DatabaseClient = PGlite;

const testDatabases = new Map<string, DatabaseClient>();

export function createDatabase(client: DatabaseClient) {
  return drizzle(client, { schema });
}

export type Database = ReturnType<typeof createDatabase>;

export function registerDatabase(databaseUrl: string, client: DatabaseClient) {
  testDatabases.set(databaseUrl, client);
}

export function resolveDatabase(databaseUrl: string): DatabaseClient | undefined {
  return testDatabases.get(databaseUrl);
}

export function unregisterDatabase(databaseUrl: string) {
  testDatabases.delete(databaseUrl);
}
