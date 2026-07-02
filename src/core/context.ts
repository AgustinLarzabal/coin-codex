import { PGlite } from "@electric-sql/pglite";

import { migrate } from "../db/migrate.js";
import { createDatabase, resolveDatabase } from "../db/setup.js";
import { IngestionInspector } from "./inspector.js";
import { IngestionService } from "./ingestion-service.js";
import { AppCrawlProvider } from "./providers/app-crawl-provider.js";
import {
  HttpFirecrawlClient,
  type FirecrawlClient,
} from "./providers/firecrawl-provider.js";
import { Worker } from "./worker.js";

type AppContextOptions = {
  databaseUrl?: string;
  firecrawlClientFactory?: () => FirecrawlClient;
};

export async function createAppContext(options: AppContextOptions) {
  const databaseUrl = options.databaseUrl;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const existingDatabase = resolveDatabase(databaseUrl);
  const client = existingDatabase ?? new PGlite(databaseUrl);
  await migrate(client);
  const db = createDatabase(client);
  const crawlProvider = new AppCrawlProvider(
    options.firecrawlClientFactory?.() ?? new HttpFirecrawlClient(),
  );
  const ingestionService = new IngestionService(db);
  const worker = new Worker(db, crawlProvider);
  const inspector = new IngestionInspector(db);

  return {
    db,
    ingestionService,
    worker,
    inspector,
    async close() {
      if (!existingDatabase) {
        await client.close();
      }
    },
  };
}
