import type { APIContext } from "astro";
import { env } from "cloudflare:workers";

export type D1Result<T> = {
  results?: T[];
};

export type D1RunResult = {
  success: boolean;
  meta: {
    changes?: number;
    [key: string]: unknown;
  };
};

export type D1PreparedStatement = {
  bind(...values: unknown[]): D1PreparedStatement;
  first<T = unknown>(): Promise<T | null>;
  all<T = unknown>(): Promise<D1Result<T>>;
  run(): Promise<D1RunResult>;
};

export type D1Database = {
  prepare(query: string): D1PreparedStatement;
};

type RuntimeLocals = APIContext["locals"] & {
  runtime?: {
    env?: {
      DB?: D1Database;
    };
  };
};

type WorkerEnv = {
  DB?: D1Database;
};

export function getDb(context: APIContext): D1Database {
  const importedEnv = env as WorkerEnv;
  if (importedEnv.DB) {
    return importedEnv.DB;
  }

  try {
    const runtimeDb = (context.locals as RuntimeLocals).runtime?.env?.DB;
    if (runtimeDb) return runtimeDb;
  } catch {
    // Astro Cloudflare runtime may throw when locals are not backed by Workers.
  }

  throw new Error("Cloudflare D1 binding DB is not available.");
}
