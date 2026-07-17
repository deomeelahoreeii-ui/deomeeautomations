import test from "node:test";
import assert from "node:assert/strict";
import { initializeClientWithRecovery } from "../lib/client-lifecycle.js";

test("transient initialization failure recreates and disposes the client", async () => {
  const created = [];
  const disposed = [];
  const client = await initializeClientWithRecovery({
    attempts: 3,
    retryDelayMs: 1,
    isTransient: (error) => /target closed/i.test(error.message),
    createClient: ({ attempt }) => {
      const candidate = {
        attempt,
        async initialize() {
          if (attempt === 1) throw new Error("Target closed");
        },
      };
      created.push(candidate);
      return candidate;
    },
    disposeClient: async (candidate) => disposed.push(candidate),
  });
  assert.equal(client.attempt, 2);
  assert.equal(created.length, 2);
  assert.deepEqual(disposed, [created[0]]);
});

test("non-transient initialization failures are not retried", async () => {
  let creates = 0;
  await assert.rejects(() => initializeClientWithRecovery({
    attempts: 3,
    retryDelayMs: 1,
    isTransient: () => false,
    createClient: () => ({
      async initialize() {
        creates += 1;
        throw new Error("authentication rejected");
      },
    }),
    disposeClient: async () => {},
  }), /authentication rejected/);
  assert.equal(creates, 1);
});
