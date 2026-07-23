import assert from "node:assert/strict";
import test from "node:test";

import {
  classifyQueueFailure,
  isWhatsAppTransportUnavailable,
  QueueFailureAction,
} from "../lib/delivery-reliability.js";
import { resolveJobPayload } from "../lib/jobs.js";


test("dead WhatsApp transport pauses the consumer before retry budget is burned", () => {
  const error = new Error("Connection Closed");

  assert.equal(isWhatsAppTransportUnavailable(error), true);
  assert.equal(
    classifyQueueFailure({
      error,
      deliveryCount: 5,
      maxDeliver: 5,
      permanent: false,
    }),
    QueueFailureAction.PAUSE_CONNECTION,
  );
});


test("message validation still terminates only the invalid message", () => {
  assert.equal(
    classifyQueueFailure({
      error: new Error("Missing file: report.pdf"),
      deliveryCount: 1,
      maxDeliver: 5,
      permanent: true,
    }),
    QueueFailureAction.TERMINATE_MESSAGE,
  );
});


test("dispatch attempt identity survives queue payload normalization", () => {
  const payload = resolveJobPayload({
    job_id: "delivery-1",
    dispatch_attempt_id: "delivery-1:attempt:retry-job-2",
    target: "923001234567@s.whatsapp.net",
    type: "contact",
    text: "Test",
  });

  assert.equal(payload.jobId, "delivery-1");
  assert.equal(
    payload.dispatchAttemptId,
    "delivery-1:attempt:retry-job-2",
  );
});
