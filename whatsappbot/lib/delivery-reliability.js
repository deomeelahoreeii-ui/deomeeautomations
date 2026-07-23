export const QueueFailureAction = Object.freeze({
  PAUSE_CONNECTION: "pause_connection",
  RETRY_MESSAGE: "retry_message",
  TERMINATE_MESSAGE: "terminate_message",
});

const TRANSPORT_UNAVAILABLE_PATTERNS = [
  /connection closed/i,
  /connection was lost/i,
  /connection lost/i,
  /socket (?:is )?(?:closed|not open)/i,
  /not connected/i,
  /stream (?:is )?closed/i,
];

export function isWhatsAppTransportUnavailable(error) {
  const message = String(error?.message || error || "");
  return TRANSPORT_UNAVAILABLE_PATTERNS.some((pattern) => pattern.test(message));
}

export function classifyQueueFailure({
  error,
  deliveryCount,
  maxDeliver,
  permanent,
}) {
  // A dead socket is a worker-level outage, not a message-level failure.  The
  // consumer must pause so all unacknowledged messages survive for the next
  // healthy socket instead of burning their delivery budget against one outage.
  if (isWhatsAppTransportUnavailable(error)) {
    return QueueFailureAction.PAUSE_CONNECTION;
  }
  if (permanent || deliveryCount >= maxDeliver) {
    return QueueFailureAction.TERMINATE_MESSAGE;
  }
  return QueueFailureAction.RETRY_MESSAGE;
}
