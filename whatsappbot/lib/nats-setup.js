import { RetentionPolicy, StorageType } from "nats";

function isNotFoundError(error) {
  const message = String(error?.message || "").toLowerCase();
  return message.includes("not found") || message.includes("404");
}

function subjectMatches(pattern, subject) {
  const patternTokens = String(pattern || "").split(".");
  const subjectTokens = String(subject || "").split(".");

  for (let index = 0; index < patternTokens.length; index += 1) {
    const patternToken = patternTokens[index];

    if (patternToken === ">") {
      return true;
    }

    if (subjectTokens[index] == null) {
      return false;
    }

    if (patternToken !== "*" && patternToken !== subjectTokens[index]) {
      return false;
    }
  }

  return patternTokens.length === subjectTokens.length;
}

async function findStreamOwningSubject(jsm, subject) {
  for await (const stream of jsm.streams.list()) {
    const subjects = stream.config?.subjects || [];
    if (subjects.some((candidate) => subjectMatches(candidate, subject))) {
      return stream.config.name;
    }
  }

  return null;
}

async function ensureStream(jsm, logger, streamName, config) {
  const subject = config.subjects?.[0];

  try {
    await jsm.streams.info(streamName);
    await jsm.streams.update(streamName, config);
    logger.info("Verified NATS stream", {
      stream: streamName,
      subjects: config.subjects,
    });
    return streamName;
  } catch (error) {
    if (!isNotFoundError(error)) {
      throw error;
    }

    const owningStream = subject
      ? await findStreamOwningSubject(jsm, subject)
      : null;

    if (owningStream) {
      logger.warn("Using existing NATS stream that already owns subject", {
        configuredStream: streamName,
        existingStream: owningStream,
        subject,
      });
      return owningStream;
    }

    try {
      await jsm.streams.add({
        name: streamName,
        ...config,
      });
    } catch (addError) {
      const message = String(addError?.message || "").toLowerCase();
      if (!subject || !message.includes("subjects overlap")) {
        throw addError;
      }

      const overlappedStream = await findStreamOwningSubject(jsm, subject);
      if (!overlappedStream) {
        throw addError;
      }

      logger.warn("Recovered from NATS stream subject overlap", {
        configuredStream: streamName,
        existingStream: overlappedStream,
        subject,
      });
      return overlappedStream;
    }

    logger.info("Created NATS stream", {
      stream: streamName,
      subjects: config.subjects,
    });
    return streamName;
  }
}

async function ensureConsumer(jsm, options) {
  const {
    ackPolicy,
    ackWaitNanos,
    consumerName,
    logger,
    maxDeliver,
    streamName,
  } = options;

  const consumerConfig = {
    ack_policy: ackPolicy,
    ack_wait: ackWaitNanos,
    durable_name: consumerName,
    max_deliver: maxDeliver,
  };
  const consumerUpdateConfig = {
    ack_policy: ackPolicy,
    ack_wait: ackWaitNanos,
    max_deliver: maxDeliver,
  };

  try {
    await jsm.consumers.info(streamName, consumerName);
    await jsm.consumers.update(streamName, consumerName, consumerUpdateConfig);
    logger.info("Verified NATS consumer", {
      stream: streamName,
      consumer: consumerName,
    });
  } catch (error) {
    if (!isNotFoundError(error)) {
      throw error;
    }

    await jsm.consumers.add(streamName, consumerConfig);
    logger.info("Created NATS consumer", {
      stream: streamName,
      consumer: consumerName,
    });
  }
}

export async function ensureJetStreamPublisherResources(jsm, options) {
  const {
    deadLetterMaxMessages,
    deadLetterStream,
    deadLetterSubject,
    logger,
    streamName,
    subject,
  } = options;

  const resolvedStreamName = await ensureStream(jsm, logger, streamName, {
    retention: RetentionPolicy.Workqueue,
    storage: StorageType.File,
    subjects: [subject],
  });

  const resolvedDeadLetterStream = await ensureStream(jsm, logger, deadLetterStream, {
    max_msgs: deadLetterMaxMessages,
    retention: RetentionPolicy.Limits,
    storage: StorageType.File,
    subjects: [deadLetterSubject],
  });

  return {
    deadLetterStream: resolvedDeadLetterStream,
    streamName: resolvedStreamName,
  };
}

export async function ensureJetStreamWorkerResources(jsm, options) {
  const resolved = await ensureJetStreamPublisherResources(jsm, options);
  await ensureConsumer(jsm, {
    ...options,
    streamName: resolved.streamName,
  });
  return resolved;
}
