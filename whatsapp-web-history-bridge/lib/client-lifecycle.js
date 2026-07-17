function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function initializeClientWithRecovery({
  createClient,
  disposeClient,
  isTransient,
  attempts = 3,
  retryDelayMs = 1000,
  onAttempt = () => {},
  onRetry = () => {},
}) {
  const total = Math.max(1, Number(attempts) || 1);
  let lastError = null;

  for (let attempt = 1; attempt <= total; attempt += 1) {
    const client = await createClient({ attempt, total });
    onAttempt({ attempt, total, client });
    try {
      await client.initialize();
      return client;
    } catch (error) {
      lastError = error;
      const retry = attempt < total && Boolean(isTransient(error));
      await disposeClient(client, { attempt, total, retry, error });
      if (!retry) throw error;
      onRetry({ attempt, total, error });
      await delay(retryDelayMs * attempt);
    }
  }
  throw lastError || new Error("WhatsApp Web client initialization failed");
}
