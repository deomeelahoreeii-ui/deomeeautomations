const levels = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function getActiveLevel() {
  const value = String(process.env.LOG_LEVEL || "info").toLowerCase();
  return levels[value] ?? levels.info;
}

function stringifyMeta(meta) {
  if (!meta || Object.keys(meta).length === 0) {
    return "";
  }

  return ` ${JSON.stringify(meta)}`;
}

export function createLogger(scope) {
  function shouldLog(level) {
    return levels[level] >= getActiveLevel();
  }

  function write(level, message, meta = {}) {
    if (!shouldLog(level)) {
      return;
    }

    const line = `[${new Date().toISOString()}] [${scope}] [${level.toUpperCase()}] ${message}${stringifyMeta(meta)}`;

    if (level === "error") {
      console.error(line);
      return;
    }

    if (level === "warn") {
      console.warn(line);
      return;
    }

    console.log(line);
  }

  return {
    debug(message, meta) {
      write("debug", message, meta);
    },
    error(message, meta) {
      write("error", message, meta);
    },
    info(message, meta) {
      write("info", message, meta);
    },
    warn(message, meta) {
      write("warn", message, meta);
    },
  };
}
