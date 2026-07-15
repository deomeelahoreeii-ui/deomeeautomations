const CACHE_KEYS = {
  PARTICIPANTS: 'participants.v2',
  PARTICIPANT_INDEX: 'participantIndex.v2',
  USERS: 'users.v2',
  ABSENCE_REASONS: 'absenceReasons.v1',
  BOOTSTRAP_BASE: 'bootstrapBase.v3',
  DASHBOARD_SUMMARY: 'dashboardSummary.v2'
};

function getCache_() {
  return CacheService.getScriptCache();
}

function cacheGetJson_(key) {
  const cache = getCache_();
  const metaRaw = cache.get(key + ':meta');
  if (!metaRaw) return null;

  try {
    const meta = JSON.parse(metaRaw);
    const parts = [];
    for (let i = 0; i < meta.chunks; i++) {
      const part = cache.get(key + ':' + i);
      if (part == null) return null;
      parts.push(part);
    }
    return JSON.parse(parts.join(''));
  } catch (error) {
    return null;
  }
}

function cachePutJson_(key, value, ttlSeconds) {
  const cache = getCache_();
  const json = JSON.stringify(value);
  const chunkSize = 85000;
  const chunks = [];
  for (let i = 0; i < json.length; i += chunkSize) {
    chunks.push(json.slice(i, i + chunkSize));
  }

  const payload = {};
  payload[key + ':meta'] = JSON.stringify({ chunks: chunks.length });
  chunks.forEach((chunk, i) => payload[key + ':' + i] = chunk);
  cache.putAll(payload, ttlSeconds || CONFIG.CACHE_TTL_SECONDS);
}

function cacheRemove_(key) {
  const cache = getCache_();
  const metaRaw = cache.get(key + ':meta');
  const keys = [key + ':meta'];
  if (metaRaw) {
    try {
      const meta = JSON.parse(metaRaw);
      for (let i = 0; i < meta.chunks; i++) keys.push(key + ':' + i);
    } catch (error) {
      for (let i = 0; i < 20; i++) keys.push(key + ':' + i);
    }
  }
  cache.removeAll(keys);
}

function clearDataCaches_() {
  Object.keys(CACHE_KEYS).forEach(name => cacheRemove_(CACHE_KEYS[name]));
}

function clearParticipantCaches_() {
  cacheRemove_(CACHE_KEYS.PARTICIPANTS);
  cacheRemove_(CACHE_KEYS.PARTICIPANT_INDEX);
  cacheRemove_(CACHE_KEYS.BOOTSTRAP_BASE);
}

function clearUserCaches_() {
  cacheRemove_(CACHE_KEYS.USERS);
  cacheRemove_(CACHE_KEYS.BOOTSTRAP_BASE);
}
