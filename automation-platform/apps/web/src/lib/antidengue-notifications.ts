import { Howl, Howler } from "howler";

export type Milestone = "started" | "report_downloaded" | "messages_sent";
export type SoundName = "chime" | "pulse" | "bell" | "soft";

export interface SoundPreference {
  enabled: boolean;
  sound: SoundName;
}

export interface AntiDengueSoundSettings {
  version: 1;
  enabled: boolean;
  volume: number;
  milestones: Record<Milestone, SoundPreference>;
}

export const SOUND_OPTIONS: Array<{ value: SoundName; label: string }> = [
  { value: "chime", label: "Bright chime" },
  { value: "pulse", label: "Short pulse" },
  { value: "bell", label: "Clear bell" },
  { value: "soft", label: "Soft tone" },
];

export const DEFAULT_SOUND_SETTINGS: AntiDengueSoundSettings = {
  version: 1,
  enabled: false,
  volume: 0.7,
  milestones: {
    started: { enabled: true, sound: "pulse" },
    report_downloaded: { enabled: true, sound: "chime" },
    messages_sent: { enabled: true, sound: "bell" },
  },
};

const SETTINGS_KEY = "antidengue.sound-settings.v1";
const PLAYED_PREFIX = "antidengue.notification.played.";
const EVENT_TO_MILESTONE: Record<string, Milestone> = {
  "antidengue.execution.started": "started",
  "antidengue.report.downloaded": "report_downloaded",
  "antidengue.messages.sent": "messages_sent",
};
const sounds = new Map<SoundName, Howl>();
let listenerStarted = false;

export function loadSoundSettings(): AntiDengueSoundSettings {
  try {
    const saved = JSON.parse(localStorage.getItem(SETTINGS_KEY) || "null");
    if (!saved || saved.version !== 1) return structuredClone(DEFAULT_SOUND_SETTINGS);
    return {
      ...structuredClone(DEFAULT_SOUND_SETTINGS),
      ...saved,
      milestones: {
        ...structuredClone(DEFAULT_SOUND_SETTINGS.milestones),
        ...(saved.milestones || {}),
      },
    };
  } catch {
    return structuredClone(DEFAULT_SOUND_SETTINGS);
  }
}

export function saveSoundSettings(settings: AntiDengueSoundSettings): void {
  const normalized = { ...settings, volume: Math.max(0, Math.min(1, settings.volume)) };
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(normalized));
  window.dispatchEvent(new CustomEvent("antidengue:sound-settings", { detail: normalized }));
}

function wavUrl(notes: Array<[number, number]>, gain = 0.3): string {
  const rate = 22050;
  const gap = 0.035;
  const duration = notes.reduce((total, [, seconds]) => total + seconds + gap, 0);
  const samples = Math.ceil(rate * duration);
  const buffer = new ArrayBuffer(44 + samples * 2);
  const view = new DataView(buffer);
  const write = (offset: number, value: string) => [...value].forEach((char, i) => view.setUint8(offset + i, char.charCodeAt(0)));
  write(0, "RIFF"); view.setUint32(4, 36 + samples * 2, true); write(8, "WAVEfmt ");
  view.setUint32(16, 16, true); view.setUint16(20, 1, true); view.setUint16(22, 1, true);
  view.setUint32(24, rate, true); view.setUint32(28, rate * 2, true); view.setUint16(32, 2, true);
  view.setUint16(34, 16, true); write(36, "data"); view.setUint32(40, samples * 2, true);
  let sampleIndex = 0;
  for (const [frequency, seconds] of notes) {
    const count = Math.floor(seconds * rate);
    for (let i = 0; i < count; i++, sampleIndex++) {
      const envelope = Math.min(1, i / (rate * 0.015)) * Math.max(0, 1 - i / count);
      view.setInt16(44 + sampleIndex * 2, Math.sin(2 * Math.PI * frequency * i / rate) * 32767 * gain * envelope, true);
    }
    sampleIndex += Math.floor(gap * rate);
  }
  return URL.createObjectURL(new Blob([buffer], { type: "audio/wav" }));
}

function sound(name: SoundName): Howl {
  const cached = sounds.get(name);
  if (cached) return cached;
  const patterns: Record<SoundName, Array<[number, number]>> = {
    chime: [[660, 0.12], [880, 0.18]],
    pulse: [[440, 0.12]],
    bell: [[784, 0.15], [1047, 0.28]],
    soft: [[523, 0.25]],
  };
  const instance = new Howl({ src: [wavUrl(patterns[name])], format: ["wav"], preload: true });
  sounds.set(name, instance);
  return instance;
}

export function previewSound(name: SoundName, volume?: number): void {
  Howler.volume(volume ?? loadSoundSettings().volume);
  sound(name).play();
}

async function claimEvent(eventId: string): Promise<boolean> {
  const claim = async () => {
    const key = `${PLAYED_PREFIX}${eventId}`;
    if (localStorage.getItem(key)) return false;
    localStorage.setItem(key, String(Date.now()));
    return true;
  };
  if (navigator.locks?.request) {
    return navigator.locks.request("antidengue-notification-audio", claim);
  }
  return claim();
}

async function handleNotification(event: MessageEvent): Promise<void> {
  const payload = JSON.parse(event.data || "{}");
  const milestone = EVENT_TO_MILESTONE[payload.type];
  if (!milestone || !payload.id || !(await claimEvent(payload.id))) return;
  const settings = loadSoundSettings();
  const preference = settings.milestones[milestone];
  if (!settings.enabled || !preference?.enabled) return;
  previewSound(preference.sound, settings.volume);
  window.dispatchEvent(new CustomEvent("antidengue:notification", { detail: payload }));
}

export function startAntiDengueNotificationListener(): void {
  if (listenerStarted || typeof EventSource === "undefined") return;
  listenerStarted = true;
  const api = import.meta.env.PUBLIC_API_BASE_URL || "http://127.0.0.1:8020";
  const source = new EventSource(`${api}/api/v1/antidengue/notification-events`);
  source.addEventListener("notification", (event) => void handleNotification(event as MessageEvent));
}
