import type { APIContext } from "astro";
import { env } from "cloudflare:workers";

type RuntimeLocals = APIContext["locals"] & {
  runtime?: {
    env?: ImageKitEnv;
  };
};

type ImageKitEnv = {
  IMAGEKIT_PUBLIC_KEY?: string;
  IMAGEKIT_PRIVATE_KEY?: string;
  IMAGEKIT_URL_ENDPOINT?: string;
};

type ImageKitUploadResponse = {
  url?: string;
  message?: string;
};

const maxImageBytes = 2 * 1024 * 1024;

function cleanText(value: unknown): string | null {
  if (typeof value !== "string" && typeof value !== "number") return null;

  const cleaned = String(value).trim();
  return cleaned.length > 0 ? cleaned : null;
}

function getRuntimeEnv(context: APIContext): ImageKitEnv {
  const importedEnv = env as ImageKitEnv;
  if (importedEnv.IMAGEKIT_PRIVATE_KEY) return importedEnv;

  try {
    return ((context.locals as RuntimeLocals).runtime?.env ?? {}) as ImageKitEnv;
  } catch {
    return {};
  }
}

function getImageKitConfig(context: APIContext): {
  privateKey: string;
} {
  const runtimeEnv = getRuntimeEnv(context);
  const privateKey = cleanText(runtimeEnv.IMAGEKIT_PRIVATE_KEY);

  if (!privateKey) {
    throw new Error("ImageKit environment variables are not configured.");
  }

  return { privateKey };
}

function sanitizeFileName(fileName: string, prefix: string): string {
  const cleaned = fileName
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .replace(/_{2,}/g, "_")
    .slice(0, 120);

  if (cleaned.includes(".")) {
    return `${prefix}-${cleaned}`;
  }

  return `${prefix}-${cleaned || "survey-image"}.jpg`;
}

export function validateBase64Image(value: string | null, label: string): string {
  if (!value) {
    throw new RangeError(`${label} is required.`);
  }

  const normalized = value.trim();
  const hasDataUrlPrefix = /^data:image\/(png|jpe?g|webp);base64,/i.test(normalized);
  const payload = hasDataUrlPrefix ? normalized.split(",", 2)[1] : normalized;
  const approximateBytes = Math.ceil((payload.length * 3) / 4);

  if (approximateBytes > maxImageBytes) {
    throw new RangeError(`${label} must be 2 MB or smaller.`);
  }

  if (!/^[a-zA-Z0-9+/]+={0,2}$/.test(payload)) {
    throw new RangeError(`${label} must be a valid JPG, PNG, or WEBP image.`);
  }

  return hasDataUrlPrefix ? normalized : payload;
}

export async function uploadSurveyImage(
  context: APIContext,
  base64Image: string,
  fileName: string,
  prefix: string,
): Promise<string> {
  const imagekit = getImageKitConfig(context);
  const formData = new FormData();

  formData.append("file", base64Image);
  formData.append("fileName", sanitizeFileName(fileName, prefix));
  formData.append("folder", "/private-institutes-survey-july-2026");
  formData.append("useUniqueFileName", "true");
  formData.append("tags", `private-institutes-survey-july-2026,${prefix}`);
  formData.append("isPublished", "true");

  const uploadResponse = await fetch("https://upload.imagekit.io/api/v1/files/upload", {
    method: "POST",
    headers: {
      authorization: `Basic ${btoa(`${imagekit.privateKey}:`)}`,
    },
    body: formData,
  });
  const uploadResult = (await uploadResponse.json()) as ImageKitUploadResponse;

  if (!uploadResponse.ok) {
    throw new Error(uploadResult.message ?? "ImageKit upload failed.");
  }

  if (!uploadResult.url) {
    throw new Error("ImageKit upload did not return a URL.");
  }

  return uploadResult.url;
}
