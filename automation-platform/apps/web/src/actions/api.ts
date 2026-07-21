import { ActionError } from "astro:actions";

const API_BASE =
  process.env.API_INTERNAL_BASE_URL ||
  import.meta.env.PUBLIC_API_BASE_URL ||
  "http://127.0.0.1:8020";

const actionCode = (status: number) => {
  if (status === 401) return "UNAUTHORIZED" as const;
  if (status === 403) return "FORBIDDEN" as const;
  if (status === 404) return "NOT_FOUND" as const;
  if (status === 409) return "CONFLICT" as const;
  if (status === 413) return "CONTENT_TOO_LARGE" as const;
  if (status === 429) return "TOO_MANY_REQUESTS" as const;
  if (status >= 500) return "SERVICE_UNAVAILABLE" as const;
  return "BAD_REQUEST" as const;
};

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const timeout = AbortSignal.timeout(30_000);
  const signal = init?.signal ? AbortSignal.any([init.signal, timeout]) : timeout;
  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      ...init,
      signal,
      headers: { "Content-Type": "application/json", ...init?.headers },
    });
  } catch (error) {
    throw new ActionError({
      code: error instanceof DOMException && error.name === "TimeoutError"
        ? "GATEWAY_TIMEOUT"
        : "SERVICE_UNAVAILABLE",
      message: "The API could not be reached. Check service readiness and try again.",
    });
  }

  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    const detail = Array.isArray(body.detail)
      ? body.detail.map((item: any) => item.msg || item.message || JSON.stringify(item)).join("; ")
      : typeof body.detail === "string"
        ? body.detail
        : body.detail?.message || response.statusText;
    throw new ActionError({ code: actionCode(response.status), message: detail });
  }

  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}
