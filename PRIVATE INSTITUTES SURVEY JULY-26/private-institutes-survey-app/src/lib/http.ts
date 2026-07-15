export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

export async function parseJsonBody<T extends Record<string, unknown>>(
  request: Request,
): Promise<T> {
  const contentType = request.headers.get("content-type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new SyntaxError("Request body must be JSON.");
  }

  const body = await request.json();
  if (typeof body !== "object" || body === null) {
    throw new SyntaxError("Request body must be a JSON object.");
  }

  return body as T;
}
