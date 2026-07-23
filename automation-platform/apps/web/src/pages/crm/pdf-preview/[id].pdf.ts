import type { APIRoute } from "astro";

export const prerender = false;

const API_BASE =
  process.env.API_INTERNAL_BASE_URL ||
  import.meta.env.PUBLIC_API_BASE_URL ||
  "http://127.0.0.1:8020";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const FORWARDED_HEADERS = [
  "accept-ranges",
  "cache-control",
  "content-length",
  "content-range",
  "etag",
  "last-modified",
] as const;

export const GET: APIRoute = async ({ params, request }) => {
  const artifactId = params.id || "";
  if (!UUID.test(artifactId)) {
    return new Response("Invalid PDF artifact identifier.", { status: 400 });
  }

  const headers = new Headers({ Accept: "application/pdf" });
  const range = request.headers.get("range");
  if (range) headers.set("Range", range);
  const upstream = await fetch(
    `${API_BASE}/api/v1/crm/official-letters/artifacts/${artifactId}/preview`,
    { headers, signal: AbortSignal.timeout(30_000) },
  );
  if (!upstream.ok || !upstream.body) {
    const detail = await upstream.text().catch(() => "PDF preview is unavailable.");
    return new Response(detail, {
      status: upstream.status || 502,
      headers: { "Content-Type": upstream.headers.get("content-type") || "text/plain" },
    });
  }
  if (!(upstream.headers.get("content-type") || "").toLowerCase().startsWith("application/pdf")) {
    return new Response("The preview service returned a non-PDF response.", { status: 502 });
  }

  const responseHeaders = new Headers({
    "Content-Type": "application/pdf",
    "Content-Disposition": `inline; filename="official-letter-${artifactId}.pdf"`,
  });
  for (const name of FORWARDED_HEADERS) {
    const value = upstream.headers.get(name);
    if (value) responseHeaders.set(name, value);
  }
  const contentRange = upstream.headers.get("content-range");
  if (contentRange) responseHeaders.set("content-range", contentRange);
  return new Response(upstream.body, {
    status: upstream.status,
    headers: responseHeaders,
  });
};
