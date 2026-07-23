import { defineMiddleware } from "astro:middleware";

export const onRequest = defineMiddleware(async (context, next) => {
  const response = await next();
  response.headers.set("x-content-type-options", "nosniff");
  if (context.url.pathname.startsWith("/crm/pdf-preview/")) {
    response.headers.set("x-frame-options", "SAMEORIGIN");
    response.headers.set("content-security-policy", "frame-ancestors 'self'; object-src 'none'");
  } else {
    response.headers.set("x-frame-options", "DENY");
  }
  response.headers.set("referrer-policy", "same-origin");
  response.headers.set(
    "permissions-policy",
    "camera=(), microphone=(), geolocation=(), payment=()",
  );
  return response;
});
