import type { MiddlewareHandler } from "astro";
import { env } from "cloudflare:workers";
import { isAuthEnabled } from "./lib/admin-auth";

type ClerkProxyEnv = {
  PUBLIC_CLERK_PUBLISHABLE_KEY?: string;
  CLERK_SECRET_KEY?: string;
};

export const onRequest: MiddlewareHandler = async (context, next) => {
  if (!isAuthEnabled()) {
    return next();
  }

  const [{ clerkMiddleware }, { clerkFrontendApiProxy, matchProxyPath }] =
    await Promise.all([
      import("@clerk/astro/server"),
      import("@clerk/backend/proxy"),
    ]);
  const clerk = clerkMiddleware();

  if (matchProxyPath(context.request)) {
    const clerkEnv = env as ClerkProxyEnv;

    return clerkFrontendApiProxy(context.request, {
      publishableKey: clerkEnv.PUBLIC_CLERK_PUBLISHABLE_KEY,
      secretKey: clerkEnv.CLERK_SECRET_KEY,
    });
  }

  return clerk(context, next) as Promise<Response>;
};
