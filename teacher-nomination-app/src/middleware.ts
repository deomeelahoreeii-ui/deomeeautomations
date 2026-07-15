import { clerkMiddleware } from "@clerk/astro/server";
import {
  clerkFrontendApiProxy,
  matchProxyPath,
} from "@clerk/backend/proxy";
import { env } from "cloudflare:workers";

type ClerkProxyEnv = {
  PUBLIC_CLERK_PUBLISHABLE_KEY?: string;
  CLERK_SECRET_KEY?: string;
};

const clerk = clerkMiddleware();

export const onRequest = async (context, next) => {
  if (matchProxyPath(context.request)) {
    const clerkEnv = env as ClerkProxyEnv;

    return clerkFrontendApiProxy(context.request, {
      publishableKey: clerkEnv.PUBLIC_CLERK_PUBLISHABLE_KEY,
      secretKey: clerkEnv.CLERK_SECRET_KEY,
    });
  }

  return clerk(context, next);
};
