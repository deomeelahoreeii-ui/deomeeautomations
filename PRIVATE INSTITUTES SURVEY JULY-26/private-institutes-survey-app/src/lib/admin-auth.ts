import type { APIContext } from "astro";
import { env } from "cloudflare:workers";

const builtInAdminEmails = [
  "deo.mee.lahore.eii@gmail.com",
  "pomubbsher@gmail.com",
];

type ClerkEmailAddress = {
  emailAddress?: string | null;
  id?: string | null;
};

type ClerkUserLike = {
  primaryEmailAddress?: ClerkEmailAddress | null;
  primaryEmailAddressId?: string | null;
  emailAddresses?: ClerkEmailAddress[];
};

type AuthResult = {
  isAuthenticated: boolean;
  redirectToSignIn: () => Response;
};

type ClerkLocals = APIContext["locals"] & {
  auth: () => AuthResult;
  currentUser: () => Promise<ClerkUserLike | null>;
};

type AuthEnv = {
  AUTH_ENABLED?: string;
  ADMIN_EMAILS?: string;
};

export function isAuthEnabled(): boolean {
  const configured = (env as AuthEnv).AUTH_ENABLED ?? process.env.AUTH_ENABLED;
  return String(configured ?? "true").toLowerCase() !== "false";
}

function parseConfiguredAdminEmails(): string[] {
  const configured = (env as { ADMIN_EMAILS?: string }).ADMIN_EMAILS ?? process.env.ADMIN_EMAILS;

  return String(configured ?? "")
    .split(",")
    .map((email) => email.trim().toLowerCase())
    .filter(Boolean);
}

export function getApprovedAdminEmails(): string[] {
  return Array.from(
    new Set([
      ...builtInAdminEmails.map((email) => email.toLowerCase()),
      ...parseConfiguredAdminEmails(),
    ]),
  );
}

export function getPrimaryEmail(user: ClerkUserLike | null): string | null {
  if (!user) return null;

  const primaryEmail =
    user.primaryEmailAddress?.emailAddress ??
    user.emailAddresses?.find(
      (email) => email.id === user.primaryEmailAddressId,
    )?.emailAddress ??
    user.emailAddresses?.[0]?.emailAddress;

  return primaryEmail ? primaryEmail.toLowerCase() : null;
}

export async function requireApprovedAdmin(
  context: APIContext,
): Promise<
  | { authorized: true; email: string }
  | { authorized: false; response: Response; email: string | null }
> {
  if (!isAuthEnabled()) {
    return { authorized: true, email: "auth-disabled@local" };
  }

  const locals = context.locals as ClerkLocals;
  const auth = locals.auth();

  if (!auth.isAuthenticated) {
    return {
      authorized: false,
      response: new Response(null, {
        status: 302,
        headers: {
          location: "/admin/login",
        },
      }),
      email: null,
    };
  }

  const user = await locals.currentUser();
  const email = getPrimaryEmail(user);
  const approvedEmails = getApprovedAdminEmails();

  if (!email || !approvedEmails.includes(email)) {
    return {
      authorized: false,
      email,
      response: new Response(null, {
        status: 302,
        headers: {
          location: "/admin/unauthorized",
        },
      }),
    };
  }

  return { authorized: true, email };
}

export async function requireApprovedAdminJson(
  context: APIContext,
): Promise<{ authorized: true; email: string } | { authorized: false; response: Response }> {
  if (!isAuthEnabled()) {
    return { authorized: true, email: "auth-disabled@local" };
  }

  const locals = context.locals as ClerkLocals;
  const auth = locals.auth();

  if (!auth.isAuthenticated) {
    return {
      authorized: false,
      response: new Response(JSON.stringify({ error: "Authentication required." }), {
        status: 401,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store",
        },
      }),
    };
  }

  const user = await locals.currentUser();
  const email = getPrimaryEmail(user);

  if (!email || !getApprovedAdminEmails().includes(email)) {
    return {
      authorized: false,
      response: new Response(JSON.stringify({ error: "Admin access denied." }), {
        status: 403,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store",
        },
      }),
    };
  }

  return { authorized: true, email };
}
