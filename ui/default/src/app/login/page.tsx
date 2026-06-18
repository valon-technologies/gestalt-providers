"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { getAuthInfo, getAuthSession, isAPIErrorStatus, startLogin } from "@/lib/api";
import { setCachedSession } from "@/lib/auth";
import {
  authReturnPathFromLoginURL,
  storeAuthReturnPath,
} from "@/lib/authReturn";
import { DOCS_PATH, HTTP_UNAUTHORIZED } from "@/lib/constants";
import Button from "@/components/Button";

export default function LoginPage() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [authLabel, setAuthLabel] = useState("Sign in");

  useEffect(() => {
    const returnPath = authReturnPathFromLoginURL();

    function redirectWithSession() {
      return getAuthSession().then((session) => {
        setCachedSession(session);
        window.location.replace(returnPath);
      });
    }

    function showLoginButton(info?: { displayName: string }) {
      if (info) {
        setAuthLabel("Sign in with " + info.displayName);
      }
    }

    getAuthSession()
      .then((session) => {
        setCachedSession(session);
        window.location.replace(returnPath);
      })
      .catch((err) => {
        if (isAPIErrorStatus(err, HTTP_UNAUTHORIZED)) {
          getAuthInfo()
            .then((info) => {
              if (!info.loginSupported) {
                return redirectWithSession();
              }
              showLoginButton(info);
            })
            .catch(() => {});
          return;
        }

        getAuthInfo()
          .then((info) => {
            if (!info.loginSupported) {
              return redirectWithSession();
            }
            showLoginButton(info);
          })
          .catch(() => {});
      });
  }, []);

  async function handleLogin() {
    setLoading(true);
    setError(null);
    try {
      const state = crypto.randomUUID();
      sessionStorage.setItem("oauth_state", state);
      storeAuthReturnPath(authReturnPathFromLoginURL());
      const { url } = await startLogin(state);
      window.location.href = url;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center">
      <div className="w-full max-w-sm animate-fade-in-up">
        <div className="rounded-lg border border-alpha bg-base-white p-10 shadow-dropdown dark:bg-surface">
          <div className="text-center">
            <h1 className="text-2xl font-heading text-primary">
              Gestalt
            </h1>
            <p className="mt-3 text-sm text-muted">
              Sign in to manage your apps.
            </p>
            <p className="mt-2 text-sm text-muted">
              Or read the{" "}
              <Link
                href={DOCS_PATH}
                className="font-medium text-muted hover:text-primary transition-colors duration-150 underline underline-offset-2 decoration-base-300 dark:decoration-base-600"
              >
                documentation
              </Link>
              .
            </p>
          </div>
          {error && (
            <p className="mt-5 text-center text-sm text-ember-500">{error}</p>
          )}
          <div className="mt-8">
            <Button onClick={handleLogin} disabled={loading} className="w-full">
              {loading ? "Redirecting..." : authLabel}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
