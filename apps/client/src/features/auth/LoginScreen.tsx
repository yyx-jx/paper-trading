import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import i18n from "../../i18n";
import { api, type Language } from "../../utils/api";
import { redactNetworkAddresses } from "../../utils/redaction";

const t = (key: string, options?: Record<string, unknown>) => i18n.t(key, options);

export function LoginScreen(props: {
  language: Language;
  error?: string;
  onLanguageChange: (language: Language) => void;
  onLogin: (username: string, password: string) => Promise<void>;
}) {
  useTranslation();
  const [username, setUsername] = useState("tester");
  const [password, setPassword] = useState("tester123");
  const [busy, setBusy] = useState(false);
  const [passwordVisible, setPasswordVisible] = useState(false);
  const [savedUsers, setSavedUsers] = useState<string[]>([]);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [serverOnline, setServerOnline] = useState(false);
  const [clock, setClock] = useState(() => new Date().toLocaleTimeString("en-GB", { hour12: false }));

  useEffect(() => {
    try {
      const parsed = JSON.parse(localStorage.getItem("ht_saved_users") ?? "[]") as string[];
      setSavedUsers(parsed.filter((item) => typeof item === "string").slice(0, 8));
    } catch {
      setSavedUsers([]);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    const ping = async () => {
      try {
        const healthy = await api.checkHealth();
        if (!cancelled) setServerOnline(healthy);
      } catch {
        if (!cancelled) setServerOnline(false);
      }
    };
    const clockTimer = setInterval(() => {
      setClock(new Date().toLocaleTimeString("en-GB", { hour12: false }));
    }, 1000);
    const pingTimer = setInterval(ping, 5000);
    void ping();
    return () => {
      cancelled = true;
      clearInterval(clockTimer);
      clearInterval(pingTimer);
    };
  }, []);

  const submit = async () => {
    setBusy(true);
    try {
      await props.onLogin(username, password);
      const nextSaved = [username, ...savedUsers.filter((item) => item !== username)].filter(Boolean).slice(0, 8);
      setSavedUsers(nextSaved);
      localStorage.setItem("ht_saved_users", JSON.stringify(nextSaved));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="terminal-login-page">
      <div className="terminal-login-center">
        <div className="terminal-login-logo">
          <span>Hyper</span>
          <em>liquid</em>
        </div>

        <div className="terminal-login-tabs">
          <button className="active">PAPER</button>
          <button className="locked" disabled>
            LIVE <span aria-hidden="true">/</span>
          </button>
        </div>

        <div className="terminal-login-card">
          {props.error ? <div className="terminal-login-error">{redactNetworkAddresses(props.error)}</div> : null}
          <label>
            <span>{t("username")}</span>
            <div className="terminal-user-wrap">
              <input
                value={username}
                autoComplete="username"
                onFocus={() => setDropdownOpen(true)}
                onBlur={() => window.setTimeout(() => setDropdownOpen(false), 120)}
                onChange={(event) => setUsername(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") void submit();
                }}
              />
              {dropdownOpen && savedUsers.length > 0 ? (
                <div className="terminal-user-dropdown">
                  {savedUsers.map((item) => (
                    <button key={item} type="button" onMouseDown={() => setUsername(item)}>
                      {item}
                    </button>
                  ))}
                </div>
              ) : null}
            </div>
          </label>
          <label>
            <span>{t("password")}</span>
            <div className="terminal-password-wrap">
              <input
                type={passwordVisible ? "text" : "password"}
                value={password}
                autoComplete="current-password"
                onChange={(event) => setPassword(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") void submit();
                }}
              />
              <button type="button" onClick={() => setPasswordVisible((value) => !value)}>
                {passwordVisible ? "hide" : "show"}
              </button>
            </div>
          </label>
          <label>
            <span>{t("language")}</span>
            <select value={props.language} onChange={(event) => props.onLanguageChange(event.target.value as Language)}>
              <option value="zh-CN">简体中文</option>
              <option value="en-US">English</option>
            </select>
          </label>
          <button className="terminal-sign-button" disabled={busy} onClick={submit}>
            {busy ? t("loading") : t("signIn")}
          </button>
        </div>
        <div className="terminal-login-version">v1.2.0 · Hyper Terminal</div>
      </div>
      <div className="terminal-login-status">
        <span className={serverOnline ? "login-status-dot" : "login-status-dot off"} />
        <span>{serverOnline ? t("serverConnected") : t("backendOffline")}</span>
        <span className="login-clock">{clock}</span>
      </div>
    </div>
  );
}
