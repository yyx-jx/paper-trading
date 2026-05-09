import { useEffect, useState, type ReactNode } from "react";
import { FieldChip } from "../../components/FieldChip";
import {
  api,
  type AuditEvent,
  type Language,
  type PositionRecord,
  type ProfileOverview,
  type PublicUser,
  type Role
} from "../../utils/api";
import { localLabel, money, signedMoney, timeText } from "../../utils/format";
import { redactNetworkAddresses } from "../../utils/redaction";

function roleTone(role: Role): "positive" | "negative" | "neutral" | "warning" {
  if (role === "Admin") return "positive";
  if (role === "Senior Tester") return "warning";
  if (role === "Test Engineer") return "neutral";
  return "positive";
}

export function PersonalHomePage(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
  token: string;
  me: PublicUser;
  language: Language;
  profile?: ProfileOverview;
  positions: PositionRecord[];
  logs: AuditEvent[];
  canOpenUserManagement: boolean;
  userManagementSlot?: ReactNode;
  onProfileRefresh: () => Promise<void>;
  onUserUpdated: (user: PublicUser) => void;
}) {
  const { t, token, me, language } = props;
  const [displayName, setDisplayName] = useState(me.displayName);
  const [selfLanguage, setSelfLanguage] = useState<Language>(me.language);
  const [passwordDialog, setPasswordDialog] = useState<{ currentPassword: string; password: string; confirmPassword: string }>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    setDisplayName(me.displayName);
    setSelfLanguage(me.language);
  }, [me.displayName, me.language]);

  const saveProfile = async () => {
    try {
      setBusy(true);
      setError(undefined);
      const updated = await api.updateMe(token, { displayName: displayName.trim(), language: selfLanguage });
      props.onUserUpdated(updated);
      await props.onProfileRefresh();
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : "Profile update failed.");
    } finally {
      setBusy(false);
    }
  };

  const changePassword = async () => {
    if (!passwordDialog) return;
    if (passwordDialog.password !== passwordDialog.confirmPassword) {
      setError(t("theNewPasswordConfirmationDoesNotMatch"));
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      await api.changeMyPassword(token, passwordDialog);
      setPasswordDialog(undefined);
      await props.onProfileRefresh();
    } catch (changeError) {
      setError(changeError instanceof Error ? changeError.message : "Password update failed.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="personal-home-page">
      <div className="personal-headband">
        <div className="personal-identity-card">
          <div className="personal-avatar">{me.displayName.slice(0, 1).toUpperCase()}</div>
          <div>
            <span>{localLabel(language, "个人主页", "Personal Home")}</span>
            <strong>{me.displayName}</strong>
            <small>
              @{me.username} ·{" "}
              {props.canOpenUserManagement
                ? localLabel(language, "可查看或管理授权范围内用户", "Can view or manage authorized users")
                : localLabel(language, "个人账号与交易分析", "Personal account and analytics")}
            </small>
          </div>
        </div>
        <div className="personal-meta-strip">
          <FieldChip label={me.role} tone={roleTone(me.role)} />
          <FieldChip label={me.isActive ? t("active") : t("disabled")} tone={me.isActive ? "positive" : "negative"} />
          <FieldChip label={me.permissionLevel ?? "Standard"} tone={me.permissionLevel === "Initial" ? "warning" : "positive"} />
        </div>
      </div>

      {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}

      <div className="personal-summary-grid">
        <div className="analytics-card">
          <span>{localLabel(language, "总资产", "Total Equity")}</span>
          <strong>{money(props.profile?.totalEquity ?? 0)}</strong>
          <small>{localLabel(language, "可用", "Available")} {money(props.profile?.availableUsdc ?? me.availableUsdc)}</small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "持仓价值", "Position Value")}</span>
          <strong>{money(props.profile?.positionValue ?? 0)}</strong>
          <small>{props.positions.filter((position) => position.status === "open").length} {localLabel(language, "个未平持仓", "open positions")}</small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "今日已实现", "Realized Today")}</span>
          <strong>{signedMoney(props.profile?.realizedPnlToday ?? 0)}</strong>
          <small>{localLabel(language, "未实现", "Unrealized")} {signedMoney(props.profile?.unrealizedPnl ?? 0)}</small>
        </div>
        <div className="analytics-card">
          <span>{localLabel(language, "参与轮次", "Rounds")}</span>
          <strong>{props.profile?.roundsParticipatedTotal ?? 0}</strong>
          <small>{localLabel(language, "今日", "Today")} {props.profile?.roundsParticipatedToday ?? 0}</small>
        </div>
      </div>

      <div className="personal-grid">
        <div className="analytics-card personal-form-card">
          <span>{localLabel(language, "账号信息", "Account")}</span>
          <label>{t("displayName")}<input value={displayName} onChange={(event) => setDisplayName(event.target.value)} /></label>
          <label>{t("language")}<select value={selfLanguage} onChange={(event) => setSelfLanguage(event.target.value as Language)}><option value="zh-CN">简体中文</option><option value="en-US">English</option></select></label>
          <div className="personal-permission-list">
            {me.permissionCodes.slice(0, 8).map((code) => <span key={code}>{code}</span>)}
          </div>
          <div className="button-row">
            <button className="secondary-button" disabled={busy} onClick={saveProfile}>{t("confirm")}</button>
            <button className="ghost-button" disabled={busy} onClick={() => setPasswordDialog({ currentPassword: "", password: "", confirmPassword: "" })}>{t("changeMyPassword")}</button>
          </div>
        </div>
        <div className="analytics-card personal-activity-card">
          <span>{localLabel(language, "最近动态", "Recent Activity")}</span>
          <div className="personal-activity-list">
            {props.logs.slice(0, 6).map((log) => (
              <div key={log.eventId}><strong>{log.actionType}</strong><small>{timeText(log.serverRecvTs)} · {redactNetworkAddresses(log.resultMessage)}</small></div>
            ))}
            {props.logs.length === 0 ? <small>{t("noData")}</small> : null}
          </div>
        </div>
      </div>

      {props.userManagementSlot}

      {passwordDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog">
            <div className="section-header"><div><p className="eyebrow">{t("identityCheck")}</p><h2>{t("changeMyPassword")}</h2></div><button className="ghost-button compact-button" onClick={() => setPasswordDialog(undefined)}>{t("close")}</button></div>
            <div className="dialog-form">
              <label>{t("currentOperatorPassword")}<input type="password" value={passwordDialog.currentPassword} onChange={(event) => setPasswordDialog({ ...passwordDialog, currentPassword: event.target.value })} /></label>
              <label>{t("newPassword")}<input type="password" value={passwordDialog.password} onChange={(event) => setPasswordDialog({ ...passwordDialog, password: event.target.value })} /></label>
              <label>{t("confirmNewPassword")}<input type="password" value={passwordDialog.confirmPassword} onChange={(event) => setPasswordDialog({ ...passwordDialog, confirmPassword: event.target.value })} /></label>
              <div className="button-row"><button className="secondary-button" disabled={busy} onClick={changePassword}>{t("confirm")}</button><button className="ghost-button" disabled={busy} onClick={() => setPasswordDialog(undefined)}>{t("cancel")}</button></div>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
