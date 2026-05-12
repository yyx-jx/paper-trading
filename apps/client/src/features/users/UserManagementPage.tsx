import { useEffect, useState, type ChangeEvent } from "react";
import { FieldChip } from "../../components/FieldChip";
import {
  api,
  type BulkCreateUsersPreviewResult,
  type BulkCreateUsersResult,
  type Language,
  type PermissionLevel,
  type PublicUser,
  type Role,
  type UpdateUserInput
} from "../../utils/api";
import { localLabel, money } from "../../utils/format";
import { redactNetworkAddresses } from "../../utils/redaction";

function BulkUserDialog(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
  token: string;
  language: Language;
  users: PublicUser[];
  busy: boolean;
  setBusy: (value: boolean) => void;
  onError: (message?: string) => void;
  onCreated: () => Promise<void>;
  onClose: () => void;
}) {
  const { t, token, language } = props;
  const template =
    "username,password,displayName,role,language,managerUsername,availableUsdc,permissionLevel,mustChangePassword\n" +
    "tester_new_01,ChangeMe123,Tester New 01,Tester,zh-CN,,10000,Standard,true";
  const [sourceText, setSourceText] = useState("");
  const [localError, setLocalError] = useState<string>();
  const [preview, setPreview] = useState<BulkCreateUsersPreviewResult>();
  const [result, setResult] = useState<BulkCreateUsersResult>();
  const previewRows = preview?.valid ?? [];
  const previewFailed = preview?.failed ?? [];

  const downloadCsv = (text: string) => {
    const blob = new Blob([text], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = localLabel(language, "批量用户模板.csv", "bulk-users-template.csv");
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  };

  const previewCsv = async (text = sourceText) => {
    if (!text.trim()) {
      setLocalError(t("importOrPasteCsvTsvContentFirst"));
      setPreview(undefined);
      return;
    }
    try {
      props.setBusy(true);
      props.onError(undefined);
      setLocalError(undefined);
      setResult(undefined);
      setPreview(await api.previewBulkUsersCsv(token, text));
    } catch (previewError) {
      const message = previewError instanceof Error ? previewError.message : "CSV preview failed.";
      setPreview(undefined);
      setLocalError(message);
      props.onError(message);
    } finally {
      props.setBusy(false);
    }
  };

  const downloadTemplate = async () => {
    try {
      props.setBusy(true);
      props.onError(undefined);
      setLocalError(undefined);
      const text = await api.downloadBulkUsersTemplate(token);
      downloadCsv(text);
      setSourceText(text.replace(/^\uFEFF/, ""));
      setPreview(undefined);
      setResult(undefined);
    } catch (downloadError) {
      const message = downloadError instanceof Error ? downloadError.message : "Template download failed.";
      setLocalError(message);
      props.onError(message);
    } finally {
      props.setBusy(false);
    }
  };

  const handleFile = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.currentTarget.files?.[0];
    if (!file) {
      return;
    }
    file
      .text()
      .then((text) => {
        setSourceText(text);
        setResult(undefined);
        setLocalError(undefined);
        void previewCsv(text);
      })
      .catch(() => {
        setLocalError(t("failedToReadTheFile"));
      });
  };

  const submit = async () => {
    if (!sourceText.trim()) {
      setLocalError(t("importOrPasteCsvTsvContentFirst"));
      return;
    }
    if (!preview) {
      await previewCsv();
      return;
    }
    if (previewFailed.length > 0 || previewRows.length === 0) {
      setLocalError(t("fixImportErrorsBeforeCreatingUsers"));
      return;
    }
    try {
      props.setBusy(true);
      props.onError(undefined);
      setLocalError(undefined);
      const nextResult = await api.bulkCreateUsersCsv(token, sourceText);
      setResult(nextResult);
      if (nextResult.failed.length === 0) {
        await props.onCreated();
        setPreview(undefined);
      }
    } catch (bulkError) {
      const message = bulkError instanceof Error ? bulkError.message : "Bulk create failed.";
      setLocalError(message);
      props.onError(message);
    } finally {
      props.setBusy(false);
    }
  };

  return (
    <div className="modal-backdrop">
      <section className="panel bulk-user-dialog" onClick={(event) => event.stopPropagation()}>
        <div className="section-header">
          <div>
            <p className="eyebrow">{t("bulkRegistration")}</p>
            <h2>{t("csvTsvUserImport")}</h2>
          </div>
          <button className="ghost-button compact-button" onClick={props.onClose}>
            {t("close")}
          </button>
        </div>
        <div className="dialog-section">
          <div className="button-row fit-actions">
            <button className="secondary-button compact-button" disabled={props.busy} onClick={downloadTemplate}>
              {localLabel(language, "下载模板", "Download Template")}
            </button>
            <label className="file-import-button">
              {t("chooseCsvTsvFile")}
              <input type="file" accept=".csv,.tsv,text/csv,text/tab-separated-values,text/plain" onChange={handleFile} />
            </label>
            <button
              className="ghost-button compact-button"
              disabled={props.busy}
              onClick={() => void previewCsv()}
            >
              {localLabel(language, "预览校验", "Preview")}
            </button>
            <button
              className="ghost-button compact-button"
              disabled={props.busy}
              onClick={() => {
                setSourceText(template);
                setPreview(undefined);
                setResult(undefined);
                setLocalError(undefined);
              }}
            >
              {t("useTemplate")}
            </button>
          </div>
          <small className="muted-line">
            {localLabel(
              language,
              "请先下载模板填写；上传或粘贴后点击预览校验，通过后再创建用户。",
              "Download the template first; upload or paste it, preview validation, then create users."
            )}
          </small>
        </div>
        <div className="dialog-form">
          <label>
            {t("pasteCsvTsvText")}
            <textarea
              value={sourceText}
              onChange={(event) => {
                setSourceText(event.target.value);
                setPreview(undefined);
                setResult(undefined);
                setLocalError(undefined);
              }}
            />
          </label>
        </div>
        {localError ? <div className="inline-error-banner">{redactNetworkAddresses(localError)}</div> : null}
        {preview ? (
          <div className={previewFailed.length ? "inline-error-banner" : "inline-info-banner"}>
            {localLabel(
              language,
              `预览 ${preview.total} 行，可创建 ${previewRows.length} 行，失败 ${previewFailed.length} 行。`,
              `Previewed ${preview.total} rows, ${previewRows.length} creatable, ${previewFailed.length} failed.`
            )}
            {previewFailed.length
              ? ` ${previewFailed.map((item) => `#${item.rowNumber}: ${redactNetworkAddresses(item.error)}`).join("; ")}`
              : ""}
          </div>
        ) : null}
        {result ? (
          <div className={result.failed.length ? "inline-error-banner" : "inline-info-banner"}>
            {localLabel(
              language,
              `创建 ${result.created.length} 个，失败 ${result.failed.length} 个。`,
              `Created ${result.created.length}, failed ${result.failed.length}.`
            )}
            {result.failed.length
              ? ` ${result.failed.map((item) => `#${item.rowNumber}: ${redactNetworkAddresses(item.error)}`).join("; ")}`
              : ""}
          </div>
        ) : null}
        <div className="dialog-table-shell">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>{t("username")}</th>
                <th>{t("displayName")}</th>
                <th>{t("role")}</th>
                <th>{t("language")}</th>
                <th>{t("seniorTester")}</th>
                <th>{t("available")}</th>
                <th>{t("validation")}</th>
              </tr>
            </thead>
            <tbody>
              {!preview || (previewRows.length === 0 && previewFailed.length === 0) ? (
                <tr>
                  <td colSpan={8}>{t("noData")}</td>
                </tr>
              ) : (
                [
                  ...previewRows.map((row) => ({
                    ...row,
                    status: "ready" as const,
                    error: ""
                  })),
                  ...previewFailed.map((row) => ({
                    rowNumber: row.rowNumber,
                    username: row.username ?? "",
                    displayName: "",
                    role: "Tester" as Role,
                    language: "zh-CN" as Language,
                    seniorTesterId: undefined,
                    managerUserId: undefined,
                    availableUsdc: undefined,
                    status: "failed" as const,
                    error: row.error
                  }))
                ].map((row) => (
                  <tr key={row.rowNumber}>
                    <td>{row.rowNumber}</td>
                    <td>{row.username || "--"}</td>
                    <td>{row.displayName || "--"}</td>
                    <td>{row.role ?? "Tester"}</td>
                    <td>{row.language ?? "zh-CN"}</td>
                    <td>{row.managerUserId ?? row.seniorTesterId ?? "--"}</td>
                    <td>{typeof row.availableUsdc === "number" ? money(row.availableUsdc) : t("default")}</td>
                    <td>
                      {row.status === "failed" ? (
                        <span className="tone-negative">{redactNetworkAddresses(row.error)}</span>
                      ) : (
                        <FieldChip label={t("ready")} tone="positive" />
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <div className="button-row dialog-actions">
          <button className="secondary-button" disabled={props.busy || previewFailed.length > 0 || previewRows.length === 0} onClick={submit}>
            {props.busy ? t("loading") : t("createUsers", { value: previewRows.length })}
          </button>
          <button className="ghost-button" disabled={props.busy} onClick={props.onClose}>
            {t("cancel")}
          </button>
        </div>
      </section>
    </div>
  );
}

export function UserManagementPage(props: {
  t: (key: string, options?: Record<string, unknown>) => string;
  token: string;
  me: PublicUser;
  language: Language;
  embedded?: boolean;
  onProfileRefresh: () => Promise<void>;
}) {
  const { token, me, language } = props;
  const t = props.t;
  const [users, setUsers] = useState<PublicUser[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [balanceDialog, setBalanceDialog] = useState<{ user: PublicUser; amount: string }>();
  const [passwordDialog, setPasswordDialog] = useState<{
    user: PublicUser;
    mode: "self" | "reset";
    currentPassword: string;
    password: string;
    confirmPassword: string;
  }>();
  const [bulkDialogOpen, setBulkDialogOpen] = useState(false);
  const [searchText, setSearchText] = useState("");
  const [roleFilter, setRoleFilter] = useState<"ALL" | Role>("ALL");
  const [editDialog, setEditDialog] = useState<{
    user: PublicUser;
    displayName: string;
    role: Role;
    language: Language;
    managerUserId: string;
    permissionLevel: PermissionLevel;
    availableUsdc: string;
    isActive: boolean;
  }>();
  const [form, setForm] = useState({
    username: "",
    password: "",
    displayName: "",
    role: "Tester" as Role,
    language: "zh-CN" as Language,
    seniorTesterId: "",
    availableUsdc: "10000"
  });
  const isAdmin = me.role === "Admin";
  const canBulkCreate = me.permissionCodes.includes("users:bulk-create");
  const canUpdateUsers = me.permissionCodes.includes("users:update");

  const loadUsers = async () => {
    try {
      setBusy(true);
      setError(undefined);
      setUsers(await api.getUsers(token));
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Load users failed.");
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void loadUsers();
  }, []);

  const seniorOptions = users.filter((user) => user.role === "Senior Tester" && user.isActive);
  const canManageTarget = (user: PublicUser) =>
    isAdmin || (me.role === "Senior Tester" && user.role === "Tester" && (user.managerUserId ?? user.seniorTesterId) === me.id);
  const canSetBalance = (user: PublicUser) => canManageTarget(user) || (me.role === "Senior Tester" && user.id === me.id);
  const visibleUsers = users.filter((user) => {
    const query = searchText.trim().toLowerCase();
    if (roleFilter !== "ALL" && user.role !== roleFilter) return false;
    if (!query) return true;
    return [user.username, user.displayName, user.role, user.permissionLevel ?? "Standard"].some((value) =>
      value.toLowerCase().includes(query)
    );
  });
  const activeCount = users.filter((user) => user.isActive).length;
  const managedCount = users.filter((user) => canManageTarget(user)).length;
  const roleCounts = users.reduce<Record<Role, number>>(
    (counts, user) => ({ ...counts, [user.role]: counts[user.role] + 1 }),
    { Tester: 0, "Senior Tester": 0, "Test Engineer": 0, Admin: 0 }
  );

  const createUser = async () => {
    try {
      setBusy(true);
      setError(undefined);
      await api.createUser(token, {
        username: form.username,
        password: form.password,
        displayName: form.displayName,
        role: form.role,
        language: form.language,
        seniorTesterId: form.role === "Tester" ? form.seniorTesterId || undefined : undefined,
        availableUsdc: Number(form.availableUsdc || 0)
      });
      setForm({
        username: "",
        password: "",
        displayName: "",
        role: "Tester",
        language: "zh-CN",
        seniorTesterId: "",
        availableUsdc: "10000"
      });
      await loadUsers();
    } catch (createError) {
      setError(createError instanceof Error ? createError.message : "Create user failed.");
    } finally {
      setBusy(false);
    }
  };

  const disableUser = async (user: PublicUser) => {
    const ok = window.confirm(t("disableAccount", { userusername: user.username }));
    if (!ok) {
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      await api.disableUser(token, user.id);
      await loadUsers();
    } catch (disableError) {
      setError(disableError instanceof Error ? disableError.message : "Disable user failed.");
    } finally {
      setBusy(false);
    }
  };

  const enableUser = async (user: PublicUser) => {
    const ok = window.confirm(t("restoreAccount", { userusername: user.username }));
    if (!ok) {
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      await api.enableUser(token, user.id);
      await loadUsers();
    } catch (enableError) {
      setError(enableError instanceof Error ? enableError.message : "Restore user failed.");
    } finally {
      setBusy(false);
    }
  };

  const submitPasswordReset = async () => {
    if (!passwordDialog) {
      return;
    }
    if (passwordDialog.password !== passwordDialog.confirmPassword) {
      setError(t("theNewPasswordConfirmationDoesNotMatch"));
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      const payload = {
        currentPassword: passwordDialog.currentPassword,
        password: passwordDialog.password,
        confirmPassword: passwordDialog.confirmPassword
      };
      if (passwordDialog.mode === "self") {
        await api.changeMyPassword(token, payload);
        await props.onProfileRefresh();
      } else {
        await api.resetUserPassword(token, passwordDialog.user.id, payload);
      }
      setPasswordDialog(undefined);
      await loadUsers();
    } catch (resetError) {
      setError(resetError instanceof Error ? resetError.message : "Reset password failed.");
    } finally {
      setBusy(false);
    }
  };

  const submitBalance = async () => {
    if (!balanceDialog) {
      return;
    }
    const amount = Number(balanceDialog.amount);
    if (!Number.isFinite(amount) || amount < 0) {
      setError(t("enterAValidAmount"));
      return;
    }
    try {
      setBusy(true);
      setError(undefined);
      const targetUserId = balanceDialog.user.id;
      await api.setUserBalance(token, targetUserId, amount);
      setBalanceDialog(undefined);
      await loadUsers();
      if (targetUserId === me.id) {
        await props.onProfileRefresh();
      }
    } catch (balanceError) {
      setError(balanceError instanceof Error ? balanceError.message : "Set balance failed.");
    } finally {
      setBusy(false);
    }
  };

  const openEditDialog = (user: PublicUser) => {
    setError(undefined);
    setEditDialog({
      user,
      displayName: user.displayName,
      role: user.role,
      language: user.language,
      managerUserId: user.managerUserId ?? user.seniorTesterId ?? "",
      permissionLevel: user.permissionLevel ?? "Standard",
      availableUsdc: String(user.availableUsdc),
      isActive: user.isActive
    });
  };

  const submitEdit = async () => {
    if (!editDialog) return;
    const amount = Number(editDialog.availableUsdc);
    if (!Number.isFinite(amount) || amount < 0) {
      setError(t("enterAValidAmount"));
      return;
    }
    const payload: UpdateUserInput = {
      displayName: editDialog.displayName,
      role: editDialog.role,
      language: editDialog.language,
      managerUserId: editDialog.role === "Tester" ? editDialog.managerUserId || null : null,
      seniorTesterId: editDialog.role === "Tester" ? editDialog.managerUserId || null : null,
      permissionLevel: editDialog.permissionLevel,
      availableUsdc: amount,
      isActive: editDialog.isActive
    };
    try {
      setBusy(true);
      setError(undefined);
      await api.updateUser(token, editDialog.user.id, payload);
      setEditDialog(undefined);
      await loadUsers();
    } catch (editError) {
      setError(editError instanceof Error ? editError.message : "Update user failed.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className={props.embedded ? "user-home-panel" : "panel log-search-panel"}>
      <div className="section-header">
        <div>
          <p className="eyebrow">{localLabel(language, "用户范围", "User Scope")}</p>
          <h2>{users.length}</h2>
        </div>
        <div className="button-row fit-actions">
          {canBulkCreate ? (
            <button
              className="secondary-button"
              disabled={busy}
              onClick={() => {
                setError(undefined);
                setBulkDialogOpen(true);
              }}
            >
              {t("bulkRegister")}
            </button>
          ) : null}
          <button
            className="secondary-button"
            disabled={busy}
            onClick={() => {
              setError(undefined);
              setPasswordDialog({ user: me, mode: "self", currentPassword: "", password: "", confirmPassword: "" });
            }}
          >
            {t("changeMyPassword")}
          </button>
          <button className="secondary-button" onClick={loadUsers} disabled={busy}>
            {busy ? props.t("loading") : props.t("search")}
          </button>
        </div>
      </div>
      {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}

      <div className="user-overview-grid">
        <div className="analytics-card"><span>{localLabel(language, "可见用户", "Visible Users")}</span><strong>{users.length}</strong><small>{managedCount} {localLabel(language, "可管理", "manageable")}</small></div>
        <div className="analytics-card"><span>{localLabel(language, "活跃账号", "Active")}</span><strong>{activeCount}</strong><small>{users.length - activeCount} {localLabel(language, "停用", "disabled")}</small></div>
        <div className="analytics-card"><span>{localLabel(language, "Tester", "Tester")}</span><strong>{roleCounts.Tester}</strong><small>{roleCounts["Senior Tester"]} Senior</small></div>
        <div className="analytics-card"><span>{localLabel(language, "工程/管理", "Engineer/Admin")}</span><strong>{roleCounts["Test Engineer"] + roleCounts.Admin}</strong><small>{roleCounts.Admin} Admin</small></div>
      </div>

      {isAdmin ? (
        <div className="filter-grid user-create-grid">
          <label>
            {props.t("username")}
            <input value={form.username} onChange={(event) => setForm({ ...form, username: event.target.value })} />
          </label>
          <label>
            {props.t("password")}
            <input value={form.password} onChange={(event) => setForm({ ...form, password: event.target.value })} />
          </label>
          <label>
            {t("displayName")}
            <input value={form.displayName} onChange={(event) => setForm({ ...form, displayName: event.target.value })} />
          </label>
          <label>
            {props.t("role")}
            <select value={form.role} onChange={(event) => setForm({ ...form, role: event.target.value as Role })}>
              {(["Tester", "Senior Tester", "Test Engineer", "Admin"] as Role[]).map((role) => (
                <option key={role} value={role}>
                  {role}
                </option>
              ))}
            </select>
          </label>
          <label>
            {props.t("language")}
            <select value={form.language} onChange={(event) => setForm({ ...form, language: event.target.value as Language })}>
              <option value="zh-CN">简体中文</option>
              <option value="en-US">English</option>
            </select>
          </label>
          <label>
            {t("seniorTester")}
            <select value={form.seniorTesterId} onChange={(event) => setForm({ ...form, seniorTesterId: event.target.value })} disabled={form.role !== "Tester"}>
              <option value="">{props.t("all")}</option>
              {seniorOptions.map((user) => (
                <option key={user.id} value={user.id}>
                  {user.username}
                </option>
              ))}
            </select>
          </label>
          <label>
            {props.t("available")}
            <input value={form.availableUsdc} onChange={(event) => setForm({ ...form, availableUsdc: event.target.value })} />
          </label>
          <button className="primary-button user-create-button" disabled={busy} onClick={createUser}>
            {t("create")}
          </button>
        </div>
      ) : null}

      <div className="user-scope-toolbar">
        <label>
          {localLabel(language, "搜索", "Search")}
          <input
            value={searchText}
            placeholder={localLabel(language, "用户名 / 昵称 / 角色", "Username / display name / role")}
            onChange={(event) => setSearchText(event.target.value)}
          />
        </label>
        <label>
          {props.t("role")}
          <select value={roleFilter} onChange={(event) => setRoleFilter(event.target.value as "ALL" | Role)}>
            <option value="ALL">{props.t("all")}</option>
            {(["Tester", "Senior Tester", "Test Engineer", "Admin"] as Role[]).map((role) => (
              <option key={role} value={role}>{role}</option>
            ))}
          </select>
        </label>
      </div>

      <table>
        <thead>
          <tr>
            <th>{props.t("username")}</th>
            <th>{t("displayName")}</th>
            <th>{props.t("role")}</th>
            <th>{props.t("status")}</th>
            <th>{t("seniorTester")}</th>
            <th>{localLabel(language, "权限等级", "Permission")}</th>
            <th>{props.t("available")}</th>
            <th>{props.t("action")}</th>
          </tr>
        </thead>
        <tbody>
          {visibleUsers.length === 0 ? (
            <tr>
              <td colSpan={8}>{props.t("noData")}</td>
            </tr>
          ) : (
            visibleUsers.map((user) => {
              const senior = users.find((candidate) => candidate.id === (user.managerUserId ?? user.seniorTesterId));
              return (
                <tr key={user.id}>
                  <td>{user.username}</td>
                  <td>{user.displayName}</td>
                  <td>{user.role}</td>
                  <td>
                    <FieldChip
                      label={user.isActive ? t("active") : t("disabled")}
                      tone={user.isActive ? "positive" : "negative"}
                    />
                  </td>
                  <td>{senior?.username ?? "--"}</td>
                  <td>{user.permissionLevel ?? "Standard"}</td>
                  <td>{money(user.availableUsdc)}</td>
                  <td>
                    <div className="table-action-cell">
                      {canUpdateUsers && canManageTarget(user) && user.id !== me.id ? (
                        <button className="ghost-button compact-button" disabled={busy} onClick={() => openEditDialog(user)}>
                          {localLabel(language, "资料", "Edit")}
                        </button>
                      ) : null}
                      {canSetBalance(user) ? (
                        <button
                          className="ghost-button compact-button"
                          disabled={busy}
                          onClick={() => {
                            setError(undefined);
                            setBalanceDialog({ user, amount: String(user.availableUsdc) });
                          }}
                        >
                          {t("balance")}
                        </button>
                      ) : null}
                      {canManageTarget(user) ? (
                        <button
                          className="ghost-button compact-button"
                          disabled={busy}
                          onClick={() => {
                            setError(undefined);
                            setPasswordDialog({ user, mode: "reset", currentPassword: "", password: "", confirmPassword: "" });
                          }}
                        >
                          {t("password")}
                        </button>
                      ) : null}
                      {canManageTarget(user) && user.id !== me.id ? (
                        user.isActive ? (
                          <button className="ghost-button compact-button" disabled={busy} onClick={() => disableUser(user)}>
                            {t("disable")}
                          </button>
                        ) : (
                          <button className="ghost-button compact-button" disabled={busy} onClick={() => enableUser(user)}>
                            {t("restore")}
                          </button>
                        )
                      ) : null}
                    </div>
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
      {editDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog user-profile-dialog">
            <div className="section-header">
              <div>
                <p className="eyebrow">{localLabel(language, "用户资料", "User Profile")}</p>
                <h2>{editDialog.user.username}</h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setEditDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}
            <div className="dialog-form">
              <label>
                {t("displayName")}
                <input value={editDialog.displayName} onChange={(event) => setEditDialog({ ...editDialog, displayName: event.target.value })} />
              </label>
              <label>
                {props.t("role")}
                <select value={editDialog.role} disabled={!isAdmin} onChange={(event) => setEditDialog({ ...editDialog, role: event.target.value as Role })}>
                  {(["Tester", "Senior Tester", "Test Engineer", "Admin"] as Role[]).map((role) => (
                    <option key={role} value={role}>{role}</option>
                  ))}
                </select>
              </label>
              <label>
                {props.t("language")}
                <select value={editDialog.language} onChange={(event) => setEditDialog({ ...editDialog, language: event.target.value as Language })}>
                  <option value="zh-CN">简体中文</option>
                  <option value="en-US">English</option>
                </select>
              </label>
              <label>
                {t("seniorTester")}
                <select
                  value={editDialog.managerUserId}
                  disabled={editDialog.role !== "Tester"}
                  onChange={(event) => setEditDialog({ ...editDialog, managerUserId: event.target.value })}
                >
                  <option value="">{props.t("all")}</option>
                  {seniorOptions.map((user) => (
                    <option key={user.id} value={user.id}>{user.username}</option>
                  ))}
                </select>
              </label>
              <label>
                {localLabel(language, "权限等级", "Permission Level")}
                <select value={editDialog.permissionLevel} onChange={(event) => setEditDialog({ ...editDialog, permissionLevel: event.target.value as PermissionLevel })}>
                  <option value="Initial">Initial</option>
                  <option value="Standard">Standard</option>
                </select>
              </label>
              <label>
                {props.t("available")}
                <input value={editDialog.availableUsdc} onChange={(event) => setEditDialog({ ...editDialog, availableUsdc: event.target.value })} />
              </label>
              {isAdmin ? (
                <label className="check-choice">
                  <input type="checkbox" checked={editDialog.isActive} onChange={(event) => setEditDialog({ ...editDialog, isActive: event.target.checked })} />
                  {editDialog.isActive ? t("active") : t("disabled")}
                </label>
              ) : null}
              <div className="button-row">
                <button className="secondary-button" disabled={busy} onClick={submitEdit}>{t("confirm")}</button>
                <button className="ghost-button" disabled={busy} onClick={() => setEditDialog(undefined)}>{props.t("cancel")}</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
      {balanceDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog">
            <div className="section-header">
              <div>
                <p className="eyebrow">{t("balance")}</p>
                <h2>{balanceDialog.user.username}</h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setBalanceDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}
            <div className="dialog-form">
              <label>
                {props.t("available")}
                <input
                  value={balanceDialog.amount}
                  onChange={(event) => setBalanceDialog({ ...balanceDialog, amount: event.target.value })}
                />
              </label>
              <div className="button-row">
                <button className="secondary-button" disabled={busy} onClick={submitBalance}>
                  {t("confirm")}
                </button>
                <button className="ghost-button" disabled={busy} onClick={() => setBalanceDialog(undefined)}>
                  {props.t("cancel")}
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
      {passwordDialog ? (
        <div className="modal-backdrop">
          <div className="panel user-action-dialog">
            <div className="section-header">
              <div>
                <p className="eyebrow">{t("identityCheck")}</p>
                <h2>
                  {passwordDialog.mode === "self"
                    ? t("changeMyPassword")
                    : t("resetPassword")}{" "}
                  / {passwordDialog.user.username}
                </h2>
              </div>
              <button className="ghost-button compact-button" onClick={() => setPasswordDialog(undefined)}>
                {props.t("close")}
              </button>
            </div>
            {error ? <div className="inline-error-banner">{redactNetworkAddresses(error)}</div> : null}
            <div className="dialog-form">
              <label>
                {t("currentOperatorPassword")}
                <input
                  type="password"
                  value={passwordDialog.currentPassword}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, currentPassword: event.target.value })}
                />
              </label>
              <label>
                {t("newPassword")}
                <input
                  type="password"
                  value={passwordDialog.password}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, password: event.target.value })}
                />
              </label>
              <label>
                {t("confirmNewPassword")}
                <input
                  type="password"
                  value={passwordDialog.confirmPassword}
                  onChange={(event) => setPasswordDialog({ ...passwordDialog, confirmPassword: event.target.value })}
                />
              </label>
              <div className="button-row">
                <button className="secondary-button" disabled={busy} onClick={submitPasswordReset}>
                  {t("confirm")}
                </button>
                <button className="ghost-button" disabled={busy} onClick={() => setPasswordDialog(undefined)}>
                  {props.t("cancel")}
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
      {bulkDialogOpen ? (
        <BulkUserDialog
          t={props.t}
          token={token}
          language={language}
          users={users}
          busy={busy}
          setBusy={setBusy}
          onError={setError}
          onCreated={loadUsers}
          onClose={() => setBulkDialogOpen(false)}
        />
      ) : null}
    </section>
  );
}

