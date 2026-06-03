import {
  Activity,
  Braces,
  Clock,
  Database,
  FileText,
  LayoutDashboard,
  MessageSquareText,
  RefreshCcw,
  Rocket,
  Save,
  Search,
  Server,
  ShieldCheck,
  Settings2,
  UsersRound
} from "lucide-react";
import type { ComponentType, DependencyList, ReactElement } from "react";
import { useEffect, useMemo, useState } from "react";

type View = "overview" | "cache" | "mq" | "config" | "logs" | "clients";

type Page<T> = {
  items: T[];
  limit: number;
  offset: number;
  next_offset: number | null;
};

type MethodMetric = {
  service: string;
  method: string;
  status: string;
  count: number;
  errors: number;
  total_latency_millis: number;
};

type Overview = {
  server_state: string;
  ready: boolean;
  runtime_metrics: { methods: MethodMetric[] };
  pg_pool: { size: number; idle: number };
  cache_summary: {
    hits: number;
    misses: number;
    writes: number;
    deletes: number;
    evictions: number;
    expired_removals: number;
    logical_key_count: number;
    logical_byte_size: number;
  };
  mq_summary: {
    queue_count: number;
    visible_message_count: number;
    in_flight_message_count: number;
    archived_message_count: number;
  };
};

type CacheNamespace = {
  name: string;
  key_count: number;
  byte_size: number;
};

type CacheEntry = {
  namespace: string;
  key: string;
  size_bytes: number;
  value_preview: string;
  value_encoding: string;
  access_count: number;
};

type QueueSummary = {
  name: string;
  visible_message_count: number;
  in_flight_message_count: number;
  archived_message_count: number;
};

type QueueMessage = {
  queue_name: string;
  message_id: number;
  read_count: number;
  payload_preview: string;
};

type DlqMessage = {
  id: number;
  original_message_id: number;
  read_count: number;
  enqueued_at: string;
  dead_lettered_at: string;
  payload: unknown;
  reason: string;
};

type LogEvent = {
  id: number;
  level: string;
  target: string;
  message: string;
  request_id?: string;
};

type ClientCredential = {
  id: number;
  client_key: string;
  active: boolean;
  roles: string[];
  created_at: string;
  updated_at: string;
};

type ClientActivity = {
  items: ClientCredential[];
  admin_sessions: Array<{ request_id: string; path: string; last_seen_at: string }>;
  api_activity: MethodMetric[];
};

type ConfigScope = {
  app_id: string;
  environment: string;
  cluster: string;
  namespace: string;
};

type ConfigScopeSummary = {
  scope: ConfigScope;
  current_revision: number;
};

type ConfigDraftItem = {
  key: string;
  value: unknown;
  deleted: boolean;
  updated_at: string;
};

type ConfigDraft = {
  scope: ConfigScope;
  items: ConfigDraftItem[];
};

type ConfigRelease = {
  scope: ConfigScope;
  revision: number;
  checksum: string;
  snapshot: unknown;
  message: string;
  published_by: string;
  published_at: string;
};

type ConfigSchemaResponse = {
  scope: ConfigScope;
  has_schema: boolean;
  schema: unknown | null;
};

type CreatedClient = {
  id: number;
  client_key: string;
  secret: string;
  roles: string[];
};

type LoadState<T> =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ready"; data: T };

const navItems: Array<{ view: View; label: string; icon: ComponentType<{ size?: number }> }> = [
  { view: "overview", label: "Overview", icon: LayoutDashboard },
  { view: "cache", label: "Cache", icon: Database },
  { view: "mq", label: "MQ", icon: MessageSquareText },
  { view: "config", label: "Config", icon: Settings2 },
  { view: "logs", label: "Logs", icon: FileText },
  { view: "clients", label: "Clients", icon: UsersRound }
];

async function fetchJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = window.sessionStorage.getItem("pgapp_admin_token");
  const headers: Record<string, string> = { Accept: "application/json" };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  let response: Response;
  try {
    response = await fetch(path, { ...init, headers: { ...headers, ...init.headers } });
  } catch {
    throw new Error("Admin API unavailable");
  }
  if (!response.ok) {
    let message = `Request failed with ${response.status}`;
    try {
      const body = (await response.json()) as { message?: string };
      message = body.message ?? message;
    } catch {
      // Keep the stable fallback message.
    }
    throw new Error(message);
  }
  return (await response.json()) as T;
}

async function sendJson<T>(path: string, method: "PUT" | "POST", body: unknown): Promise<T> {
  return fetchJson<T>(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
}

async function postAction<T>(path: string): Promise<T> {
  return fetchJson<T>(path, { method: "POST" });
}

async function deleteAction<T>(path: string): Promise<T> {
  return fetchJson<T>(path, { method: "DELETE" });
}

function useAsyncData<T>(
  factory: () => Promise<T>,
  deps: DependencyList,
  enabled = true
): LoadState<T> {
  const [state, setState] = useState<LoadState<T>>({ status: "loading" });

  useEffect(() => {
    let cancelled = false;
    if (!enabled) {
      setState({ status: "error", message: "Admin token required" });
      return () => {
        cancelled = true;
      };
    }
    setState({ status: "loading" });
    factory()
      .then((data) => {
        if (!cancelled) {
          setState({ status: "ready", data });
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setState({ status: "error", message: error instanceof Error ? error.message : "Unknown error" });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [...deps, enabled]);

  return state;
}

export default function App(): ReactElement {
  const [view, setView] = useState<View>("overview");
  const [tokenReady, setTokenReady] = useState<boolean>(() =>
    Boolean(window.sessionStorage.getItem("pgapp_admin_token"))
  );
  const overview = useAsyncData<Overview>(
    () => fetchJson("/api/admin/overview"),
    [tokenReady],
    tokenReady
  );

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">
            <Server size={18} />
          </div>
          <div className="brand-copy">
            <span>pgapp Admin</span>
            <small>PostgreSQL-first ops</small>
          </div>
        </div>
        <nav className="nav-list" aria-label="Admin sections">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                key={item.view}
                type="button"
                className={view === item.view ? "nav-button active" : "nav-button"}
                onClick={() => setView(item.view)}
                aria-label={item.label}
              >
                <Icon size={18} />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
        <div className="sidebar-note">
          <strong>PG-first control plane</strong>
          <span>Cache, MQ, Config, logs, and client activity backed by PostgreSQL.</span>
        </div>
      </aside>
      <section className="workspace">
        <header className="topbar">
          <label className="global-search">
            <Search size={17} />
            <input aria-label="Search admin resources" type="search" placeholder="Search Cache, MQ, Config, logs..." />
          </label>
          <div className="top-actions">
            <span className="icon-status" aria-label="Admin token stored">
              <ShieldCheck size={17} />
            </span>
            <StatusPill state={overview.status === "ready" ? overview.data.server_state : "loading"} />
          </div>
        </header>
        <div className="page">
          <header className="page-head">
            <div>
              <p className="eyebrow">local admin</p>
              <h1>{titleFor(view)}</h1>
              <p className="head-desc">{descriptionFor(view)}</p>
            </div>
          </header>
          {!tokenReady && <TokenPrompt onSave={() => setTokenReady(true)} />}
          {tokenReady && view === "overview" && <OverviewView state={overview} />}
          {tokenReady && view === "cache" && <CacheView />}
          {tokenReady && view === "mq" && <MqView />}
          {tokenReady && view === "config" && <ConfigView />}
          {tokenReady && view === "logs" && <LogsView />}
          {tokenReady && view === "clients" && <ClientsView />}
        </div>
      </section>
    </main>
  );
}

function TokenPrompt({ onSave }: { onSave: () => void }): ReactElement {
  const [value, setValue] = useState<string>("");
  return (
    <section className="panel token-panel">
      <div className="panel-heading">
        <h2>Admin token</h2>
      </div>
      <form
        className="token-form"
        onSubmit={(event) => {
          event.preventDefault();
          if (value.trim()) {
            window.sessionStorage.setItem("pgapp_admin_token", value.trim());
            onSave();
          }
        }}
      >
        <input
          aria-label="Admin token"
          autoComplete="off"
          type="password"
          value={value}
          onChange={(event) => setValue(event.target.value)}
        />
        <button type="submit">Save</button>
      </form>
    </section>
  );
}

function titleFor(view: View): string {
  return {
    overview: "PGApp Console",
    cache: "Cache",
    mq: "MQ",
    config: "Config",
    logs: "Logs",
    clients: "Clients"
  }[view];
}

function descriptionFor(view: View): string {
  return {
    overview: "A PostgreSQL-backed operational surface for service health, traffic, and storage state.",
    cache: "Inspect namespace usage and cached entry previews without mutating runtime data.",
    mq: "Review queues and message previews while preserving delivery and acknowledgement state.",
    config: "Manage scoped JSON drafts, publish immutable releases, and inspect release history.",
    logs: "Trace persisted admin and service events from the server-side PostgreSQL log store.",
    clients: "Observe Admin sessions and API method activity across SDK and console traffic."
  }[view];
}

function StatusPill({ state }: { state: string }): ReactElement {
  return <span className={`status-pill ${state === "ready" ? "good" : ""}`}>{state}</span>;
}

function OverviewView({ state }: { state: LoadState<Overview> }): ReactElement {
  if (state.status !== "ready") {
    return <StateMessage state={state} />;
  }
  const data = state.data;
  return (
    <div className="stack">
      <section className="metric-grid" aria-label="Overview metrics">
        <Metric title="PG Pool" value={`${data.pg_pool.idle}/${data.pg_pool.size}`} detail="idle / size" icon={Database} />
        <Metric title="Cache keys" value={data.cache_summary.logical_key_count.toString()} detail={`${data.cache_summary.logical_byte_size} bytes`} icon={Activity} />
        <Metric title="MQ queues" value={data.mq_summary.queue_count.toString()} detail={`${data.mq_summary.visible_message_count} visible`} icon={MessageSquareText} />
        <Metric title="Errors" value={sumErrors(data.runtime_metrics.methods).toString()} detail={`${data.runtime_metrics.methods.length} methods`} icon={Clock} />
      </section>
      <section className="panel">
        <div className="panel-heading">
          <h2>Runtime</h2>
        </div>
        <DataTable
          columns={["service", "method", "status", "count", "errors"]}
          rows={data.runtime_metrics.methods.map((metric) => [
            metric.service,
            metric.method,
            metric.status,
            metric.count,
            metric.errors
          ])}
          empty="No request metrics"
        />
      </section>
    </div>
  );
}

function CacheView(): ReactElement {
  const namespaces = useAsyncData<Page<CacheNamespace>>(
    () => fetchJson("/api/admin/cache/namespaces?limit=50&offset=0"),
    []
  );
  const entries = useAsyncData<Page<CacheEntry>>(
    () => fetchJson("/api/admin/cache/entries?limit=50&offset=0"),
    []
  );

  return (
    <div className="split">
      <section className="panel">
        <div className="panel-heading">
          <h2>Namespaces</h2>
        </div>
        <AsyncTable
          state={namespaces}
          columns={["namespace", "keys", "bytes"]}
          mapRow={(row) => [row.name, row.key_count, row.byte_size]}
          empty="No namespaces"
        />
      </section>
      <section className="panel">
        <div className="panel-heading">
          <h2>Entries</h2>
        </div>
        <AsyncTable
          state={entries}
          columns={["namespace", "key", "bytes", "encoding", "preview"]}
          mapRow={(row) => [row.namespace, row.key, row.size_bytes, row.value_encoding, row.value_preview]}
          empty="No entries"
        />
      </section>
    </div>
  );
}

function MqView(): ReactElement {
  const queues = useAsyncData<Page<QueueSummary>>(() => fetchJson("/api/admin/mq/queues?limit=50&offset=0"), []);
  const selectedQueue = queues.status === "ready" ? queues.data.items[0]?.name : undefined;
  const [feedback, setFeedback] = useState<string>("");
  const messages = useAsyncData<Page<QueueMessage>>(
    () => {
      if (!selectedQueue) {
        return Promise.resolve({ items: [], limit: 50, offset: 0, next_offset: null });
      }
      return fetchJson(`/api/admin/mq/queues/${selectedQueue}/messages?limit=50&offset=0`);
    },
    [selectedQueue]
  );
  const dlq = useAsyncData<Page<DlqMessage>>(
    () => {
      if (!selectedQueue) {
        return Promise.resolve({ items: [], limit: 50, offset: 0, next_offset: null });
      }
      return fetchJson(`/api/admin/mq/queues/${selectedQueue}/dlq?limit=50&offset=0`);
    },
    [selectedQueue]
  );

  async function reprocessDlq(messageId: number): Promise<void> {
    if (!selectedQueue) {
      return;
    }
    setFeedback("");
    try {
      await postAction<{ success: boolean }>(`/api/admin/mq/queues/${selectedQueue}/dlq/${messageId}/reprocess`);
      setFeedback(`Reprocessed ${messageId}`);
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  async function purgeDlq(): Promise<void> {
    if (!selectedQueue) {
      return;
    }
    setFeedback("");
    try {
      const result = await postAction<{ deleted_count: number }>(`/api/admin/mq/queues/${selectedQueue}/dlq/purge`);
      setFeedback(`Purged ${result.deleted_count}`);
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  return (
    <div className="stack">
      <div className="split">
        <section className="panel">
          <div className="panel-heading">
            <h2>Queues</h2>
          </div>
          <AsyncTable
            state={queues}
            columns={["queue", "visible", "in-flight", "archived"]}
            mapRow={(row) => [row.name, row.visible_message_count, row.in_flight_message_count, row.archived_message_count]}
            empty="No queues"
          />
        </section>
        <section className="panel">
          <div className="panel-heading">
            <h2>Messages</h2>
          </div>
          <AsyncTable
            state={messages}
            columns={["id", "queue", "reads", "payload"]}
            mapRow={(row) => [row.message_id, row.queue_name, row.read_count, row.payload_preview]}
            empty="No messages"
          />
        </section>
      </div>
      <section className="panel">
        <div className="panel-heading">
          <h2>Dead letters</h2>
          {selectedQueue && (
            <button type="button" className="icon-button" aria-label={`Purge DLQ for ${selectedQueue}`} onClick={() => void purgeDlq()}>
              <RefreshCcw size={16} />
              <span>Purge</span>
            </button>
          )}
        </div>
        {dlq.status === "ready" ? (
          <DataTable
            columns={["original id", "reads", "reason", "payload", "action"]}
            rows={dlq.data.items.map((message) => [
              message.original_message_id,
              message.read_count,
              message.reason,
              compactJson(message.payload),
              <button
                key={message.original_message_id}
                type="button"
                className="icon-button"
                aria-label={`Reprocess DLQ message ${message.original_message_id}`}
                onClick={() => void reprocessDlq(message.original_message_id)}
              >
                <RefreshCcw size={16} />
                <span>Reprocess</span>
              </button>
            ])}
            empty="No dead letters"
          />
        ) : (
          <StateMessage state={dlq} />
        )}
        {feedback && <p className={feedback.includes("failed") ? "state error compact panel-feedback" : "state compact panel-feedback"}>{feedback}</p>}
      </section>
    </div>
  );
}

function ConfigView(): ReactElement {
  const scopes = useAsyncData<Page<ConfigScopeSummary>>(
    () => fetchJson("/api/admin/config/scopes?limit=50&offset=0"),
    []
  );
  const [selectedScopeId, setSelectedScopeId] = useState<string>("");
  if (scopes.status !== "ready") {
    return <StateMessage state={scopes} />;
  }
  const selected =
    scopes.data.items.find((item) => scopeId(item.scope) === selectedScopeId)?.scope ??
    scopes.data.items[0]?.scope;
  if (!selected) {
    return (
      <section className="panel">
        <div className="panel-heading">
          <h2>Scopes</h2>
        </div>
        <p className="empty">No config scopes</p>
      </section>
    );
  }

  return (
    <div className="config-layout">
      <section className="panel">
        <div className="panel-heading">
          <h2>Scopes</h2>
        </div>
        <ScopeList
          scopes={scopes.data.items}
          selected={scopeId(selected)}
          onSelect={setSelectedScopeId}
        />
      </section>
      <ConfigDetails scope={selected} />
    </div>
  );
}

function ScopeList({
  scopes,
  selected,
  onSelect
}: {
  scopes: ConfigScopeSummary[];
  selected: string;
  onSelect: (id: string) => void;
}): ReactElement {
  if (scopes.length === 0) {
    return <p className="empty">No config scopes</p>;
  }
  return (
    <div className="scope-list">
      {scopes.map((item) => {
        const id = scopeId(item.scope);
        return (
          <button
            key={id}
            type="button"
            className={id === selected ? "scope-row active" : "scope-row"}
            aria-label={`Select scope ${item.scope.app_id}`}
            onClick={() => onSelect(id)}
          >
            <strong>{item.scope.app_id}</strong>
            <span>{item.scope.environment}</span>
            <span>{item.scope.cluster}</span>
            <span>{item.scope.namespace}</span>
            <small>Revision {item.current_revision}</small>
          </button>
        );
      })}
    </div>
  );
}

function ConfigDetails({ scope }: { scope: ConfigScope }): ReactElement {
  const query = scopeQuery(scope);
  const draft = useAsyncData<ConfigDraft>(() => fetchJson(`/api/admin/config/draft?${query}`), [query]);
  const releases = useAsyncData<Page<ConfigRelease>>(
    () => fetchJson(`/api/admin/config/releases?${query}`),
    [query]
  );
  const schema = useAsyncData<ConfigSchemaResponse>(
    () => fetchJson(`/api/admin/config/schema?${query}`),
    [query]
  );
  const [selectedKey, setSelectedKey] = useState<string>("");
  const [jsonValue, setJsonValue] = useState<string>("{\n}");
  const [schemaValue, setSchemaValue] = useState<string>("{\n}");
  const [feedback, setFeedback] = useState<string>("");

  useEffect(() => {
    if (draft.status !== "ready") {
      return;
    }
    const first = draft.data.items.find((item) => !item.deleted) ?? draft.data.items[0];
    if (!first) {
      setSelectedKey("");
      setJsonValue("{\n}");
      return;
    }
    setSelectedKey(first.key);
    setJsonValue(JSON.stringify(first.value, null, 2));
  }, [draft.status === "ready" ? JSON.stringify(draft.data.items) : draft.status]);

  useEffect(() => {
    if (schema.status !== "ready") {
      return;
    }
    setSchemaValue(schema.data.has_schema ? JSON.stringify(schema.data.schema, null, 2) : "{\n}");
  }, [schema.status === "ready" ? JSON.stringify(schema.data.schema) : schema.status]);

  async function saveItem(): Promise<void> {
    setFeedback("");
    let parsed: unknown;
    try {
      parsed = JSON.parse(jsonValue);
    } catch {
      setFeedback("Invalid JSON");
      return;
    }
    if (!selectedKey.trim()) {
      setFeedback("Key required");
      return;
    }
    try {
      await sendJson<{ success: boolean }>("/api/admin/config/items", "PUT", {
        scope,
        key: selectedKey.trim(),
        value: parsed
      });
      setFeedback("Draft saved");
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  async function saveSchema(): Promise<void> {
    setFeedback("");
    let parsed: unknown;
    try {
      parsed = JSON.parse(schemaValue);
    } catch {
      setFeedback("Invalid JSON Schema");
      return;
    }
    try {
      await sendJson<{ success: boolean }>("/api/admin/config/schema", "PUT", {
        scope,
        schema: parsed
      });
      setFeedback("Schema saved");
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  async function removeSchema(): Promise<void> {
    setFeedback("");
    try {
      await deleteAction<{ success: boolean }>(`/api/admin/config/schema?${query}`);
      setSchemaValue("{\n}");
      setFeedback("Schema removed");
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  async function publish(): Promise<void> {
    setFeedback("");
    try {
      await sendJson<ConfigRelease>("/api/admin/config/releases", "POST", {
        scope,
        message: "Admin UI publish",
        published_by: "admin-ui"
      });
      setFeedback("Release published");
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  return (
    <div className="config-detail">
      <section className="panel">
        <div className="panel-heading">
          <h2>Draft items</h2>
        </div>
        {draft.status === "ready" ? (
          <DataTable
            columns={["key", "state", "value"]}
            rows={draft.data.items.map((item) => [
              item.key,
              item.deleted ? "deleted" : "active",
              item.deleted ? "" : compactJson(item.value)
            ])}
            empty="No draft items"
          />
        ) : (
          <StateMessage state={draft} />
        )}
      </section>

      <section className="panel">
        <div className="panel-heading">
          <h2>JSON editor</h2>
          <div className="toolbar">
            <button type="button" className="icon-button" aria-label="Save config item" onClick={() => void saveItem()}>
              <Save size={16} />
              <span>Save</span>
            </button>
            <button type="button" className="icon-button primary" aria-label="Publish config release" onClick={() => void publish()}>
              <Rocket size={16} />
              <span>Publish</span>
            </button>
          </div>
        </div>
        <div className="editor-body">
          <label>
            <span>Key</span>
            <input value={selectedKey} onChange={(event) => setSelectedKey(event.target.value)} />
          </label>
          <label>
            <span>Value</span>
            <textarea
              aria-label="Config JSON value"
              value={jsonValue}
              onChange={(event) => setJsonValue(event.target.value)}
              spellCheck={false}
            />
          </label>
          {feedback && <p className={feedback.toLowerCase().includes("invalid") || feedback.includes("rejected") ? "state error compact" : "state compact"}>{feedback}</p>}
        </div>
      </section>

      <section className="panel">
        <div className="panel-heading">
          <h2>JSON Schema</h2>
          <div className="toolbar">
            <button type="button" className="icon-button" aria-label="Save config schema" onClick={() => void saveSchema()}>
              <Save size={16} />
              <span>Save</span>
            </button>
            <button type="button" className="icon-button" aria-label="Remove config schema" onClick={() => void removeSchema()}>
              <RefreshCcw size={16} />
              <span>Remove</span>
            </button>
          </div>
        </div>
        {schema.status === "ready" ? (
          <div className="editor-body">
            <label>
              <span>Schema</span>
              <textarea
                aria-label="Config JSON schema"
                value={schemaValue}
                onChange={(event) => setSchemaValue(event.target.value)}
                spellCheck={false}
              />
            </label>
          </div>
        ) : (
          <StateMessage state={schema} />
        )}
      </section>

      <section className="panel">
        <div className="panel-heading">
          <h2>Release history</h2>
          <Braces size={17} />
        </div>
        {releases.status === "ready" ? (
          <DataTable
            columns={["revision", "checksum", "message", "snapshot"]}
            rows={releases.data.items.map((release) => [
              `Revision ${release.revision}`,
              release.checksum,
              release.message,
              compactJson(release.snapshot)
            ])}
            empty="No releases"
          />
        ) : (
          <StateMessage state={releases} />
        )}
      </section>
    </div>
  );
}

function LogsView(): ReactElement {
  const logs = useAsyncData<Page<LogEvent>>(() => fetchJson("/api/admin/logs?limit=50&offset=0"), []);
  return (
    <section className="panel">
      <div className="panel-heading">
        <h2>Events</h2>
      </div>
      <AsyncTable
        state={logs}
        columns={["level", "target", "message", "request"]}
        mapRow={(row) => [row.level, row.target, row.message, row.request_id ?? ""]}
        empty="No logs"
      />
    </section>
  );
}

function ClientsView(): ReactElement {
  const clients = useAsyncData<ClientActivity>(() => fetchJson("/api/admin/clients"), []);
  const [clientKey, setClientKey] = useState<string>("");
  const [roles, setRoles] = useState<string>("");
  const [secret, setSecret] = useState<string>("");
  const [feedback, setFeedback] = useState<string>("");
  if (clients.status !== "ready") {
    return <StateMessage state={clients} />;
  }

  async function createClient(): Promise<void> {
    setFeedback("");
    setSecret("");
    if (!clientKey.trim()) {
      setFeedback("Client key required");
      return;
    }
    try {
      const created = await sendJson<CreatedClient>("/api/admin/clients", "POST", {
        client_key: clientKey.trim(),
        roles: roles
          .split(",")
          .map((role) => role.trim())
          .filter(Boolean)
      });
      setSecret(created.secret);
      setFeedback(`Created ${created.client_key}`);
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  async function rotateClient(clientKey: string): Promise<void> {
    setFeedback("");
    setSecret("");
    try {
      const rotated = await postAction<CreatedClient>(`/api/admin/clients/${clientKey}/rotate`);
      setSecret(rotated.secret);
      setFeedback(`Rotated ${rotated.client_key}`);
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  async function deactivateClient(clientKey: string): Promise<void> {
    setFeedback("");
    try {
      await postAction<{ success: boolean }>(`/api/admin/clients/${clientKey}/deactivate`);
      setFeedback(`Deactivated ${clientKey}`);
    } catch (error) {
      setFeedback(errorMessage(error));
    }
  }

  return (
    <div className="stack">
      <section className="panel">
        <div className="panel-heading">
          <h2>Client credentials</h2>
          <button type="button" className="icon-button primary" aria-label="Create client credential" onClick={() => void createClient()}>
            <Save size={16} />
            <span>Create</span>
          </button>
        </div>
        <div className="editor-body client-form">
          <label>
            <span>Key</span>
            <input aria-label="New client key" value={clientKey} onChange={(event) => setClientKey(event.target.value)} />
          </label>
          <label>
            <span>Roles</span>
            <input aria-label="New client roles" value={roles} onChange={(event) => setRoles(event.target.value)} />
          </label>
          {secret && <p className="state compact secret-output">{secret}</p>}
          {feedback && <p className={feedback.includes("required") ? "state error compact" : "state compact"}>{feedback}</p>}
        </div>
        <DataTable
          columns={["key", "active", "roles", "updated", "actions"]}
          rows={clients.data.items.map((credential) => [
            credential.client_key,
            credential.active ? "active" : "inactive",
            credential.roles.join(", "),
            credential.updated_at,
            <div key={credential.client_key} className="row-actions">
              <button type="button" className="icon-button" aria-label={`Rotate ${credential.client_key}`} onClick={() => void rotateClient(credential.client_key)}>
                <RefreshCcw size={16} />
                <span>Rotate</span>
              </button>
              <button type="button" className="icon-button" aria-label={`Deactivate ${credential.client_key}`} onClick={() => void deactivateClient(credential.client_key)}>
                <ShieldCheck size={16} />
                <span>Deactivate</span>
              </button>
            </div>
          ])}
          empty="No client credentials"
        />
      </section>
      <div className="split">
      <section className="panel">
        <div className="panel-heading">
          <h2>Admin sessions</h2>
        </div>
        <DataTable
          columns={["request", "path", "last seen"]}
          rows={clients.data.admin_sessions.map((session) => [
            session.request_id,
            session.path,
            session.last_seen_at
          ])}
          empty="No admin sessions"
        />
      </section>
      <section className="panel">
        <div className="panel-heading">
          <h2>API activity</h2>
        </div>
        <DataTable
          columns={["service", "method", "status", "count"]}
          rows={clients.data.api_activity.map((metric) => [
            metric.service,
            metric.method,
            metric.status,
            metric.count
          ])}
          empty="No API activity"
        />
      </section>
      </div>
    </div>
  );
}

function Metric({
  title,
  value,
  detail,
  icon: Icon
}: {
  title: string;
  value: string;
  detail: string;
  icon: ComponentType<{ size?: number }>;
}): ReactElement {
  return (
    <div className="metric">
      <div className="metric-icon">
        <Icon size={18} />
      </div>
      <div>
        <span>{title}</span>
        <strong>{value}</strong>
        <small>{detail}</small>
      </div>
    </div>
  );
}

function AsyncTable<T>({
  state,
  columns,
  mapRow,
  empty
}: {
  state: LoadState<Page<T>>;
  columns: string[];
  mapRow: (row: T) => TableRow;
  empty: string;
}): ReactElement {
  if (state.status !== "ready") {
    return <StateMessage state={state} />;
  }
  return <DataTable columns={columns} rows={state.data.items.map(mapRow)} empty={empty} />;
}

function DataTable({
  columns,
  rows,
  empty
}: {
  columns: string[];
  rows: TableRow[];
  empty: string;
}): ReactElement {
  if (rows.length === 0) {
    return <p className="empty">{empty}</p>;
  }
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              {row.map((cell, cellIndex) => (
                <td key={cellIndex}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function StateMessage<T>({ state }: { state: LoadState<T> }): ReactElement {
  if (state.status === "error") {
    return <p className="state error">{state.message}</p>;
  }
  return <p className="state">Loading</p>;
}

function sumErrors(methods: MethodMetric[]): number {
  return methods.reduce((total, metric) => total + metric.errors, 0);
}

function scopeQuery(scope: ConfigScope): string {
  const params = new URLSearchParams({
    app_id: scope.app_id,
    environment: scope.environment,
    cluster: scope.cluster,
    namespace: scope.namespace
  });
  return params.toString();
}

function scopeId(scope: ConfigScope): string {
  return `${scope.app_id}/${scope.environment}/${scope.cluster}/${scope.namespace}`;
}

function compactJson(value: unknown): string {
  return JSON.stringify(value);
}

type TableCell = string | number | ReactElement;
type TableRow = TableCell[];

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown error";
}
