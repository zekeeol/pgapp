import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import App from "./App";

const overview = {
  server_state: "ready",
  ready: true,
  pg_pool: { size: 5, idle: 3 },
  runtime_metrics: {
    methods: [{ service: "cache", method: "get", status: "ok", count: 12, errors: 0, total_latency_millis: 34 }]
  },
  cache_summary: {
    hits: 20,
    misses: 4,
    writes: 8,
    deletes: 1,
    evictions: 2,
    expired_removals: 0,
    logical_key_count: 2,
    logical_byte_size: 6
  },
  mq_summary: {
    queue_count: 1,
    visible_message_count: 1,
    in_flight_message_count: 0,
    archived_message_count: 1
  }
};

const pages = new Map<string, unknown>([
  ["/api/admin/overview", overview],
  ["/api/admin/cache/namespaces?limit=50&offset=0", { items: [{ name: "default", key_count: 2, byte_size: 6 }], limit: 50, offset: 0, next_offset: null }],
  ["/api/admin/cache/entries?limit=50&offset=0", { items: [{ namespace: "default", key: "a", size_bytes: 3, value_preview: "6f6e65", value_encoding: "hex", access_count: 0 }], limit: 50, offset: 0, next_offset: null }],
  ["/api/admin/mq/queues?limit=50&offset=0", { items: [{ name: "orders", visible_message_count: 1, in_flight_message_count: 0, archived_message_count: 1 }], limit: 50, offset: 0, next_offset: null }],
  ["/api/admin/mq/queues/orders/messages?limit=50&offset=0", { items: [{ queue_name: "orders", message_id: 7, read_count: 0, payload_preview: "{\"ok\":true}" }], limit: 50, offset: 0, next_offset: null }],
  ["/api/admin/logs?limit=50&offset=0", { items: [{ id: 1, level: "INFO", target: "pgapp_server::admin_http", message: "admin request completed", request_id: "req-1" }], limit: 50, offset: 0, next_offset: null }],
  ["/api/admin/clients", { admin_sessions: [{ request_id: "req-1", path: "/api/admin/overview", last_seen_at: "2026-05-25T00:00:00Z" }], api_activity: [{ service: "cache", method: "get", status: "ok", count: 12, errors: 0, total_latency_millis: 34 }] }]
]);

describe("App", () => {
  beforeEach(() => {
    window.sessionStorage.setItem("pgapp_admin_token", "test-token");
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = input.toString();
        const body = pages.get(url);
        if (!body) {
          return new Response(JSON.stringify({ code: "not_found", message: url }), { status: 404 });
        }
        return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
      })
    );
  });

  afterEach(() => {
    window.sessionStorage.clear();
    vi.unstubAllGlobals();
  });

  test("renders the operational console and read-only resource views", async () => {
    render(<App />);

    expect(await screen.findByText("pgapp Admin")).toBeInTheDocument();
    expect(screen.getByText("ready")).toBeInTheDocument();
    expect(screen.getByText("PG Pool")).toBeInTheDocument();
    expect(screen.getByText("Cache keys")).toBeInTheDocument();
    expect(screen.getByText("MQ queues")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Cache" }));
    expect(await screen.findAllByText("default")).toHaveLength(2);
    expect(screen.getByText("6f6e65")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /delete|set|invalidate/i })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "MQ" }));
    expect(await screen.findByText("orders")).toBeInTheDocument();
    expect(await screen.findByText("{\"ok\":true}")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /send|archive|purge|drop|ack/i })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Logs" }));
    expect(await screen.findByText("admin request completed")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Clients" }));
    expect(await screen.findByText("/api/admin/overview")).toBeInTheDocument();
    expect(screen.getByText("cache")).toBeInTheDocument();

    await waitFor(() => expect(fetch).toHaveBeenCalled());
  });
});
