import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import {
  ActorFilter,
  App,
  AutoupdateNotice,
  Filters,
  JobDetail,
  JobDetailPage,
  JobsList,
  KnowledgePage,
  KnowledgeProposals,
  KnowledgeRules,
  McpPage,
  ProductMeta,
  SectionNav,
  StatusBadge,
  SystemdUnits,
  UserMenu,
  WebPushControl,
  WebhookHookDetailPage,
  WebhookPage,
  buildJobQuery,
  buildKnowledgeQuery,
  changelogMarkdown,
  formatRuntimeUsageSeconds,
  groupSessionEvents,
  groupTranscriptEntries,
  hasActionableAutoupdate,
  isKnowledgePath,
  isMcpPath,
  isRetryableStatus,
  isSystemPath,
  isWebhooksPath,
  metricsSummaryPath,
  runtimeBucketLabel,
  selectedJobIdFromPath,
  shouldRefreshJobForSessionEvent,
  urlBase64ToUint8Array,
  webhookDeliveriesPath,
  webhookHooksPath,
  webhookQuerySelection,
  webhookTimeseriesPath,
  selectedWebhookHookIdFromPath,
  selectedWebhookDeliveryIdFromPath,
  WebhookDeliveryDetailPage,
} from "./main";

describe("dashboard routing and API query helpers", () => {
  it("builds trimmed job queries and preserves the requested limit", () => {
    expect(
      buildJobQuery(
        {
          status: " pending ",
          repo: " gisce/github-agent-bridge ",
          thread: "",
          action: " open_issue ",
          intent: " work_allowed ",
          actor: " ecarreras ",
        },
        24,
      ),
    ).toBe("/api/jobs?status=pending&repo=gisce%2Fgithub-agent-bridge&action=open_issue&intent=work_allowed&actor=ecarreras&limit=24");
  });

  it("builds knowledge queries and recognizes the knowledge route", () => {
    expect(buildKnowledgeQuery(" gisce/github-agent-bridge ", " proposed ", 25)).toBe("/api/knowledge?repo=gisce%2Fgithub-agent-bridge&status=proposed&limit=25");
    expect(isKnowledgePath("/knowledge")).toBe(true);
    expect(isKnowledgePath("/knowledge/")).toBe(true);
    expect(isKnowledgePath("/knowledge/extra")).toBe(false);
  });

  it("recognizes the MCP route", () => {
    expect(isMcpPath("/mcp")).toBe(true);
    expect(isMcpPath("/mcp/")).toBe(true);
    expect(isMcpPath("/mcp/tokens")).toBe(false);
  });

  it("recognizes the dedicated system route", () => {
    expect(isSystemPath("/system")).toBe(true);
    expect(isSystemPath("/system/")).toBe(true);
    expect(isSystemPath("/system/processes")).toBe(false);
  });

  it("recognizes the webhook monitoring route", () => {
    expect(isWebhooksPath("/webhooks")).toBe(true);
    expect(isWebhooksPath("/webhooks/")).toBe(true);
    expect(isWebhooksPath("/webhooks/github")).toBe(false);
  });

  it("loads only the active webhook dataset and builds bounded queries", () => {
    expect(webhookQuerySelection("overview")).toEqual({ timeseries: true, hooks: false, deliveries: false });
    expect(webhookQuerySelection("hooks")).toEqual({ timeseries: false, hooks: true, deliveries: false });
    expect(webhookQuerySelection("deliveries")).toEqual({ timeseries: false, hooks: false, deliveries: true });
    expect(webhookTimeseriesPath("2026-09-01T00:00:00Z", "2026-10-01T00:00:00Z"))
      .toBe("/api/webhooks/github/timeseries?from=2026-09-01T00%3A00%3A00Z&to=2026-10-01T00%3A00%3A00Z&bucket=day");
    expect(webhookDeliveriesPath(50, "next page"))
      .toBe("/api/webhooks/github/deliveries?limit=50&cursor=next+page");
    expect(webhookHooksPath(25, "next hook"))
      .toBe("/api/webhooks/github/hooks?limit=25&cursor=next+hook");
    expect(webhookDeliveriesPath(50, null, { hook_id: "42", event_name: " issue_comment " }))
      .toBe("/api/webhooks/github/deliveries?limit=50&hook_id=42&event_name=issue_comment");
  });

  it("recognizes only canonical job detail routes", () => {
    expect(selectedJobIdFromPath("/jobs/45")).toBe(45);
    expect(selectedJobIdFromPath("/jobs/45/")).toBe(45);
    expect(selectedJobIdFromPath("/jobs/not-a-number")).toBeNull();
    expect(selectedJobIdFromPath("/jobs/45/activity")).toBeNull();
  });

  it("recognizes shareable webhook hook detail routes", () => {
    expect(isWebhooksPath("/webhooks/hooks/690954530")).toBe(true);
    expect(selectedWebhookHookIdFromPath("/webhooks/hooks/690954530")).toBe("690954530");
    expect(selectedWebhookHookIdFromPath("/webhooks")).toBeNull();
  });

  it("recognizes shareable webhook delivery detail routes", () => {
    expect(isWebhooksPath("/webhooks/deliveries/abc%2F123")).toBe(true);
    expect(selectedWebhookDeliveryIdFromPath("/webhooks/deliveries/abc%2F123")).toBe("abc/123");
    expect(selectedWebhookDeliveryIdFromPath("/webhooks/hooks/42")).toBeNull();
  });

  it("shows a knowledge badge when proposed rules need moderation", () => {
    const onNavigate = vi.fn();
    const { rerender } = render(<SectionNav isDashboardRoute={true} isSystemRoute={false} isKnowledgeRoute={false} isMcpRoute={false} knowledgeBadgeCount={2} systemUpdateAvailable />);

    expect(screen.getByRole("link", { name: /Knowledge/i })).toContainElement(screen.getByLabelText("2 proposed knowledge items"));
    expect(screen.getByRole("link", { name: /System/i })).toContainElement(screen.getByLabelText("System update available"));
    expect(screen.getByRole("link", { name: /Jobs/i })).toHaveClass("bg-primary");
    expect(screen.getByRole("link", { name: /System/i })).not.toHaveClass("bg-primary");
    expect(screen.getByRole("link", { name: /MCP/i })).not.toHaveClass("bg-primary");

    rerender(<SectionNav isDashboardRoute={false} isSystemRoute={true} isKnowledgeRoute={false} isMcpRoute={false} knowledgeBadgeCount={0} onNavigate={onNavigate} />);
    expect(screen.getByRole("link", { name: /System/i })).toHaveClass("bg-primary");

    rerender(<SectionNav isDashboardRoute={false} isSystemRoute={false} isKnowledgeRoute={true} isMcpRoute={false} knowledgeBadgeCount={0} />);
    expect(screen.queryByLabelText(/proposed knowledge/i)).not.toBeInTheDocument();

    rerender(<SectionNav isDashboardRoute={false} isSystemRoute={false} isKnowledgeRoute={false} isMcpRoute={true} knowledgeBadgeCount={0} />);
    expect(screen.getByRole("link", { name: /MCP/i })).toHaveClass("bg-primary");
  });

  it("shows the webhook section only when shadow ingestion is configured", () => {
    const { rerender } = render(<SectionNav isDashboardRoute={true} isKnowledgeRoute={false} showWebhooks={false} />);
    expect(screen.queryByRole("link", { name: /Webhooks/i })).not.toBeInTheDocument();

    rerender(<SectionNav isDashboardRoute={false} isKnowledgeRoute={false} isWebhooksRoute={true} showWebhooks={true} />);
    expect(screen.getByRole("link", { name: /Webhooks/i })).toHaveClass("bg-primary");
  });

  it("renders the webhook status exported by the backend", () => {
    render(<WebhookPage summary={{ mode: "shadow", configured: true, receipts: { observed: 7 }, duplicate_deliveries: 2, cross_source_matches: 3, totals: { hooks: 143, deliveries: 912 } }} timeseries={[]} section="overview" summaryLoading={false} sectionLoading={false} loadingMore={false} hasMore={false} deliveryFilters={{ hook_id: "", event_name: "", repository: "", result: "", enqueue_status: "" }} error={null} onSectionChange={vi.fn()} onLoadMore={vi.fn()} onDeliveryFiltersChange={vi.fn()} onViewHook={vi.fn()} onRefresh={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "GitHub webhooks" })).toBeInTheDocument();
    expect(screen.getByText("shadow")).toBeInTheDocument();
    expect(screen.getByText("observed")).toBeInTheDocument();
    expect(screen.getAllByText("7").length).toBeGreaterThan(0);
    expect(screen.getByRole("tab", { name: "Hooks (143 total)" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Deliveries (912 total)" })).toBeInTheDocument();
  });

  it("navigates webhook hook inventory and delivery details", async () => {
    const user = userEvent.setup();
    const onSectionChange = vi.fn();
    const onViewHook = vi.fn();
    const summary = { mode: "shadow", configured: true, receipts: { observed: 7 }, duplicate_deliveries: 2, cross_source_matches: 3, totals: { hooks: 101, deliveries: 912 } };
    const hooks = [{ id: "42", target: "gisce", target_type: "organization" as const, active: true, events: ["issue_comment"], status: "receiving" as const, last_ping_at: "2026-10-02T10:00:00Z", last_event_at: "2026-10-02T10:05:00Z" }];
    const deliveries = [{ delivery_id: "delivery-1", created_at: "2026-10-02T10:05:00Z", hook_id: "42", hook: { id: "42", target: "gisce", target_type: "organization" as const }, event_name: "issue_comment", action: "created", repository: "gisce/github-agent-bridge", status: "observed", enqueue_status: "enqueued", job_id: 81 }];
    const common = { summary, summaryLoading: false, sectionLoading: false, loadingMore: false, hasMore: false, deliveryFilters: { hook_id: "", event_name: "", repository: "", result: "", enqueue_status: "" }, error: null, onSectionChange, onLoadMore: vi.fn(), onDeliveryFiltersChange: vi.fn(), onViewHook, onRefresh: vi.fn() };
    const { rerender } = render(<WebhookPage {...common} section="overview" />);

    expect(screen.getByRole("tab", { name: "Hooks (101 total)" })).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: /Hooks/ }));
    expect(onSectionChange).toHaveBeenCalledWith("hooks");
    rerender(<WebhookPage {...common} hooks={hooks} section="hooks" />);
    expect(screen.getByRole("tab", { name: "Hooks (101 total)" })).toBeInTheDocument();
    expect(screen.getByTestId("lazy-scroll-hooks")).toHaveClass("max-h-[640px]", "overflow-auto");
    expect(screen.getByText("organization · #42")).toBeInTheDocument();
    expect(screen.getByText("receiving")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "gisce" }));
    expect(onViewHook).toHaveBeenCalledWith("42");

    await user.click(screen.getByRole("tab", { name: /Deliveries/ }));
    expect(onSectionChange).toHaveBeenCalledWith("deliveries");
    rerender(<WebhookPage {...common} deliveries={deliveries} section="deliveries" />);
    expect(screen.getByTestId("lazy-scroll-deliveries")).toHaveClass("max-h-[640px]", "overflow-auto");
    expect(screen.getAllByText("issue_comment · created").length).toBeGreaterThan(0);
    expect(screen.getByText("gisce/github-agent-bridge")).toBeInTheDocument();
    expect(screen.getByText("#42")).toBeInTheDocument();
    expect(screen.getByText("enqueued")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Job #81" })).toHaveAttribute("href", "/jobs/81");
    expect(document.querySelector('time[datetime="2026-10-02T10:05:00.000Z"]')).toBeInTheDocument();
  });

  it("shows sanitized hook configuration and can request a fresh ping", async () => {
    const onPing = vi.fn().mockResolvedValue("GitHub accepted the ping request.");
    const user = userEvent.setup();
    render(<WebhookHookDetailPage data={{ hook: { id: "42", target: "gisce", target_type: "organization", name: "web", active: true, events: ["issue_comment"], content_type: "json", ssl_verify: true, delivery_url: "https://gab.gisce.net/api/webhooks/github", ping_url: "https://api.github.com/orgs/gisce/hooks/42/pings", github_created_at: "2026-10-02T11:07:17Z", github_updated_at: "2026-10-02T11:07:17Z", last_ping_at: "2026-10-02T11:07:20Z", last_event_at: "2026-10-02T11:08:00Z", last_delivery_id: "delivery-1", last_event_name: "issue_comment", last_action: "created", last_repository: "gisce/github-agent-bridge", last_result: "observed", status: "receiving", admin_url: "https://github.com/organizations/gisce/settings/hooks/42" }, stats: { deliveries: 3, duplicates: 1, unsupported: 0 }, recent_actions: [{ id: 1, action: "ping", actor: "operator", status: "succeeded", created_at: "2026-10-02T11:07:19Z" }], recent_deliveries: [{ delivery_id: "delivery-1", created_at: "2026-10-02T11:08:00Z", hook_id: "42", hook: { id: "42", target: "gisce", target_type: "organization" }, event_name: "issue_comment", action: "created", repository: "gisce/github-agent-bridge", status: "observed" }] }} loading={false} error={null} onBack={vi.fn()} onRefresh={vi.fn()} onViewHook={vi.fn()} onPing={onPing} />);

    expect(screen.getByRole("link", { name: /Open in GitHub/i })).toHaveAttribute("href", "https://github.com/organizations/gisce/settings/hooks/42");
    expect(screen.getByText("https://gab.gisce.net/api/webhooks/github")).toBeInTheDocument();
    expect(screen.getAllByText("issue_comment · created").length).toBeGreaterThan(0);
    expect(screen.getAllByText("delivery-1").length).toBeGreaterThan(0);
    expect(screen.getByText("by @operator")).toBeInTheDocument();
    expect(document.querySelectorAll("time").length).toBeGreaterThanOrEqual(6);
    expect(document.querySelector('time[datetime="2026-10-02T11:07:17.000Z"]')).toHaveAttribute("title", "UTC: 2026-10-02T11:07:17.000Z");
    await user.click(screen.getByRole("button", { name: "Send ping" }));
    expect(onPing).toHaveBeenCalledWith("42");
    expect(await screen.findByText("GitHub accepted the ping request.")).toBeInTheDocument();
  });

  it("shows the full webhook payload and linked job status", async () => {
    const onViewJob = vi.fn();
    const user = userEvent.setup();
    render(<WebhookDeliveryDetailPage data={{ delivery: { delivery_id: "delivery-1", created_at: "2026-10-02T11:08:00Z", hook_id: "42", event_name: "issue_comment", action: "created", event_key: "issue_comment:created:gisce/github-agent-bridge:7", repository: "gisce/github-agent-bridge", status: "observed" }, payload_hash: "abc123", payload: { action: "created", comment: { id: 7, body: "@giscebot fix it" } }, job: { id: 91, work_key: "gisce/github-agent-bridge#191", status: "running", action: "reply_comment", decision: "auto_trusted", work_intent: "work_allowed", updated_at: "2026-10-02T11:09:00Z" } }} loading={false} error={null} onBack={vi.fn()} onRefresh={vi.fn()} onViewHook={vi.fn()} onViewJob={onViewJob} />);

    expect(screen.getByText(/"body": "@giscebot fix it"/)).toBeInTheDocument();
    expect(screen.getByText("Job #91 · running · reply_comment · work_allowed")).toBeInTheDocument();
    expect(document.querySelector('time[datetime="2026-10-02T11:08:00.000Z"]')).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Open job" }));
    expect(onViewJob).toHaveBeenCalledWith(91);
  });

  it("loads the next hook cursor page when the inventory sentinel enters view", async () => {
    const onLoadMore = vi.fn();
    class ImmediateIntersectionObserver {
      constructor(private callback: IntersectionObserverCallback) {}
      observe(target: Element) {
        this.callback([{ isIntersecting: true, target } as IntersectionObserverEntry], this as unknown as IntersectionObserver);
      }
      disconnect() {}
      unobserve() {}
      takeRecords() { return []; }
    }
    vi.stubGlobal("IntersectionObserver", ImmediateIntersectionObserver);
    render(<WebhookPage summary={{ mode: "shadow", configured: true, receipts: {}, duplicate_deliveries: 0, cross_source_matches: 0 }} hooks={[{ id: "42", target: "gisce", target_type: "organization", active: true, events: [], status: "quiet" }]} section="hooks" summaryLoading={false} sectionLoading={false} loadingMore={false} hasMore deliveryFilters={{ hook_id: "", event_name: "", repository: "", result: "", enqueue_status: "" }} error={null} onSectionChange={vi.fn()} onLoadMore={onLoadMore} onDeliveryFiltersChange={vi.fn()} onViewHook={vi.fn()} onRefresh={vi.fn()} />);

    await waitFor(() => expect(onLoadMore).toHaveBeenCalledTimes(1));
    vi.unstubAllGlobals();
  });

  it("fetches and accumulates cursor-paginated hooks through the dashboard", async () => {
    window.history.replaceState({}, "", "/webhooks");
    class ImmediateIntersectionObserver {
      constructor(private callback: IntersectionObserverCallback) {}
      observe(target: Element) {
        this.callback([{ isIntersecting: true, target } as IntersectionObserverEntry], this as unknown as IntersectionObserver);
      }
      disconnect() {}
      unobserve() {}
      takeRecords() { return []; }
    }
    vi.stubGlobal("IntersectionObserver", ImmediateIntersectionObserver);
    const jsonResponse = (body: unknown) => Promise.resolve(new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } }));
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/api/status") return jsonResponse({ service: "github-agent-bridge-dashboard", read_only: false, admin_actions: [], webhook_configured: true, autoupdate: {} });
      if (path === "/api/me") return jsonResponse({ user: { login: "operator", avatar_url: "", html_url: "", is_admin: true } });
      if (path === "/api/about") return jsonResponse({ service: "github-agent-bridge", version: "0.63.0", repository_url: "https://github.com/gisce/github-agent-bridge" });
      if (path === "/api/web-push/config") return jsonResponse({ status: { enabled: false } });
      if (path === "/api/webhooks/github/summary") return jsonResponse({ mode: "shadow", configured: true, receipts: {}, duplicate_deliveries: 0, cross_source_matches: 0 });
      if (path.startsWith("/api/webhooks/github/timeseries")) return jsonResponse({ from: "", to: "", bucket: "day", points: [] });
      if (path === "/api/webhooks/github/hooks?limit=50") return jsonResponse({ hooks: [{ id: "42", target: "gisce", target_type: "organization", active: true, events: [], status: "quiet" }], next_cursor: "page-2" });
      if (path === "/api/webhooks/github/hooks?limit=50&cursor=page-2") return jsonResponse({ hooks: [{ id: "41", target: "gisce/repository", target_type: "repository", active: true, events: [], status: "quiet" }], next_cursor: null });
      throw new Error(`Unexpected fetch: ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(<QueryClientProvider client={client}><App /></QueryClientProvider>);

    await user.click(await screen.findByRole("tab", { name: /Hooks/ }));
    expect(await screen.findByText("gisce/repository")).toBeInTheDocument();
    expect(screen.getByText("organization · #42")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith("/api/webhooks/github/hooks?limit=50&cursor=page-2", expect.anything());

    window.history.replaceState({}, "", "/");
    vi.unstubAllGlobals();
  });

  it("uses client-side navigation for dashboard section links", async () => {
    const user = userEvent.setup();
    const onNavigate = vi.fn();
    render(<SectionNav isDashboardRoute={false} isSystemRoute={false} isKnowledgeRoute={true} onNavigate={onNavigate} />);

    await user.click(screen.getByRole("link", { name: /Jobs/i }));

    expect(onNavigate).toHaveBeenCalledWith("/");
  });

  it("refreshes job data only for session events that can change job state", () => {
    expect(shouldRefreshJobForSessionEvent("claimed")).toBe(true);
    expect(shouldRefreshJobForSessionEvent("dispatch_finished")).toBe(true);
    expect(shouldRefreshJobForSessionEvent("done")).toBe(true);
    expect(shouldRefreshJobForSessionEvent("openclaw_stdout")).toBe(false);
    expect(shouldRefreshJobForSessionEvent("openclaw_stderr")).toBe(false);
  });

  it("limits retry actions to manually recoverable job states", () => {
    expect(isRetryableStatus("blocked")).toBe(true);
    expect(isRetryableStatus("denied")).toBe(true);
    expect(isRetryableStatus("waiting_approval")).toBe(true);
    expect(isRetryableStatus("pending")).toBe(false);
    expect(isRetryableStatus("running")).toBe(false);
    expect(isRetryableStatus("done")).toBe(false);
  });

  it("requests metrics using the browser timezone and labels runtime buckets", () => {
    expect(metricsSummaryPath("America/New_York")).toBe("/api/metrics/summary?timezone=America%2FNew_York");
    expect(runtimeBucketLabel("2026-06-02", "day")).toMatch(/Jun|2/);
    expect(runtimeBucketLabel("2026-06", "month")).toMatch(/Jun|2026/);
  });

  it("formats runtime usage as human-readable hours and minutes", () => {
    expect(formatRuntimeUsageSeconds(30)).toBe("30s");
    expect(formatRuntimeUsageSeconds(1800)).toBe("30m");
    expect(formatRuntimeUsageSeconds(5400)).toBe("1h 30m");
    expect(formatRuntimeUsageSeconds(7200)).toBe("2h");
  });

  it("decodes VAPID public keys for push manager subscriptions", () => {
    expect(Array.from(urlBase64ToUint8Array("AQIDBA"))).toEqual([1, 2, 3, 4]);
  });

  it("shows a compact disabled notification control before push is configured", () => {
    render(<WebPushControl config={{ configured: false, public_key: "", status: { enabled: false, subscriptions: [] } }} loading={false} onEnable={vi.fn()} onDisable={vi.fn()} />);

    expect(screen.getByRole("button", { name: "Notifications unavailable" })).toBeDisabled();
  });
});

describe("MCP access page", () => {
  const admin = { login: "admin", avatar_url: "", html_url: "https://github.com/admin", is_admin: true };

  it("lets admins create and revoke MCP tokens", async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn().mockResolvedValue({
      token: "gab_mcp_secret",
      record: {
        id: "token-1",
        name: "local agent",
        user_login: "bob",
        created_by: "admin",
        created_at: "2026-06-23T11:00:00Z",
        last_used_at: null,
        revoked_at: null,
        expires_at: null,
      },
      detail: "mcp_token_created",
    });
    const onRevoke = vi.fn().mockResolvedValue(undefined);
    const onUpdateOwner = vi.fn().mockResolvedValue(undefined);
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);

    render(
      <McpPage
        tokens={[
          {
            id: "token-1",
            name: "local agent",
            user_login: "bob",
            created_by: "admin",
            created_at: "2026-06-23T11:00:00Z",
            last_used_at: null,
            revoked_at: null,
            expires_at: null,
          },
        ]}
        loading={false}
        error={null}
        user={admin}
        ownerOptions={[
          { login: "admin", avatar_url: "", html_url: "https://github.com/admin", is_admin: true },
          { login: "bob", avatar_url: "", html_url: "https://github.com/bob", is_admin: false },
        ]}
        dashboardUrl="https://bridge.example.com/ops"
        dashboardUrlSource="configured"
        now={Date.parse("2026-06-23T11:05:00Z")}
        onCreate={onCreate}
        onUpdateOwner={onUpdateOwner}
        onRevoke={onRevoke}
        onRefresh={vi.fn()}
      />,
    );

    expect(screen.getByText("Connect an agent")).toBeInTheDocument();
    expect(screen.getByText("Public dashboard URL")).toBeInTheDocument();
    expect(screen.getByText("Configured public URL")).toBeInTheDocument();
    expect(screen.getByText("https://bridge.example.com/ops/mcp")).toBeInTheDocument();
    expect(screen.getByText("https://bridge.example.com/ops/api/mcp")).toBeInTheDocument();
    expect(screen.queryByText(/Set GITHUB_AGENT_BRIDGE_DASHBOARD_PUBLIC_URL/)).not.toBeInTheDocument();
    expect(screen.getByText("Remote agents connect directly with a bearer token; no local `gab` binary is required on the agent host.")).toBeInTheDocument();
    expect(screen.getByText(/\"url\": \"https:\/\/bridge.example.com\/ops\/api\/mcp\"/)).toBeInTheDocument();
    expect(screen.getByText(/\"Authorization\": \"Bearer/)).toBeInTheDocument();
    expect(screen.queryByText("Local fallback")).not.toBeInTheDocument();
    expect(screen.queryByText(/mcp-serve/)).not.toBeInTheDocument();

    await user.type(screen.getByLabelText("Token name"), "local agent");
    await user.selectOptions(screen.getByLabelText("Owner"), "bob");
    await user.click(screen.getByRole("button", { name: "Create token" }));

    expect(onCreate).toHaveBeenCalledWith("local agent", "bob");
    expect(screen.getAllByText("@bob").length).toBeGreaterThan(0);
    expect(await screen.findByText("gab_mcp_secret")).toBeInTheDocument();
    await user.selectOptions(screen.getAllByLabelText("Owner for local agent")[0], "admin");
    expect(onUpdateOwner).toHaveBeenCalledWith("token-1", "admin");

    await user.click(screen.getAllByRole("button", { name: "Revoke" })[0]);

    expect(confirm).toHaveBeenCalledWith("Revoke this MCP token?");
    expect(onRevoke).toHaveBeenCalledWith("token-1");
    confirm.mockRestore();
  });

  it("lets non-admin users manage their own MCP tokens", async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn().mockResolvedValue({
      token: "gab_mcp_reader_secret",
      record: {
        id: "token-2",
        name: "reader agent",
        user_login: "reader",
        created_by: "reader",
        created_at: "2026-06-23T11:00:00Z",
        last_used_at: null,
        revoked_at: null,
        expires_at: null,
      },
      detail: "mcp_token_created",
    });
    render(
      <McpPage
        tokens={[
          {
            id: "token-2",
            name: "reader agent",
            user_login: "reader",
            created_by: "reader",
            created_at: "2026-06-23T11:00:00Z",
            last_used_at: null,
            revoked_at: null,
            expires_at: null,
          },
        ]}
        loading={false}
        error={null}
        user={{ login: "reader", avatar_url: "", html_url: "https://github.com/reader", is_admin: false }}
        ownerOptions={[{ login: "reader", avatar_url: "", html_url: "https://github.com/reader", is_admin: false }]}
        dashboardUrl="https://bridge.example.com"
        dashboardUrlSource="configured"
        now={Date.parse("2026-06-23T11:05:00Z")}
        onCreate={onCreate}
        onUpdateOwner={vi.fn()}
        onRevoke={vi.fn()}
        onRefresh={vi.fn()}
      />,
    );

    expect(screen.getByText("Issue and revoke read-only tokens linked to your user.")).toBeInTheDocument();
    expect(screen.queryByLabelText("Owner")).not.toBeInTheDocument();

    await user.type(screen.getByLabelText("Token name"), "reader agent");
    await user.click(screen.getByRole("button", { name: "Create token" }));

    expect(onCreate).toHaveBeenCalledWith("reader agent", undefined);
    expect(await screen.findByText("gab_mcp_reader_secret")).toBeInTheDocument();
  });

  it("requires a configured public URL before showing a remote MCP endpoint", () => {
    render(
      <McpPage
        tokens={[]}
        loading={false}
        error={null}
        user={admin}
        ownerOptions={[admin]}
        dashboardUrl="http://127.0.0.1:8765"
        dashboardUrlSource="request"
        now={Date.parse("2026-06-23T11:05:00Z")}
        onCreate={vi.fn()}
        onUpdateOwner={vi.fn()}
        onRevoke={vi.fn()}
        onRefresh={vi.fn()}
      />,
    );

    expect(screen.getByText("Needs public URL")).toBeInTheDocument();
    expect(screen.getByText("Set GITHUB_AGENT_BRIDGE_DASHBOARD_PUBLIC_URL or forward X-Forwarded-* headers")).toBeInTheDocument();
    expect(screen.getByText("Public dashboard URL required before connecting remote agents")).toBeInTheDocument();
    expect(screen.queryByText("http://127.0.0.1:8765/mcp")).not.toBeInTheDocument();
    expect(screen.queryByText("http://127.0.0.1:8765/api/mcp")).not.toBeInTheDocument();
    expect(screen.getByText(/\"url\": \"https:\/\/bridge.example.com\/api\/mcp\"/)).toBeInTheDocument();
  });
});

describe("status badges", () => {
  const job = {
    id: 58,
    work_key: "gisce/github-agent-bridge#58",
    repo: "gisce/github-agent-bridge",
    thread: 58,
    status: "pending",
    action: "open_issue",
    decision: "allowed",
    intent: "work_allowed",
    subject: "El dot del badge queda per sobre del header de la taula",
    trigger_actor: "ecarreras",
    trigger_actor_avatar_url: null,
    attempts: 1,
    coalesced_count: 1,
    last_error: null,
    locked_by: null,
    created_at: "2026-05-31T19:11:06Z",
    updated_at: "2026-05-31T19:11:06Z",
    started_at: null,
    finished_at: null,
    queue_wait_seconds: null,
    runtime_seconds: null,
    github_urls: [],
    model_route: {
      configured: true,
      model: "openai/gpt-5.4-mini",
      thinking: "medium",
      summary: "model=openai/gpt-5.4-mini thinking=medium",
    },
  };

  it("pulses pending and running jobs, but leaves waiting approval static", () => {
    const { rerender } = render(<StatusBadge status="pending" />);
    expect(screen.getByText("pending").querySelector("span")).toHaveClass("animate-live-pulse");

    rerender(<StatusBadge status="running" />);
    expect(screen.getByText("running").querySelector("span")).toHaveClass("animate-live-pulse");

    rerender(<StatusBadge status="waiting_approval" />);
    expect(screen.getByText("waiting_approval").querySelector("span")).not.toHaveClass("animate-live-pulse");
  });

  it("keeps the jobs table header above animated status dots while hiding model routing from the list", () => {
    render(
      <JobsList
        jobs={[job]}
        loading={false}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={() => undefined}
      />,
    );

    expect(screen.getByRole("columnheader", { name: "Status" }).parentElement).toHaveClass("sticky", "top-0", "z-10");
    expect(screen.getByTestId("lazy-scroll-jobs")).toHaveClass("max-h-[640px]", "overflow-auto");
    expect(screen.getByRole("columnheader", { name: "Job" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Model" })).not.toBeInTheDocument();
    expect(screen.queryByText("openai/gpt-5.4-mini · medium")).not.toBeInTheDocument();
    expect(screen.getAllByText("El dot del badge queda per sobre del header de la taula").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByLabelText("Work allowed: work_allowed").length).toBeGreaterThanOrEqual(1);
  });

  it("keeps long desktop job titles in the flexible job column", () => {
    const longSubject = "Improve the desktop dashboard table so a very long GitHub issue title stays readable without pushing status, actor, timing, updated, or action controls off the row";
    render(
      <JobsList
        jobs={[
          {
            ...job,
            id: 154,
            thread: 154,
            subject: longSubject,
            action: "open_issue_with_an_unusually_long_action_name",
          },
        ]}
        loading={false}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={() => undefined}
      />,
    );

    expect(screen.getByRole("columnheader", { name: "Job" })).toHaveClass("w-[42%]");
    expect(screen.getAllByText(longSubject).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByTitle(longSubject)).toHaveClass("line-clamp-2", "[overflow-wrap:anywhere]");
    expect(screen.getByRole("columnheader", { name: "Timing" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Attempts" })).not.toBeInTheDocument();
  });

  it("shows review mode with an icon in the jobs list", () => {
    render(
      <JobsList
        jobs={[{ ...job, intent: "review_only", action_mode: "review_only" }]}
        loading={false}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={() => undefined}
      />,
    );

    expect(screen.getAllByLabelText("Review only: review_only").length).toBeGreaterThanOrEqual(1);
  });

  it("uses the action mode for the visual state when it differs from the stored intent", () => {
    render(
      <JobDetail
        job={{ ...job, intent: "work_allowed", action_mode: "review_only", worklog: [] }}
        session={undefined}
        sessionEvents={[]}
        transcript={[]}
        now={Date.parse("2026-06-08T16:40:00Z")}
      />,
    );

    expect(screen.getByLabelText("Review only: review_only")).toBeInTheDocument();
  });

  it("shows GitHub links at the top of the job detail", () => {
    const githubUrl = "https://github.com/gisce/github-agent-bridge/issues/114#issuecomment-4651153034";
    const { container } = render(
      <JobDetail
        job={{ ...job, github_urls: [githubUrl], worklog: [] }}
        session={undefined}
        sessionEvents={[]}
        transcript={[]}
        now={Date.parse("2026-06-08T16:40:00Z")}
      />,
    );

    expect(screen.getByRole("link", { name: githubUrl })).toHaveAttribute("href", githubUrl);
    expect(screen.getByLabelText("Sticky job header")).toHaveClass("sticky", "top-0", "z-20");
    const content = container.textContent ?? "";
    expect(content.indexOf("GitHub links")).toBeLessThan(content.indexOf("Queue wait"));
    expect(content.indexOf("GitHub links")).toBeLessThan(content.indexOf("Timeline"));
    expect(screen.getByLabelText("Work allowed: fix_allowed")).toBeInTheDocument();
    expect(screen.getByText("Reasoning")).toBeInTheDocument();
    expect(screen.getByText("medium")).toBeInTheDocument();
  });

  it("explains when a pending job is serialized behind the same work key", () => {
    render(
      <JobDetail
        job={{
          ...job,
          runnable: false,
          blocked_by_job_id: 57,
          queue_state: "serialized_by_work_key",
          worklog: [],
        }}
        session={undefined}
        sessionEvents={[]}
        transcript={[]}
        now={Date.parse("2026-06-08T16:40:00Z")}
      />,
    );

    expect(screen.getByText("Serialized behind running job #57 for this work key.")).toBeInTheDocument();
  });

  it("shows intent classifier decisions in the job detail", () => {
    render(
      <JobDetail
        job={{
          ...job,
          action_mode: "fix_allowed",
          intent_classifier: {
            enabled: true,
            parser: { action: "reply_comment", work_intent: "review_only" },
            llm: {
              addressed_to_agent: true,
              action: "reply_comment",
              work_intent: "work_allowed",
              write_permission: "state_change_allowed",
              scope: "Update the PR tests.",
              main_request: "Please fix the failing tests.",
              confidence: 0.91,
              reason: "The request asks the configured agent to modify repository state.",
              applied: true,
            },
          },
          worklog: [],
        }}
        session={undefined}
        sessionEvents={[]}
        transcript={[]}
        now={Date.parse("2026-06-08T16:40:00Z")}
      />,
    );

    expect(screen.getByText("Action mode")).toBeInTheDocument();
    expect(screen.getByText("fix_allowed")).toBeInTheDocument();
    expect(screen.getByText("state_change_allowed")).toBeInTheDocument();
    expect(screen.getByText("reply_comment / review_only")).toBeInTheDocument();
    expect(screen.getByText("reply_comment / work_allowed")).toBeInTheDocument();
    expect(screen.getByText("Update the PR tests.")).toBeInTheDocument();
    expect(screen.getByText("Please fix the failing tests.")).toBeInTheDocument();
    expect(screen.getByText("91%")).toBeInTheDocument();
  });

  it("returns from job detail through client-side dashboard navigation", async () => {
    const user = userEvent.setup();
    const onBackToDashboard = vi.fn();
    render(
      <JobDetailPage
        jobId={58}
        detail={<div>Job detail content</div>}
        selectedJob={job}
        user={{ login: "reader", avatar_url: "", html_url: "https://github.com/reader", is_admin: false }}
        onBackToDashboard={onBackToDashboard}
        onRetry={vi.fn()}
        onDismiss={vi.fn()}
        onCancel={vi.fn()}
        onRefresh={vi.fn()}
      />,
    );

    await user.click(screen.getByRole("link", { name: /Dashboard/i }));

    expect(onBackToDashboard).toHaveBeenCalledTimes(1);
  });

  it("loads the next jobs batch when the list sentinel enters view", async () => {
    const onLoadMore = vi.fn();
    class ImmediateIntersectionObserver {
      constructor(private callback: IntersectionObserverCallback) {}
      observe(target: Element) {
        this.callback([{ isIntersecting: true, target } as IntersectionObserverEntry], this as unknown as IntersectionObserver);
      }
      disconnect() {}
      unobserve() {}
      takeRecords() {
        return [];
      }
    }
    vi.stubGlobal("IntersectionObserver", ImmediateIntersectionObserver);

    render(
      <JobsList
        jobs={[job]}
        loading={false}
        hasMore
        loadingMore={false}
        onLoadMore={onLoadMore}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={() => undefined}
      />,
    );

    await waitFor(() => expect(onLoadMore).toHaveBeenCalledTimes(1));
    vi.unstubAllGlobals();
  });

  it("uses an explicit load more action for the mobile jobs list", async () => {
    const user = userEvent.setup();
    const onLoadMore = vi.fn();
    class IdleIntersectionObserver {
      observe() {}
      disconnect() {}
      unobserve() {}
      takeRecords() {
        return [];
      }
    }
    vi.stubGlobal("IntersectionObserver", IdleIntersectionObserver);

    render(
      <JobsList
        jobs={[job]}
        loading={false}
        hasMore
        loadingMore={false}
        onLoadMore={onLoadMore}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={() => undefined}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Load more jobs" }));

    expect(onLoadMore).toHaveBeenCalledTimes(1);
    vi.unstubAllGlobals();
  });

  it("does not request another jobs batch while one is already loading", () => {
    const onLoadMore = vi.fn();
    render(
      <JobsList
        jobs={[job]}
        loading={false}
        hasMore
        loadingMore
        onLoadMore={onLoadMore}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={() => undefined}
      />,
    );

    expect(screen.getAllByText("Loading more jobs...").length).toBeGreaterThan(0);
    expect(onLoadMore).not.toHaveBeenCalled();
  });

  it("lets admins retry recoverable jobs from the jobs list without opening the detail page", async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn().mockResolvedValue(undefined);
    const onViewJob = vi.fn();
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);

    render(
      <JobsList
        jobs={[
          {
            id: 58,
            work_key: "gisce/github-agent-bridge#58",
            repo: "gisce/github-agent-bridge",
            thread: 58,
            status: "blocked",
            action: "reply_comment",
            decision: "allowed",
            intent: "work_allowed",
            subject: "Needs a guarded retry from the list",
            trigger_actor: "ecarreras",
            trigger_actor_avatar_url: null,
            attempts: 1,
            coalesced_count: 1,
            last_error: null,
            locked_by: null,
            created_at: "2026-05-31T19:11:06Z",
            updated_at: "2026-05-31T19:11:06Z",
            started_at: null,
            finished_at: null,
            queue_wait_seconds: null,
            runtime_seconds: null,
            github_urls: [],
          },
        ]}
        loading={false}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={onViewJob}
        onRetry={onRetry}
        user={{ login: "admin", avatar_url: "", html_url: "https://github.com/admin", is_admin: true }}
      />,
    );

    await user.click(screen.getAllByRole("button", { name: "Retry job #58" })[0]);

    expect(confirm).toHaveBeenCalledWith("Retry job #58?");
    expect(onRetry).toHaveBeenCalledWith(58);
    expect(onViewJob).not.toHaveBeenCalled();
    confirm.mockRestore();
  });

  it("lets admins dismiss recoverable jobs from the jobs list without opening the detail page", async () => {
    const user = userEvent.setup();
    const onDismiss = vi.fn().mockResolvedValue(undefined);
    const onViewJob = vi.fn();
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);

    render(
      <JobsList
        jobs={[
          {
            id: 58,
            work_key: "gisce/github-agent-bridge#58",
            repo: "gisce/github-agent-bridge",
            thread: 58,
            status: "blocked",
            action: "reply_comment",
            decision: "allowed",
            intent: "work_allowed",
            subject: "Needs an acknowledgement from the list",
            trigger_actor: "ecarreras",
            trigger_actor_avatar_url: null,
            attempts: 1,
            coalesced_count: 1,
            last_error: null,
            locked_by: null,
            created_at: "2026-05-31T19:11:06Z",
            updated_at: "2026-05-31T19:11:06Z",
            started_at: null,
            finished_at: null,
            queue_wait_seconds: null,
            runtime_seconds: null,
            github_urls: [],
          },
        ]}
        loading={false}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={onViewJob}
        onDismiss={onDismiss}
        user={{ login: "admin", avatar_url: "", html_url: "https://github.com/admin", is_admin: true }}
      />,
    );

    await user.click(screen.getAllByRole("button", { name: "Dismiss job #58" })[0]);

    expect(confirm).toHaveBeenCalledWith("Dismiss job #58?");
    expect(onDismiss).toHaveBeenCalledWith(58);
    expect(onViewJob).not.toHaveBeenCalled();
    confirm.mockRestore();
  });

  it("lets admins cancel running jobs from the jobs list with an optional reason", async () => {
    const user = userEvent.setup();
    const onCancel = vi.fn().mockResolvedValue(undefined);
    const onViewJob = vi.fn();
    const prompt = vi.spyOn(window, "prompt").mockReturnValue("operator requested stop");

    render(
      <JobsList
        jobs={[
          {
            id: 58,
            work_key: "gisce/github-agent-bridge#58",
            repo: "gisce/github-agent-bridge",
            thread: 58,
            status: "running",
            action: "reply_comment",
            decision: "allowed",
            intent: "work_allowed",
            subject: "Needs cancellation from the list",
            trigger_actor: "ecarreras",
            trigger_actor_avatar_url: null,
            attempts: 1,
            coalesced_count: 1,
            last_error: null,
            locked_by: "worker-1",
            created_at: "2026-05-31T19:11:06Z",
            updated_at: "2026-05-31T19:11:06Z",
            started_at: "2026-05-31T19:11:30Z",
            finished_at: null,
            queue_wait_seconds: 24,
            runtime_seconds: null,
            github_urls: [],
          },
        ]}
        loading={false}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={onViewJob}
        onCancel={onCancel}
        user={{ login: "admin", avatar_url: "", html_url: "https://github.com/admin", is_admin: true }}
      />,
    );

    await user.click(screen.getAllByRole("button", { name: "Cancel job #58" })[0]);

    expect(prompt).toHaveBeenCalledWith("Cancel job #58? Optional reason:");
    expect(onCancel).toHaveBeenCalledWith(58, "operator requested stop");
    expect(onViewJob).not.toHaveBeenCalled();
    prompt.mockRestore();
  });

  it("hides list retry actions from read-only users and non-retryable jobs", () => {
    render(
      <JobsList
        jobs={[
          {
            id: 58,
            work_key: "gisce/github-agent-bridge#58",
            repo: "gisce/github-agent-bridge",
            thread: 58,
            status: "pending",
            action: "reply_comment",
            decision: "allowed",
            intent: "work_allowed",
            subject: "Pending jobs are not manually retried",
            trigger_actor: "ecarreras",
            trigger_actor_avatar_url: null,
            attempts: 1,
            coalesced_count: 1,
            last_error: null,
            locked_by: null,
            created_at: "2026-05-31T19:11:06Z",
            updated_at: "2026-05-31T19:11:06Z",
            started_at: null,
            finished_at: null,
            queue_wait_seconds: null,
            runtime_seconds: null,
            github_urls: [],
          },
          {
            id: 59,
            work_key: "gisce/github-agent-bridge#59",
            repo: "gisce/github-agent-bridge",
            thread: 59,
            status: "running",
            action: "reply_comment",
            decision: "allowed",
            intent: "work_allowed",
            subject: "Running jobs owned by another actor cannot be cancelled",
            trigger_actor: "ecarreras",
            trigger_actor_avatar_url: null,
            attempts: 1,
            coalesced_count: 1,
            last_error: null,
            locked_by: "worker-1",
            created_at: "2026-05-31T19:11:06Z",
            updated_at: "2026-05-31T19:11:06Z",
            started_at: "2026-05-31T19:11:30Z",
            finished_at: null,
            queue_wait_seconds: 24,
            runtime_seconds: null,
            github_urls: [],
          },
        ]}
        loading={false}
        now={Date.parse("2026-05-31T19:12:00Z")}
        onViewJob={() => undefined}
        onRetry={vi.fn()}
        onDismiss={vi.fn()}
        onCancel={vi.fn()}
        user={{ login: "reader", avatar_url: "", html_url: "https://github.com/reader", is_admin: false }}
      />,
    );

    expect(screen.queryByRole("button", { name: "Retry job #58" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Dismiss job #58" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel job #59" })).not.toBeInTheDocument();
  });
});

describe("system page", () => {
  const systemdUnit = {
    role: "executor",
    kind: "service",
    unit: "github-agent-bridge.service",
    load_state: "loaded",
    active_state: "active",
    sub_state: "running",
    result: "success",
    exec_main_status: "0",
    main_pid: 123,
    uptime_seconds: 90,
    active_enter_timestamp: "Sat 2026-06-06 09:00:00 UTC",
    inactive_enter_timestamp: "",
    next_elapse: "",
    last_trigger: "",
    unit_file_state: "enabled",
    ok: true,
  };

  it("renders systemd service and timer status cards", () => {
    render(
      <SystemdUnits
        loading={false}
        data={{
          available: true,
          errors: [],
          units: [
            systemdUnit,
            {
              role: "reader",
              kind: "timer",
              unit: "github-agent-bridge-reader.timer",
              load_state: "loaded",
              active_state: "active",
              sub_state: "waiting",
              result: "success",
              exec_main_status: null,
              main_pid: null,
              uptime_seconds: null,
              active_enter_timestamp: "",
              inactive_enter_timestamp: "",
              next_elapse: "Sat 2026-06-06 09:15:00 UTC",
              last_trigger: "Sat 2026-06-06 09:10:00 UTC",
              unit_file_state: "enabled",
              ok: true,
            },
          ],
        }}
      />,
    );

    expect(screen.getByText("github-agent-bridge.service")).toBeInTheDocument();
    expect(screen.getByText("github-agent-bridge-reader.timer")).toBeInTheDocument();
    expect(screen.getByText("1m 30s")).toBeInTheDocument();
    expect(screen.getByText(/next Sat 2026-06-06/)).toBeInTheDocument();
  });

  it("streams a unit journal when its process row is expanded", async () => {
    const listeners = new Map<string, (message: MessageEvent) => void>();
    const close = vi.fn();
    const EventSourceMock = vi.fn(function (this: EventSource) {
      this.addEventListener = ((event: string, callback: (message: MessageEvent) => void) => {
        listeners.set(event, callback);
      }) as EventSource["addEventListener"];
      this.close = close;
      this.onerror = null;
    });
    vi.stubGlobal("EventSource", EventSourceMock);

    render(
      <SystemdUnits
        loading={false}
        data={{
          available: true,
          errors: [],
          units: [systemdUnit],
        }}
      />,
    );

    expect(EventSourceMock).not.toHaveBeenCalled();

    const rowSummary = screen.getByText("github-agent-bridge.service").closest("summary");
    expect(rowSummary).not.toBeNull();
    fireEvent.click(rowSummary!);

    await waitFor(() => expect(EventSourceMock).toHaveBeenCalledWith("/api/systemd/journal/stream?unit=github-agent-bridge.service"));
    act(() => {
      listeners.get("journal_line")?.(new MessageEvent("journal_line", { data: JSON.stringify({ unit: "github-agent-bridge.service", line: "started worker" }) }));
    });

    expect(screen.getByText("started worker")).toBeInTheDocument();
    expect(screen.getByText("1 lines streamed")).toBeInTheDocument();

    fireEvent.click(rowSummary!);

    await waitFor(() => expect(close).toHaveBeenCalled());
    vi.unstubAllGlobals();
  });
});

describe("product metadata", () => {
  it("shows the bridge version and upstream repository link", () => {
    render(<ProductMeta about={{ service: "github-agent-bridge-dashboard", version: "0.18.7", repository_url: "https://github.com/gisce/github-agent-bridge" }} />);

    expect(screen.getByText("Operational dashboard")).toBeInTheDocument();
    expect(screen.getByText("v0.18.7")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /github/i })).toHaveAttribute("href", "https://github.com/gisce/github-agent-bridge");
  });
});

describe("autoupdate notice", () => {
  const updateState = {
    updated_at: "2026-06-04T16:30:00Z",
    installed_tag: "v0.27.0",
    target: {
      tag_name: "v0.28.0",
      url: "https://github.com/gisce/github-agent-bridge/releases/tag/v0.28.0",
      body: "## Changes\n- Add **safe** autoupdate planning\n- Improve [dashboard release visibility](https://github.com/gisce/github-agent-bridge/releases/tag/v0.28.0)",
    },
    decision: "stage_defer_executor_reload",
    executor_reload_pending: true,
    dashboard_applied_at: "2026-06-04T16:31:00Z",
    blocked_reason: "active_jobs_block_executor_reload",
    queue: { active_counts: { pending: 1 }, active_total: 1 },
    classification: { risk: "executor_or_queue", migration_files: [], risky_files: ["src/github_agent_bridge/queue.py"] },
    warnings: [],
  };

  it("treats only non-noop releases as actionable", () => {
    expect(hasActionableAutoupdate(updateState)).toBe(true);
    expect(hasActionableAutoupdate({ ...updateState, decision: "noop" })).toBe(false);
    expect(hasActionableAutoupdate({ ...updateState, target: undefined })).toBe(false);
  });

  it("keeps update attention on System and renders update controls only there", async () => {
    window.history.replaceState({}, "", "/");
    class ResizeObserverMock {
      observe() {}
      disconnect() {}
      unobserve() {}
    }
    vi.stubGlobal("ResizeObserver", ResizeObserverMock);
    const jsonResponse = (body: unknown) => Promise.resolve(new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } }));
    const emptyMetrics = {
      db_exists: true,
      status_counts: {},
      by_repo: {},
      by_action: {},
      by_intent: {},
      by_created_day: {},
      runtime_usage: { day: [], month: [] },
      runtime_seconds: { median: null, p90: null, p99: null },
      queue_wait_seconds: { median: null, p90: null, p99: null },
    };
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const path = String(input);
      if (path.startsWith("/api/metrics/summary")) return jsonResponse({ metrics: emptyMetrics });
      if (path === "/api/status") return jsonResponse({ service: "github-agent-bridge-dashboard", read_only: false, admin_actions: [], autoupdate: updateState });
      if (path === "/api/me") return jsonResponse({ user: { login: "operator", avatar_url: "", html_url: "", is_admin: true } });
      if (path === "/api/about") return jsonResponse({ service: "github-agent-bridge", version: "0.67.0", repository_url: "https://github.com/gisce/github-agent-bridge" });
      if (path === "/api/web-push/config") return jsonResponse({ configured: false, public_key: "", status: { enabled: false, subscriptions: [] } });
      if (path === "/api/jobs/actors") return jsonResponse({ actors: [] });
      if (path.startsWith("/api/jobs?")) return jsonResponse({ jobs: [] });
      if (path === "/api/processes") return jsonResponse({ running_jobs: [], executor: { service: "bridge", pid: null, children: [] }, signals: { live_process: { state: "idle", child_count: 0 }, process_activity: { state: "idle", idle_seconds: null, sample_ts: null }, semantic_progress: [], visible_progress: [] }, alerts: [], samples: [], detail: "" });
      if (path === "/api/systemd") return jsonResponse({ available: true, units: [], errors: [] });
      if (path === "/api/alerts") return jsonResponse({ alerts: [] });
      throw new Error(`Unexpected fetch: ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(<QueryClientProvider client={client}><App /></QueryClientProvider>);

    const systemLink = await screen.findByRole("link", { name: /System/ });
    expect(systemLink).toContainElement(await screen.findByLabelText("System update available"));
    expect(screen.queryByLabelText("Update available")).not.toBeInTheDocument();

    await user.click(systemLink);

    expect(await screen.findByLabelText("Update available")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /apply update/i })).toBeInTheDocument();

    window.history.replaceState({}, "", "/");
    vi.unstubAllGlobals();
  });

  it("shows release impact only to admins", () => {
    const { rerender } = render(<AutoupdateNotice state={updateState} isAdmin={false} />);
    expect(screen.queryByLabelText("Update available")).not.toBeInTheDocument();

    rerender(<AutoupdateNotice state={updateState} isAdmin={true} />);

    expect(screen.getByLabelText("Update available")).toBeInTheDocument();
    expect(screen.getByText("v0.28.0")).toBeInTheDocument();
    expect(screen.getByText("Dashboard reload can be staged; executor reload waits for the queue")).toBeInTheDocument();
    expect(screen.getByText("executor or queue")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Changes" })).toBeInTheDocument();
    expect(screen.getByText("safe")).toHaveClass("font-semibold");
    expect(screen.getByRole("link", { name: "dashboard release visibility" })).toHaveAttribute("href", "https://github.com/gisce/github-agent-bridge/releases/tag/v0.28.0");
    expect(screen.getByRole("link", { name: /^release$/i })).toHaveAttribute("href", "https://github.com/gisce/github-agent-bridge/releases/tag/v0.28.0");
  });

  it("offers manual admin actions for recorded autoupdate plans", async () => {
    const user = userEvent.setup();
    const onRefresh = vi.fn();
    const onApply = vi.fn();
    const onCompletePending = vi.fn();
    vi.stubGlobal("confirm", vi.fn(() => true));

    render(<AutoupdateNotice state={updateState} isAdmin={true} onRefresh={onRefresh} onApply={onApply} onCompletePending={onCompletePending} />);

    await user.click(screen.getByRole("button", { name: /check now/i }));
    await user.click(screen.getByRole("button", { name: /apply update/i }));
    await user.click(screen.getByRole("button", { name: /complete reload/i }));

    expect(onRefresh).toHaveBeenCalledTimes(1);
    expect(onApply).toHaveBeenCalledTimes(1);
    expect(onCompletePending).toHaveBeenCalledTimes(1);
  });

  it("does not show apply update for migration-blocked plans", () => {
    render(
      <AutoupdateNotice
        state={{ ...updateState, classification: { ...updateState.classification, migration_files: ["src/github_agent_bridge/sql/2.sql"] } }}
        isAdmin={true}
        onApply={vi.fn()}
      />,
    );

    expect(screen.queryByRole("button", { name: /apply update/i })).not.toBeInTheDocument();
  });

  it("does not offer completion before the update has been applied", () => {
    render(<AutoupdateNotice state={{ ...updateState, dashboard_applied_at: undefined }} isAdmin={true} onCompletePending={vi.fn()} />);

    expect(screen.getByText("executor reload pending")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /complete reload/i })).not.toBeInTheDocument();
  });

  it("keeps full changelog markdown for rendering", () => {
    expect(changelogMarkdown("  # v1\n\n- First\n* Second\nplain\n- Fourth\n- Fifth  ")).toBe("# v1\n\n- First\n* Second\nplain\n- Fourth\n- Fifth");
  });
});

describe("user menu", () => {
  it("shows admin and read-only modes beside the signed-in user", () => {
    const { rerender } = render(<UserMenu user={{ login: "alice", avatar_url: "", html_url: "https://github.com/alice", is_admin: true }} loading={false} />);
    expect(screen.getByText("Signed in · admin")).toBeInTheDocument();

    rerender(<UserMenu user={{ login: "bob", avatar_url: "", html_url: "https://github.com/bob", is_admin: false }} loading={false} />);
    expect(screen.getByText("Signed in · read-only")).toBeInTheDocument();
  });
});

describe("actor filter", () => {
  it("filters actors, selects a suggestion, and clears the selection", async () => {
    const user = userEvent.setup();
    let value = "";
    const options = [
      { login: "ecarreras", avatar_url: "https://example.com/ecarreras.png", job_count: 7, last_seen: "2026-05-25T12:00:00Z" },
      { login: "octocat", avatar_url: null, job_count: 2, last_seen: null },
    ];
    const onChange = (actor: string) => {
      value = actor;
      rerender(<ActorFilter value={value} options={options} onChange={onChange} />);
    };
    const { rerender } = render(<ActorFilter value={value} options={options} onChange={onChange} />);

    await user.type(screen.getByPlaceholderText("@login"), "eca");
    expect(screen.getByText("@ecarreras")).toBeInTheDocument();
    expect(screen.queryByText("@octocat")).not.toBeInTheDocument();

    await user.click(screen.getByText("@ecarreras"));
    expect(screen.getByPlaceholderText("@login")).toHaveValue("ecarreras");

    fireEvent.click(screen.getByLabelText("Clear actor filter"));
    expect(screen.getByPlaceholderText("@login")).toHaveValue("");
  });
});

describe("job filters", () => {
  it("shows applied filters while collapsed and clears them without expanding", async () => {
    const user = userEvent.setup();
    let filters = {
      status: "pending",
      repo: "gisce/github-agent-bridge",
      thread: "164",
      action: "open_issue",
      intent: "work_allowed",
      actor: "ecarreras",
    };
    const onChange = vi.fn((nextFilters: typeof filters) => {
      filters = nextFilters;
      rerender(<Filters filters={filters} actorOptions={[]} onChange={onChange} />);
    });
    const { rerender } = render(<Filters filters={filters} actorOptions={[]} onChange={onChange} />);

    const appliedFilters = within(screen.getByLabelText("Applied filters"));
    expect(appliedFilters.getByText("Status")).toBeInTheDocument();
    expect(appliedFilters.getByText("pending")).toBeInTheDocument();
    expect(appliedFilters.getByText("Repo")).toBeInTheDocument();
    expect(appliedFilters.getByText("gisce/github-agent-bridge")).toBeInTheDocument();
    expect(appliedFilters.getByText("@ecarreras")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Clear filters" }));

    expect(onChange).toHaveBeenLastCalledWith({ status: "", repo: "", thread: "", action: "", intent: "", actor: "" });
    expect(screen.queryByLabelText("Applied filters")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Clear filters" })).not.toBeInTheDocument();
  });

  it("clears all applied filter fields at once", async () => {
    const user = userEvent.setup();
    let filters = {
      status: "pending",
      repo: "gisce/github-agent-bridge",
      thread: "82",
      action: "open_issue",
      intent: "work_allowed",
      actor: "ecarreras",
    };
    const onChange = vi.fn((nextFilters: typeof filters) => {
      filters = nextFilters;
      rerender(<Filters filters={filters} actorOptions={[]} onChange={onChange} />);
    });
    const { rerender } = render(<Filters filters={filters} actorOptions={[]} onChange={onChange} />);

    expect(screen.getByLabelText("Repository")).toHaveValue("gisce/github-agent-bridge");
    expect(screen.getByLabelText("Thread")).toHaveValue("82");
    await user.click(screen.getByRole("button", { name: "Clear" }));

    expect(onChange).toHaveBeenLastCalledWith({ status: "", repo: "", thread: "", action: "", intent: "", actor: "" });
    expect(screen.getByLabelText("Status")).toHaveValue("");
    expect(screen.getByLabelText("Repository")).toHaveValue("");
    expect(screen.getByLabelText("Thread")).toHaveValue("");
    expect(screen.getByLabelText("Action")).toHaveValue("");
    expect(screen.getByPlaceholderText("@login")).toHaveValue("");
    expect(screen.getByLabelText("Intent")).toHaveValue("");
    expect(screen.getByRole("button", { name: "Clear" })).toBeDisabled();
  });
});

describe("knowledge proposals", () => {
  it("keeps knowledge records separated behind tabs", async () => {
    const user = userEvent.setup();
    render(
      <KnowledgePage
        data={{
          repositories: ["gisce/github-agent-bridge"],
          summary: { proposed: 1, approved: 0, rules: 1, events: 1 },
          proposals: [
            {
              id: "feedback-proposal-1",
              event_id: "event-1",
              created_at: "2026-06-04T10:00:00Z",
              updated_at: "2026-06-04T10:01:00Z",
              status: "proposed",
              scope: "repo:gisce/github-agent-bridge",
              type: "operating_rule",
              confidence: 0.72,
              rule: "Keep knowledge moderation auditable.",
              reason: "A reusable process correction.",
              model: "gpt-test",
              error: null,
              source_event: null,
            },
          ],
          rules: [
            {
              id: "rule-1",
              scope: "repo:gisce/github-agent-bridge",
              type: "style_preference",
              rule: "Keep rule rows compact.",
              confidence: 0.82,
              observations: 2,
              source_events: ["event-1"],
              created_at: "2026-06-04T10:00:00Z",
              last_seen: "2026-06-04T10:01:00Z",
              source_event_details: [
                {
                  id: "event-1",
                  occurred_at: "2026-06-04T10:00:00Z",
                  captured_at: "2026-06-04T10:01:00Z",
                  source: "github",
                  scope: "repo:gisce/github-agent-bridge",
                  actor: "ecarreras",
                  trigger_actor: "ecarreras",
                  trigger_actor_avatar_url: "https://avatars.githubusercontent.com/u/294235?v=4",
                  github_urls: ["https://github.com/gisce/github-agent-bridge/issues/73#issuecomment-1"],
                  source_url: "https://github.com/gisce/github-agent-bridge/issues/73#issuecomment-1",
                  source_job_id: 510,
                  source_table: "job",
                  github_context: { urls: ["https://github.com/gisce/github-agent-bridge/issues/73#issuecomment-1"] },
                  comment: "Prefer tabs for knowledge.",
                  context: { issue: 73 },
                  classification: "style_preference",
                  confidence: 0.84,
                  memorable: true,
                },
              ],
            },
          ],
          events: [
            {
              id: "event-1",
              occurred_at: "2026-06-04T10:00:00Z",
              captured_at: "2026-06-04T10:01:00Z",
              source: "github",
              scope: "repo:gisce/github-agent-bridge",
              actor: "ecarreras",
              trigger_actor: "ecarreras",
              trigger_actor_avatar_url: "https://avatars.githubusercontent.com/u/294235?v=4",
              github_urls: ["https://github.com/gisce/github-agent-bridge/issues/73#issuecomment-1"],
              source_url: "https://github.com/gisce/github-agent-bridge/issues/73#issuecomment-1",
              source_job_id: 510,
              source_table: "job",
              github_context: { urls: ["https://github.com/gisce/github-agent-bridge/issues/73#issuecomment-1"] },
              comment: "Prefer tabs for knowledge.",
              context: { issue: 73 },
              classification: "style_preference",
              confidence: 0.84,
              memorable: true,
            },
          ],
        }}
        loading={false}
        error={null}
        repo=""
        status="proposed"
        user={{ login: "admin", avatar_url: "", html_url: "https://github.com/admin", is_admin: true }}
        now={Date.parse("2026-06-04T10:02:00Z")}
        onRepoChange={vi.fn()}
        onStatusChange={vi.fn()}
        onApprove={vi.fn()}
        onReject={vi.fn()}
        onUpdateRuleScope={vi.fn()}
        onDeleteRule={vi.fn()}
        onRefresh={vi.fn()}
      />,
    );

    expect(screen.queryByRole("link", { name: /^Dashboard$/i })).not.toBeInTheDocument();
    expect(screen.getByText("Keep knowledge moderation auditable.")).toBeInTheDocument();
    expect(screen.queryByText("Keep rule rows compact.")).not.toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: /rules \(1\)/i }));
    expect(screen.getByText("Keep rule rows compact.")).toBeInTheDocument();
    expect(screen.queryByText("Keep knowledge moderation auditable.")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Proposal status")).not.toBeInTheDocument();
    expect(screen.getByText("@ecarreras")).toBeInTheDocument();
    expect(screen.getByText("Job #510")).toBeInTheDocument();
    expect(screen.getByText("gisce/github-agent-bridge/issues/73#issuecomment-1")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: /events \(1\)/i }));
    expect(screen.getByText("Prefer tabs for knowledge.")).toBeInTheDocument();
    expect(screen.getByText("@ecarreras")).toBeInTheDocument();
    expect(screen.getByText("Job #510")).toBeInTheDocument();
    expect(screen.getByText("gisce/github-agent-bridge/issues/73#issuecomment-1")).toBeInTheDocument();
  });

  it("lets manageable curated rules edit scope only after entering edit mode", async () => {
    const user = userEvent.setup();
    const onUpdateRuleScope = vi.fn().mockResolvedValue(undefined);
    const rules = [
      {
        id: "rule-1",
        scope: "repo:gisce/github-agent-bridge",
        type: "style_preference",
        rule: "Keep rule rows compact.",
        confidence: 0.82,
        observations: 2,
        source_events: [],
        created_at: "2026-06-04T10:00:00Z",
        last_seen: "2026-06-04T10:01:00Z",
        source_event_details: [],
        can_manage: false,
      },
    ];

    const { rerender } = render(
      <KnowledgeRules
        rules={rules}
        loading={false}
        now={Date.parse("2026-06-04T10:02:00Z")}
        onUpdateRuleScope={onUpdateRuleScope}
        onDeleteRule={vi.fn()}
      />,
    );
    expect(screen.queryByLabelText("Scope")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /edit/i })).not.toBeInTheDocument();

    rerender(
      <KnowledgeRules
        rules={[{ ...rules[0], can_manage: true }]}
        loading={false}
        now={Date.parse("2026-06-04T10:02:00Z")}
        onUpdateRuleScope={onUpdateRuleScope}
        onDeleteRule={vi.fn()}
      />,
    );
    expect(screen.queryByLabelText("Scope")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /edit/i }));
    await user.selectOptions(screen.getByLabelText("Scope"), "global");
    await user.click(screen.getByRole("button", { name: /save/i }));

    expect(onUpdateRuleScope).toHaveBeenCalledWith("rule-1", "global");
  });

  it("cancels curated rule scope edits without saving", async () => {
    const user = userEvent.setup();
    const onUpdateRuleScope = vi.fn().mockResolvedValue(undefined);
    render(
      <KnowledgeRules
        rules={[
          {
            id: "rule-1",
            scope: "repo:gisce/github-agent-bridge",
            type: "style_preference",
            rule: "Keep rule rows compact.",
            confidence: 0.82,
            observations: 2,
            source_events: [],
            created_at: "2026-06-04T10:00:00Z",
            last_seen: "2026-06-04T10:01:00Z",
            source_event_details: [],
            can_manage: true,
          },
        ]}
        loading={false}
        now={Date.parse("2026-06-04T10:02:00Z")}
        onUpdateRuleScope={onUpdateRuleScope}
        onDeleteRule={vi.fn()}
      />,
    );

    await user.click(screen.getByRole("button", { name: /edit/i }));
    await user.selectOptions(screen.getByLabelText("Scope"), "global");
    await user.click(screen.getByRole("button", { name: /cancel/i }));

    expect(screen.queryByLabelText("Scope")).not.toBeInTheDocument();
    expect(onUpdateRuleScope).not.toHaveBeenCalled();
  });

  it("shows moderation actions only to admins for proposed rules", async () => {
    const user = userEvent.setup();
    const onApprove = vi.fn().mockResolvedValue(undefined);
    const onReject = vi.fn().mockResolvedValue(undefined);
    const proposals = [
      {
        id: "feedback-proposal-1",
        event_id: "event-1",
        created_at: "2026-06-04T10:00:00Z",
        updated_at: "2026-06-04T10:01:00Z",
        status: "proposed",
        scope: "repo:gisce/github-agent-bridge",
        type: "operating_rule",
        confidence: 0.72,
        rule: "Keep knowledge moderation auditable.",
        reason: "A reusable process correction.",
        model: "gpt-test",
        error: null,
        source_event: null,
      },
    ];

    const { rerender } = render(<KnowledgeProposals proposals={proposals} loading={false} isAdmin={false} now={Date.parse("2026-06-04T10:02:00Z")} onApprove={onApprove} onReject={onReject} />);
    expect(screen.queryByRole("button", { name: "Approve" })).not.toBeInTheDocument();

    rerender(<KnowledgeProposals proposals={proposals} loading={false} isAdmin={true} now={Date.parse("2026-06-04T10:02:00Z")} onApprove={onApprove} onReject={onReject} />);
    await user.click(screen.getByRole("button", { name: "Approve" }));

    expect(onApprove).toHaveBeenCalledWith("feedback-proposal-1");
    expect(onReject).not.toHaveBeenCalled();
  });

  it("shows proposal source actor and links", () => {
    const proposals = [
      {
        id: "feedback-proposal-1",
        event_id: "event-1",
        created_at: "2026-06-04T10:00:00Z",
        updated_at: "2026-06-04T10:01:00Z",
        status: "proposed",
        scope: "repo:gisce/github-agent-bridge",
        type: "operating_rule",
        confidence: 0.72,
        rule: "Keep knowledge moderation auditable.",
        reason: "A reusable process correction.",
        model: "gpt-test",
        error: null,
        source_event: {
          id: "event-1",
          occurred_at: "2026-06-04T10:00:00Z",
          captured_at: "2026-06-04T10:01:00Z",
          source: "github",
          scope: "repo:gisce/github-agent-bridge",
          actor: "copilot-pull-request-reviewer[bot]",
          trigger_actor: "copilot-pull-request-reviewer[bot]",
          trigger_actor_avatar_url: "",
          github_urls: ["https://github.com/gisce/github-agent-bridge/pull/117#pullrequestreview-1"],
          source_url: "https://github.com/gisce/github-agent-bridge/pull/117#pullrequestreview-1",
          source_job_id: 510,
          source_table: "job",
          github_context: { urls: ["https://github.com/gisce/github-agent-bridge/pull/117#pullrequestreview-1"] },
          comment: "Preserve backward compatibility.",
          context: {},
          classification: "technical_criterion",
          confidence: 0.74,
          memorable: false,
        },
      },
    ];

    render(<KnowledgeProposals proposals={proposals} loading={false} isAdmin={false} now={Date.parse("2026-06-04T10:02:00Z")} onApprove={vi.fn()} onReject={vi.fn()} />);

    expect(screen.getByText("@copilot-pull-request-reviewer[bot]")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Job #510/i })).toHaveAttribute("href", "/jobs/510");
    expect(screen.getByRole("link", { name: /github.com\/gisce\/github-agent-bridge/i })).toHaveAttribute("href", "https://github.com/gisce/github-agent-bridge/pull/117#pullrequestreview-1");
  });
});

describe("log grouping", () => {
  it("collapses consecutive OpenClaw CLI events while preserving boundaries", () => {
    const grouped = groupSessionEvents([
      { id: 1, ts: "2026-05-25T12:00:00Z", job_id: 45, work_key: "repo#45", session_id: "s1", event_type: "openclaw_stdout", summary: "stdout", detail: "first line" },
      { id: 2, ts: "2026-05-25T12:00:01Z", job_id: 45, work_key: "repo#45", session_id: "s1", event_type: "openclaw_stdout", summary: "stdout", detail: "second line" },
      { id: 3, ts: "2026-05-25T12:00:02Z", job_id: 45, work_key: "repo#45", session_id: "s1", event_type: "agent_message", summary: "done", detail: null },
    ]);

    expect(grouped).toHaveLength(2);
    expect(grouped[0]).toMatchObject({ count: 2, summary: "stdout (2): first line" });
    expect(grouped[0].detail).toBe("first line\nsecond line");
    expect(grouped[1]).toMatchObject({ count: 1, summary: "done" });
  });

  it("collapses consecutive transcript CLI entries", () => {
    const grouped = groupTranscriptEntries([
      { timestamp: "2026-05-25T12:00:00Z", role: "assistant", kind: "openclaw_stderr", title: "stderr", text: "warning" },
      { timestamp: "2026-05-25T12:00:01Z", role: "assistant", kind: "openclaw_stderr", title: "stderr", text: "details" },
      { timestamp: "2026-05-25T12:00:02Z", role: "assistant", kind: "message", title: "message", text: "finished" },
    ]);

    expect(grouped).toHaveLength(2);
    expect(grouped[0]).toMatchObject({ count: 2, summary: "assistant · openclaw_stderr (2): warning" });
    expect(grouped[0].text).toBe("warning\ndetails");
  });
});
