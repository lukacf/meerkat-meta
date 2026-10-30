// Host-provided attention items for the "Needs you" list.
//
// A host configures same-origin JSON sources (console config
// `[[attention.sources]]`). Each answers `{"items": [...]}`; the console reads
// them with the operator's session and shows every item as a link card beside
// the gating approvals. The console never decides an item: the card's only
// action opens the host's own page (top level), which owns the decision.
// A source that fails is reported by its label and never hides approvals or
// another source's items.

export interface HostAttentionSourceConfig {
  id: string;
  label: string;
  url: string;
}

export interface HostAttentionDetail {
  label: string;
  value: string;
}

export interface HostAttentionItem {
  sourceId: string;
  sourceLabel: string;
  id: string;
  title: string;
  details: HostAttentionDetail[];
  href: string;
  tone?: string;
}

export interface HostAttentionSnapshot {
  status: "idle" | "loading" | "ready";
  items: HostAttentionItem[];
  /** Labels of the sources that could not be read on the last load. */
  failedSources: string[];
}

export const HOST_ATTENTION_LIMITS = {
  itemsPerSource: 50,
  detailsPerItem: 12,
  textLength: 300,
} as const;

const TONES = new Set(["attention", "info", "warning", "critical"]);

/** The operator panel path rules: one leading "/", no scheme or authority, no
 *  whitespace, control characters or backslashes, no "." or ".." segment in
 *  any spelling, at most 2048 UTF-8 bytes. */
export function isSameOriginPath(value: unknown): value is string {
  if (typeof value !== "string" || !value.startsWith("/") || value.startsWith("//")) return false;
  if (new TextEncoder().encode(value).length > 2048) return false;
  // eslint-disable-next-line no-control-regex
  if (/[\s\u0000-\u001f\u007f-\u009f\\\uFEFF]/.test(value)) return false;
  const path = value.split(/[?#]/, 1)[0];
  return !path.split("/").some((segment) => {
    const decoded = segment.toLowerCase().replace(/%2e/g, ".");
    return decoded === "." || decoded === "..";
  });
}

function text(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  return trimmed.length > HOST_ATTENTION_LIMITS.textLength
    ? `${trimmed.slice(0, HOST_ATTENTION_LIMITS.textLength - 1)}…`
    : trimmed;
}

/** The valid items of one source's answer, bounded; anything else is dropped. */
export function normalizeHostAttention(source: HostAttentionSourceConfig, body: unknown): HostAttentionItem[] | null {
  if (!body || typeof body !== "object" || !Array.isArray((body as { items?: unknown }).items)) return null;
  const items: HostAttentionItem[] = [];
  const seen = new Set<string>();
  for (const raw of (body as { items: unknown[] }).items) {
    if (items.length >= HOST_ATTENTION_LIMITS.itemsPerSource) break;
    if (!raw || typeof raw !== "object") continue;
    const record = raw as Record<string, unknown>;
    const id = text(record.id);
    const title = text(record.title);
    if (!id || !title || seen.has(id) || !isSameOriginPath(record.href)) continue;
    const details: HostAttentionDetail[] = [];
    if (Array.isArray(record.details)) {
      for (const detail of record.details) {
        if (details.length >= HOST_ATTENTION_LIMITS.detailsPerItem) break;
        if (!detail || typeof detail !== "object") continue;
        const label = text((detail as Record<string, unknown>).label);
        const value = text((detail as Record<string, unknown>).value);
        if (label && value) details.push({ label, value });
      }
    }
    const tone = typeof record.tone === "string" && TONES.has(record.tone) ? record.tone : undefined;
    seen.add(id);
    items.push({ sourceId: source.id, sourceLabel: source.label, id, title, details, href: record.href, tone });
  }
  return items;
}

/** Read every source once. Each source fails alone. */
export async function loadHostAttention(
  sources: readonly HostAttentionSourceConfig[],
  fetchImpl: typeof fetch,
  signal?: AbortSignal,
): Promise<HostAttentionSnapshot> {
  const results = await Promise.all(sources.map(async (source) => {
    if (!isSameOriginPath(source.url)) return { source, items: null };
    try {
      const response = await fetchImpl(source.url, {
        credentials: "same-origin",
        headers: { accept: "application/json" },
        redirect: "error",
        signal,
      });
      if (!response.ok) return { source, items: null };
      return { source, items: normalizeHostAttention(source, await response.json()) };
    } catch {
      return { source, items: null };
    }
  }));
  return {
    status: "ready",
    items: results.flatMap((result) => result.items || []),
    failedSources: results.filter((result) => result.items === null).map((result) => result.source.label),
  };
}
