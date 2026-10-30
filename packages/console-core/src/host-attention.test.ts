import assert from "node:assert/strict";
import test from "node:test";
import { HOST_ATTENTION_LIMITS, isSameOriginPath, loadHostAttention, normalizeHostAttention } from "./host-attention";

const source = { id: "host", label: "Host", url: "/host/attention" };
const item = (id: string, extra: Record<string, unknown> = {}) => ({
  id, title: `Confirm ${id}`, href: `/host/items/${id}`, details: [{ label: "For", value: "plugin:x" }], ...extra,
});
function respond(map: Record<string, () => Response | Promise<Response>>): typeof fetch {
  return (async (url: string | URL | Request, init?: RequestInit) => {
    assert.equal(init?.credentials, "same-origin");
    assert.equal(init?.redirect, "error");
    const handler = map[String(url)];
    if (!handler) throw new Error(`unexpected ${String(url)}`);
    return handler();
  }) as typeof fetch;
}
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

test("same-origin paths follow the operator panel path rules", () => {
  for (const ok of ["/host/attention", "/a/b?next=/../", "/a.b/..c"]) assert.equal(isSameOriginPath(ok), true, ok);
  for (const bad of ["https://evil.example/x", "//evil.example/x", "x/y", "/a b", "/a\\b", "/a/../b", "/%2e%2E/x", "/\uFEFFx", "/a\u0085b", 7, null]) {
    assert.equal(isSameOriginPath(bad), false, String(bad));
  }
});

test("items are validated, bounded and keep their source", () => {
  const items = normalizeHostAttention(source, {
    items: [
      item("a", { tone: "attention" }),
      item("b", { href: "https://evil.example/b" }),
      item("c", { href: "javascript:alert(1)" }),
      { id: "d", href: "/host/d" },
      item("a"),
      item("e", { tone: "loud", details: [{ label: "x" }, { label: "y", value: "z".repeat(400) }, "nope"] }),
    ],
  });
  assert.deepEqual(items?.map((entry) => entry.id), ["a", "e"]);
  assert.equal(items?.[0].sourceLabel, "Host");
  assert.equal(items?.[0].tone, "attention");
  assert.equal(items?.[1].tone, undefined);
  assert.equal(items?.[1].details.length, 1);
  assert.equal(items?.[1].details[0].value.length, HOST_ATTENTION_LIMITS.textLength);
  const many = normalizeHostAttention(source, { items: Array.from({ length: 80 }, (_, i) => item(`i${i}`)) });
  assert.equal(many?.length, HOST_ATTENTION_LIMITS.itemsPerSource);
  assert.equal(normalizeHostAttention(source, { nope: [] }), null);
  assert.equal(normalizeHostAttention(source, "items"), null);
});

test("a failing source is reported alone and never hides another's items", async () => {
  const other = { id: "other", label: "Other", url: "/other/attention" };
  const broken = { id: "broken", label: "Broken", url: "/broken/attention" };
  const down = { id: "down", label: "Down", url: "/down/attention" };
  const snapshot = await loadHostAttention([source, broken, other, down], respond({
    "/host/attention": () => json({ items: [item("a")] }),
    "/broken/attention": () => json({ items: "no" }),
    "/other/attention": () => json({ items: [item("b")] }),
    "/down/attention": () => json({ error: "forbidden" }, 403),
  }));
  assert.equal(snapshot.status, "ready");
  assert.deepEqual(snapshot.items.map((entry) => `${entry.sourceId}:${entry.id}`), ["host:a", "other:b"]);
  assert.deepEqual(snapshot.failedSources, ["Broken", "Down"]);
});

test("a network error or a non-path url fails only that source", async () => {
  const snapshot = await loadHostAttention(
    [source, { id: "abs", label: "Absolute", url: "https://evil.example/a" }],
    respond({ "/host/attention": () => { throw new TypeError("network"); } }),
  );
  assert.deepEqual(snapshot.items, []);
  assert.deepEqual(snapshot.failedSources, ["Host", "Absolute"]);
});
