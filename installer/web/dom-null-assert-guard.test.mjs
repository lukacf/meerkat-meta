import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import test from "node:test";
import { domNullAssertions, nullAssertions } from "./test-support/dom-null-asserts.mjs";

const here = new URL(".", import.meta.url);

test("web tests compare a DOM node with null as a boolean, never the node itself", () => {
  const findings = [];
  for (const name of readdirSync(here).filter((entry) => entry.endsWith(".test.mjs")).sort()) {
    const source = readFileSync(new URL(name, here), "utf8");
    for (const finding of domNullAssertions(source)) {
      findings.push(`${name}:${finding.line}: ${finding.text.split("\n")[0]}`);
    }
  }
  // The message holds strings only: a node in it is what this rule prevents.
  assert.equal(findings.length, 0,
    `A failing assertion formats its operands. Write assert.equal(node === null, true):\n${findings.join("\n")}`);
});

test("the guard finds DOM nodes compared with null and leaves other values alone", () => {
  const source = [
    "const panel = () => document.querySelector('.panel');",
    "const rows = document.querySelectorAll('tr');",
    "function link(view) { return [...view.querySelectorAll('a')].find((item) => item.href) || null; }",
    "const f = { open: doc.querySelector('.open'), count: 2 };",
    "assert.equal(document.querySelector('.x'), null);",
    "assert.strictEqual(panel(), null, 'gone');",
    "assert.equal(rows[0], null);",
    "assert.equal(link(document), null);",
    "assert.equal(f.open, null);",
    "assert.equal(button.closest('form'), null);",
    "assert.equal(document.querySelector('.x') === null, true);",
    "assert.equal(document.querySelector('.x')?.textContent ?? null, null);",
    "assert.equal(storage.getItem('key'), null);",
    "assert.equal(f.count, null);",
    "// assert.equal(document.querySelector('.comment'), null);",
    "const text = \"assert.equal(document.querySelector('.string'), null)\";",
  ].join("\n");
  assert.deepEqual(domNullAssertions(source).map((finding) => finding.line), [5, 6, 7, 8, 9, 10]);
  assert.equal(nullAssertions(source).length, 9);
});
