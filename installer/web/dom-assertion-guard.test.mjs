import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import test from "node:test";
import { domAssertions, equalityAssertions } from "./test-support/dom-assertions.mjs";

const here = new URL(".", import.meta.url);
// Open branches edit these rows; remove this exemption when they land. The
// commit that rewrites the rows removes the entry too: the check below fails
// once an exempted file has no rows left.
const PENDING = new Set(["deployment-port-recovery.test.mjs"]);

test("web tests compare a DOM node as a boolean, never pass the node itself to an equality assertion", () => {
  const findings = [];
  for (const name of readdirSync(here).filter((entry) => entry.endsWith(".test.mjs")).sort()) {
    const source = readFileSync(new URL(name, here), "utf8");
    const rows = domAssertions(source);
    if (PENDING.has(name)) {
      // An entry whose rows are gone must go too.
      if (rows.length === 0) findings.push(`${name}: has no rows that compare a node; remove it from PENDING`);
      continue;
    }
    for (const finding of rows) {
      findings.push(`${name}:${finding.line}: ${finding.text.split("\n")[0]}`);
    }
  }
  for (const name of PENDING) {
    if (!readdirSync(here).includes(name)) findings.push(`${name}: not found; remove it from PENDING`);
  }
  // The message holds strings only: a node in it is what this rule prevents.
  assert.equal(findings.length, 0, "A failing assertion formats its operands. Compare a boolean instead, as in "
    + `assert.equal(node === null, true) or assert.equal(document.activeElement === editor, true):\n${findings.join("\n")}`);
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
  assert.deepEqual(domAssertions(source).map((finding) => finding.line), [5, 6, 7, 8, 9, 10]);
  assert.equal(equalityAssertions(source).length, 10);
});

test("the guard finds DOM nodes compared with undefined or with another node and keeps text compares", () => {
  const source = [
    "const button = (label) => [...document.querySelectorAll('button')].find((node) => node.textContent === label);",
    "function fixture() { const open = document.querySelector('.open'); return { document, button, open }; }",
    "test('focus', () => { const ui = fixture(), editor = document.querySelector('textarea');",
    "  assert.equal(document.activeElement, editor);",
    "  assert.notEqual(document.activeElement, ui.open, 'moved');",
    "  assert.equal(button('Save'), undefined);",
    "  assert.equal(ui.button('Save'), undefined, 'gone');",
    "  assert.equal(ui.open.parentNode, document.body);",
    "  assert.deepEqual(document.querySelectorAll('li'), []);",
    "  assert.equal(document.activeElement === editor, true);",
    "  assert.equal(!!document.querySelector('.x'), false);",
    "  assert.equal(document.querySelector('textarea').value, draft);",
    "  assert.equal(document.querySelectorAll('li').length, 2);",
    "  assert.equal(ui.open.children[0], 'Continue');",
    "});",
    "test('names repeat across tests', () => { const task = document.querySelector('textarea');",
    "  assert.equal(document.activeElement, task); });",
    "test('a text task', () => { const task = 'Plan a trip';",
    "  assert.equal(saved.task, task); });",
    "test('a parameter shadows', () => { const field = document.querySelector('input');",
    "  const edit = (value, target = field) => target.focus();",
    "  const fetch = (target, options) => { assert.equal(target, url(options)); }; });",
  ].join("\n");
  assert.deepEqual(domAssertions(source).map((finding) => finding.line), [4, 5, 6, 7, 8, 9, 17]);
  assert.deepEqual(equalityAssertions(source).map((finding) => finding.method).filter((method) => method !== "equal"),
    ["notEqual", "deepEqual"]);
});

test("the guard reads functions, a node's own values, request bodies and text calls as values", () => {
  const source = [
    "const pick = () => document.querySelector('.x');",
    "const value = document.querySelector('.save');",
    "Object.defineProperty(globalThis, 'saveButton', { configurable: true, value });",
    "const children = document.querySelector('ul').children;",
    "assert.equal(handler, pick);",
    "assert.equal(document.querySelector('input').value, draft);",
    "assert.equal(init.body, payload);",
    "assert.equal(children[0], label.trim());",
    "assert.equal(document.body, current);",
    "assert.equal(children[0], label);",
    "assert.equal(!document.activeElement, expected);",
  ].join("\n");
  assert.deepEqual(domAssertions(source).map((finding) => finding.line), [9, 10]);
});
