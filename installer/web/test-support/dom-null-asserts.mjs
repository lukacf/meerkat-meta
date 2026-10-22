// Find test assertions that compare a DOM node with null.
//
// node:assert formats both operands when an equality assertion fails. For a
// rendered element that formatting walks the attached React tree, and one
// failing row can grow to tens of gigabytes before it reports. Compare a
// boolean instead: assert.equal(element === null, true).

const EQUALITY_CALL = /\bassert\s*\.\s*(equal|strictEqual|deepEqual|deepStrictEqual)\s*\(/g;
const NODE_METHODS = new Set(["querySelector", "getElementById", "closest", "elementFromPoint"]);
const COLLECTION_METHODS = new Set(["querySelectorAll", "getElementsByClassName", "getElementsByTagName",
  "getElementsByName"]);
const NODE_PROPERTIES = new Set(["activeElement", "firstElementChild", "lastElementChild", "parentElement",
  "nextElementSibling", "previousElementSibling", "firstChild", "lastChild", "parentNode", "nextSibling",
  "previousSibling", "offsetParent"]);
const COLLECTION_PROPERTIES = new Set(["children", "childNodes"]);
// Array methods that return one item of their receiver, or a subset of it.
const PICK_METHODS = new Set(["find", "findLast", "at", "item", "pop", "shift"]);
const SUBSET_METHODS = new Set(["filter", "slice", "reverse", "toReversed", "sort", "toSorted"]);
const NODE = "node";
const COLLECTION = "collection";
const IDENTIFIER = String.raw`[A-Za-z_$][\w$]*`;
const WORD = /[\w$]/;
const REGEX_AFTER_WORD = new Set(["return", "typeof", "case", "do", "else", "in", "of", "new", "delete",
  "void", "throw", "instanceof", "yield", "await"]);
const REGEX_AFTER_PUNCTUATION = new Set([..."(,=:[!&|?{};+-*%<>~^"]);
const CONTINUATION = new Set([...".?:|&+-*/%=<>"]);
const MAX_HELPER_DEPTH = 4;

function blank(out, from, to) {
  for (let index = from; index < to; index += 1) {
    if (out[index] !== "\n") out[index] = " ";
  }
}

function skipString(source, start, quote) {
  let index = start + 1;
  while (index < source.length) {
    const character = source[index];
    if (character === "\\") index += 2;
    else if (character === quote) return index + 1;
    else if (character === "\n") return index;
    else index += 1;
  }
  return index;
}

function skipRegex(source, start) {
  let index = start + 1;
  let inClass = false;
  while (index < source.length) {
    const character = source[index];
    if (character === "\\") index += 2;
    else if (character === "\n") return index;
    else if (inClass) {
      if (character === "]") inClass = false;
      index += 1;
    } else if (character === "[") {
      inClass = true;
      index += 1;
    } else if (character === "/") {
      index += 1;
      while (index < source.length && /[a-z]/i.test(source[index])) index += 1;
      return index;
    } else index += 1;
  }
  return index;
}

function skipTemplate(source, out, start) {
  let index = start + 1;
  while (index < source.length) {
    const character = source[index];
    if (character === "\\") index += 2;
    else if (character === "`") return index + 1;
    else if (character === "$" && source[index + 1] === "{") index = scanCode(source, out, index + 2, true) + 1;
    else index += 1;
  }
  return index;
}

// Blank comments and literal contents in place. A template expression scan
// returns the index of its closing brace.
function scanCode(source, out, start, untilBrace) {
  let index = start;
  let depth = 0;
  let previous = "";
  let previousWord = false;
  while (index < source.length) {
    const character = source[index];
    const next = source[index + 1];
    if (character === "/" && next === "/") {
      const end = source.indexOf("\n", index);
      const stop = end < 0 ? source.length : end;
      blank(out, index, stop);
      index = stop;
    } else if (character === "/" && next === "*") {
      const end = source.indexOf("*/", index + 2);
      const stop = end < 0 ? source.length : end + 2;
      blank(out, index, stop);
      index = stop;
    } else if (character === "'" || character === "\"") {
      const stop = skipString(source, index, character);
      blank(out, index + 1, stop - 1);
      index = stop;
      previous = character;
      previousWord = false;
    } else if (character === "`") {
      const stop = skipTemplate(source, out, index);
      blank(out, index + 1, stop - 1);
      index = stop;
      previous = character;
      previousWord = false;
    } else if (character === "/" && (previous === ""
        || (previousWord ? REGEX_AFTER_WORD.has(previous) : REGEX_AFTER_PUNCTUATION.has(previous)))) {
      const stop = skipRegex(source, index);
      blank(out, index + 1, stop - 1);
      index = stop;
      previous = "/";
      previousWord = false;
    } else if (WORD.test(character)) {
      // A number or a name divides; a keyword such as return starts a regex.
      let stop = index;
      while (stop < source.length && WORD.test(source[stop])) stop += 1;
      previous = source.slice(index, stop);
      previousWord = true;
      index = stop;
    } else if (/\s/.test(character)) {
      index += 1;
    } else {
      if (character === "{") depth += 1;
      else if (character === "}") {
        if (untilBrace && depth === 0) return index;
        depth -= 1;
      }
      previous = character;
      previousWord = false;
      index += 1;
    }
  }
  return index;
}

export function maskSource(source) {
  // UTF-16 units, so masked offsets match source.slice offsets.
  const out = source.split("");
  scanCode(source, out, 0, false);
  return out.join("");
}

const PAIRS = { "(": ")", "[": "]", "{": "}" };
const REVERSE = { ")": "(", "]": "[", "}": "{" };

function closing(text, open) {
  const stack = [PAIRS[text[open]]];
  for (let index = open + 1; index < text.length; index += 1) {
    const character = text[index];
    if (PAIRS[character]) stack.push(PAIRS[character]);
    else if (REVERSE[character]) {
      if (stack.pop() !== character) return -1;
      if (stack.length === 0) return index;
    }
  }
  return -1;
}

function opening(text, close) {
  const stack = [REVERSE[text[close]]];
  for (let index = close - 1; index >= 0; index -= 1) {
    const character = text[index];
    if (REVERSE[character]) stack.push(REVERSE[character]);
    else if (PAIRS[character]) {
      if (stack.pop() !== character) return -1;
      if (stack.length === 0) return index;
    }
  }
  return -1;
}

function trimmedSpan(text, start, end) {
  while (start < end && /\s/.test(text[start])) start += 1;
  while (end > start && /\s/.test(text[end - 1])) end -= 1;
  return [start, end];
}

function argumentSpans(text, open, close) {
  const spans = [];
  let depth = 0;
  let start = open + 1;
  for (let index = open + 1; index < close; index += 1) {
    const character = text[index];
    if (PAIRS[character]) depth += 1;
    else if (REVERSE[character]) depth -= 1;
    else if (character === "," && depth === 0) {
      spans.push(trimmedSpan(text, start, index));
      start = index + 1;
    }
  }
  const last = trimmedSpan(text, start, close);
  if (last[1] > last[0]) spans.push(last);
  return spans;
}

// An arrow body or assigned value ends at a top-level ";" or ",", at the
// enclosing bracket, or at a line break that does not continue it.
function expressionEnd(text, start) {
  let depth = 0;
  for (let index = start; index < text.length; index += 1) {
    const character = text[index];
    if (PAIRS[character]) depth += 1;
    else if (REVERSE[character]) {
      if (depth === 0) return index;
      depth -= 1;
    } else if (depth === 0 && (character === ";" || character === ",")) return index;
    else if (depth === 0 && character === "\n") {
      const rest = text.slice(index + 1).match(/^\s*(\S)/);
      if (!rest || !CONTINUATION.has(rest[1])) return index;
    }
  }
  return text.length;
}

function blockAt(text, brace) {
  const end = closing(text, brace);
  return end < 0 ? null : text.slice(brace, end + 1);
}

function bodyAfterArrow(text, arrowEnd) {
  let start = arrowEnd;
  while (start < text.length && /\s/.test(text[start])) start += 1;
  if (text[start] === "{") return blockAt(text, start);
  return text.slice(start, expressionEnd(text, start));
}

function functionBody(text, from) {
  const open = text.indexOf("(", from);
  if (open < 0) return null;
  const close = closing(text, open);
  if (close < 0) return null;
  const brace = text.indexOf("{", close);
  return brace < 0 ? null : blockAt(text, brace);
}

function escaped(name) {
  return name.replace(/\$/g, "\\$");
}

function definitions(masked, name, member) {
  const bodies = [];
  const id = escaped(name);
  for (const match of masked.matchAll(new RegExp(String.raw`(?<![\w$.])function\s*\*?\s*${id}\s*\(`, "g"))) {
    const body = functionBody(masked, match.index);
    if (body) bodies.push(body);
  }
  const arrow = String.raw`(?:async\s+)?(?:\([^()]*\)|${IDENTIFIER})\s*=>`;
  const binding = member ? String.raw`(?<![\w$.])${id}\s*:\s*`
    : String.raw`(?<![\w$.])(?:const|let|var)\s+${id}\s*=\s*`;
  for (const match of masked.matchAll(new RegExp(binding + arrow, "g"))) {
    const body = bodyAfterArrow(masked, match.index + match[0].length);
    if (body) bodies.push(body);
  }
  for (const match of masked.matchAll(new RegExp(binding + String.raw`(?:async\s+)?function\b`, "g"))) {
    const body = functionBody(masked, match.index + match[0].length);
    if (body) bodies.push(body);
  }
  if (member) {
    const shorthand = new RegExp(String.raw`(?<=[{,;\n]\s*)(?:async\s+)?(?:get\s+)?${id}\s*\([^()]*\)\s*\{`, "g");
    for (const match of masked.matchAll(shorthand)) {
      const body = blockAt(masked, match.index + match[0].length - 1);
      if (body) bodies.push(body);
    }
  }
  return bodies;
}

function returnedValues(body) {
  if (!body.startsWith("{")) return [body];
  return [...body.matchAll(/\breturn\b/g)].map((match) => {
    const start = match.index + match[0].length;
    return body.slice(start, expressionEnd(body, start));
  });
}

function assignedValues(masked, name) {
  const id = escaped(name);
  const values = [];
  for (const match of masked.matchAll(new RegExp(String.raw`(?<![\w$.])(?:(?:const|let|var)\s+)?${id}\s*=(?![=>])\s*`, "g"))) {
    const start = match.index + match[0].length;
    values.push({ value: masked.slice(start, expressionEnd(masked, start)), item: false });
  }
  // for (const button of buttons) binds one item of the iterated collection.
  for (const match of masked.matchAll(new RegExp(String.raw`\bfor\s*\(\s*(?:const|let|var)\s+${id}\s+of\s+`, "g"))) {
    const open = masked.lastIndexOf("(", match.index + match[0].length);
    const close = closing(masked, open);
    if (close >= 0) values.push({ value: masked.slice(match.index + match[0].length, close), item: true });
  }
  return values;
}

// Split a value at top-level ||, ?? and && and at the branches of a
// conditional, whose condition is not a result.
function alternatives(text) {
  const parts = [];
  let depth = 0;
  let start = 0;
  let split = false;
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    const next = text[index + 1];
    if (PAIRS[character]) depth += 1;
    else if (REVERSE[character]) depth -= 1;
    else if (depth !== 0) continue;
    else if ((character === "|" && next === "|") || (character === "?" && next === "?")
        || (character === "&" && next === "&")) {
      parts.push(text.slice(start, index));
      start = index + 2;
      index += 1;
      split = true;
    } else if (character === "?" && next !== ".") {
      start = index + 1;
      split = true;
    } else if (character === ":") {
      parts.push(text.slice(start, index));
      start = index + 1;
      split = true;
    }
  }
  if (!split) return null;
  parts.push(text.slice(start));
  return parts;
}

function unwrap(text) {
  let value = text.trim();
  for (;;) {
    const prefix = value.match(/^(?:await|void|typeof)\s+/);
    if (prefix && prefix[0].startsWith("await")) value = value.slice(prefix[0].length).trim();
    else if (value.startsWith("(") && closing(value, 0) === value.length - 1) value = value.slice(1, -1).trim();
    else return value;
  }
}

class Analysis {
  constructor(masked) {
    this.masked = masked;
    this.cache = new Map();
  }

  helper(name, member, depth) {
    const key = (member ? "member:" : "call:") + name;
    if (this.cache.has(key)) return this.cache.get(key);
    if (depth >= MAX_HELPER_DEPTH) return null;
    this.cache.set(key, null);
    let kind = null;
    for (const body of definitions(this.masked, name, member)) {
      for (const value of returnedValues(body)) {
        const found = this.kind(value, depth + 1);
        if (found === NODE) kind = NODE;
        else if (found === COLLECTION && kind === null) kind = COLLECTION;
      }
    }
    this.cache.set(key, kind);
    return kind;
  }

  variable(name, depth) {
    const key = "value:" + name;
    if (this.cache.has(key)) return this.cache.get(key);
    if (depth >= MAX_HELPER_DEPTH) return null;
    this.cache.set(key, null);
    let kind = null;
    for (const { value, item } of assignedValues(this.masked, name)) {
      const found = this.kind(value, depth + 1);
      const result = item ? (found === COLLECTION ? NODE : null) : found;
      if (result === NODE) kind = NODE;
      else if (result === COLLECTION && kind === null) kind = COLLECTION;
    }
    this.cache.set(key, kind);
    return kind;
  }

  // A fixture property: an object literal in this file sets it, as in
  // { open } or { open: doc.querySelector(selector) }.
  property(name, depth) {
    const key = "property:" + name;
    if (this.cache.has(key)) return this.cache.get(key);
    if (depth >= MAX_HELPER_DEPTH) return null;
    this.cache.set(key, null);
    const id = escaped(name);
    let kind = null;
    const merge = (found) => {
      if (found === NODE) kind = NODE;
      else if (found === COLLECTION && kind === null) kind = COLLECTION;
    };
    if (new RegExp(String.raw`(?<=[{,]\s*)${id}(?=\s*[,}])`).test(this.masked)) merge(this.variable(name, depth + 1));
    const value = new RegExp(String.raw`(?<=[{,]\s*)${id}\s*:\s*(?!(?:async\s+)?(?:\([^()]*\)|${IDENTIFIER})\s*=>|(?:async\s+)?function\b)`, "g");
    for (const match of this.masked.matchAll(value)) {
      const start = match.index + match[0].length;
      merge(this.kind(this.masked.slice(start, expressionEnd(this.masked, start)), depth + 1));
    }
    this.cache.set(key, kind);
    return kind;
  }

  // NODE when a value can be a DOM node, COLLECTION when it is a list of them.
  kind(text, depth = 0) {
    const value = unwrap(text);
    if (!value) return null;
    const parts = alternatives(value);
    if (parts) {
      const kinds = parts.map((part) => this.kind(part, depth));
      return kinds.includes(NODE) ? NODE : kinds.includes(COLLECTION) ? COLLECTION : null;
    }
    if (value.endsWith("]")) {
      const open = opening(value, value.length - 1);
      if (open === 0) {
        const spread = value.slice(1, -1).trim().match(/^\.\.\.([^]*)$/);
        return spread && this.kind(spread[1], depth) === COLLECTION ? COLLECTION : null;
      }
      return open > 0 && this.kind(value.slice(0, open), depth) === COLLECTION ? NODE : null;
    }
    if (value.endsWith(")")) {
      const open = opening(value, value.length - 1);
      if (open <= 0) return null;
      const callee = value.slice(0, open).replace(/\?\.\s*$/, "").trimEnd();
      const member = callee.match(new RegExp(String.raw`(?:\?\.|\.)\s*(${IDENTIFIER})$`));
      if (member) {
        const name = member[1];
        const receiver = callee.slice(0, member.index);
        if (NODE_METHODS.has(name)) return NODE;
        if (COLLECTION_METHODS.has(name)) return COLLECTION;
        if (name === "from" && receiver.trim() === "Array") {
          const first = argumentSpans(value, open, value.length - 1)[0];
          return first && this.kind(value.slice(...first), depth) === COLLECTION ? COLLECTION : null;
        }
        if (PICK_METHODS.has(name)) return this.kind(receiver, depth) === COLLECTION ? NODE : null;
        if (SUBSET_METHODS.has(name)) return this.kind(receiver, depth) === COLLECTION ? COLLECTION : null;
        return this.helper(name, true, depth);
      }
      const bare = callee.match(new RegExp(String.raw`^(${IDENTIFIER})$`));
      return bare ? this.helper(bare[1], false, depth) : null;
    }
    const property = value.match(new RegExp(String.raw`(?:\?\.|\.)\s*(${IDENTIFIER})$`));
    if (property) {
      if (NODE_PROPERTIES.has(property[1])) return NODE;
      if (COLLECTION_PROPERTIES.has(property[1])) return COLLECTION;
      return this.property(property[1], depth);
    }
    const identifier = value.match(new RegExp(String.raw`^(${IDENTIFIER})$`));
    return identifier ? this.variable(identifier[1], depth) : null;
  }
}

// Every equality assertion in this source whose expected value is null. A
// row is dom when its actual value can be a DOM node: read directly, through
// a same-file helper or through a same-file variable.
export function nullAssertions(source) {
  const masked = maskSource(source);
  const analysis = new Analysis(masked);
  const findings = [];
  for (const match of masked.matchAll(EQUALITY_CALL)) {
    const open = match.index + match[0].length - 1;
    const close = closing(masked, open);
    if (close < 0) continue;
    const spans = argumentSpans(masked, open, close);
    if (spans.length < 2 || masked.slice(...spans[1]) !== "null") continue;
    findings.push({
      line: source.slice(0, match.index).split("\n").length,
      method: match[1],
      actual: spans[0],
      expected: spans[1],
      dom: analysis.kind(masked.slice(...spans[0])) === NODE,
      text: source.slice(match.index, close + 1),
    });
  }
  return findings;
}

export function domNullAssertions(source) {
  return nullAssertions(source).filter((finding) => finding.dom);
}
