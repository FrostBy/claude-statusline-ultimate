'use strict';
const fs = require('node:fs/promises');
const { createHash } = require('node:crypto');
const MARK = '[session-sync:v2:';
// Claude persists large hook output to a file; Codex: ceil(UTF-8 bytes / 4) <= 16,000 tokens.
const LIMITS = { claude: Infinity, codex: 64000 };
const hash = text => createHash('sha256').update(text).digest('hex');
// Claude shows a 2,000-character preview of saved hook output and may answer without opening the file,
// so the newest messages lead the block.
const LATEST = 1400;
const entry = e => `(${e.origin || 'sync'}/${e.role})\n${e.text}`;
function latest(entries) {
  const parts = [];
  for (let i = entries.length - 1, size = 0; i >= 0 && size < LATEST; i--) {
    let text = entry(entries[i]);
    if (size + text.length > LATEST) text = '…' + text.slice(-(LATEST - size)).replace(/^[\uDC00-\uDFFF]/, '');
    parts.unshift(text); size += text.length + 2;
  }
  return parts.length ? `Latest paired-session messages (newest last; the full context follows):\n${parts.join('\n\n')}\n\n` : '';
}
function chunk(text, position, available) {
  const bytes = Buffer.from(text);
  let end = Math.min(bytes.length, position + available);
  while (end < bytes.length && (bytes[end] & 0xc0) === 0x80) end--;
  return { text: bytes.subarray(position, end).toString('utf8'), next: end, complete: end === bytes.length };
}
async function event(file, ref) {
  const handle = await fs.open(file, 'r');
  try {
    const bytes = Buffer.alloc(ref.length);
    if ((await handle.read(bytes, 0, bytes.length, ref.offset)).bytesRead !== bytes.length) throw Error('Incomplete event archive');
    const value = JSON.parse(bytes.toString('utf8'));
    if (value.key !== ref.key) throw Error('Event archive mismatch');
    return value;
  } finally { await handle.close(); }
}
function acknowledge(endpoint, marks) {
  for (const item of endpoint.inflight || []) {
    if (!marks.has(item.key)) continue;
    if (item.snapshot) {
      if (endpoint.snapshot?.key !== item.snapshot) continue;
      endpoint.snapshot.position = item.next;
      if (item.complete) { endpoint.compactAcknowledged = endpoint.snapshot.compactKey; endpoint.snapshot = null; }
    } else {
      const ref = endpoint.pending.find(ref => ref.key === item.ref);
      if (!ref) continue;
      if (item.complete) endpoint.pending = endpoint.pending.filter(ref => ref.key !== item.ref);
      else ref.position = item.next;
    }
  }
  endpoint.inflight = (endpoint.inflight || []).filter(item => !marks.has(item.key));
}
async function output(kind, endpoint, journal, budget, context) {
  const limit = Math.min(LIMITS[kind], budget), blocks = [], inflight = [];
  const claudeKey = kind === 'claude' ? hash(JSON.stringify([endpoint.snapshot || endpoint.pending, limit])) : null;
  let used = 0;
  async function block(text, identity, position, descriptor) {
    const key = claudeKey || hash(`${identity}:${position}:${limit}`);
    const prefix = claudeKey && blocks.length ? '' : `${MARK}${key}]\n` + (claudeKey
      ? 'Full paired-session context follows; if this is a preview, read the saved file path given by Claude Code before answering\n' : '');
    const available = limit - used - Buffer.byteLength(prefix) - (blocks.length ? 2 : 0);
    if (available < 128) return false;
    const part = chunk(text, position, available);
    blocks.push(prefix + part.text); used += Buffer.byteLength(prefix + part.text) + (blocks.length > 1 ? 2 : 0);
    inflight.push({ key, ...descriptor, next: part.next, complete: part.complete });
    return part.complete;
  }
  if (endpoint.snapshot) {
    const snapshot = endpoint.snapshot;
    const entries = await context.materialize(journal, snapshot, snapshot.budget || budget);
    const text = (kind === 'claude' ? latest(entries) : '') + 'Current active context from the paired session (replaces the pending backlog; continued across hooks).\n' + (kind === 'claude'
      ? entries.map(entry).join('\n\n') : context.render(entries));
    // Claude delivery is a single block; saved positions come from the retired 10k-chunk renderer, so restarting from one would begin mid-record.
    await block(text, snapshot.key, kind === 'claude' ? 0 : snapshot.position || 0, { snapshot: snapshot.key });
  } else {
    const values = kind === 'claude' ? await Promise.all(endpoint.pending.map(ref => event(journal, ref))) : null;
    let lead = values ? latest(values) : '';
    for (const [index, ref] of endpoint.pending.entries()) {
      const value = values ? values[index] : await event(journal, ref);
      const text = lead + `Historical observation (${value.origin}/${value.role}); completed tools are not live calls.\n${value.text}`;
      lead = '';
      if (!await block(text, ref.key, kind === 'claude' ? 0 : ref.position || 0, { ref: ref.key })) break;
    }
  }
  endpoint.inflight = inflight;
  return blocks.length ? blocks.join('\n\n') : null;
}
module.exports = { LIMITS, acknowledge, output, event };
