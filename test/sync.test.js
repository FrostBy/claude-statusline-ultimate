'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const { randomUUID } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const { configuration, hook, eligible, extract, incremental, withConfig, write, hookConfigs } = require('../src/sync');
const { configure } = require('../src/sync/install');
const { slug, activated, projectPaths } = require('../src/sync/activation');
const opts = { env: {}, interactive: async () => true };
const jsonl = records => records.map(JSON.stringify).join('\n') + '\n';
const c = (id, text, extra = {}) => ({ type: 'user', uuid: randomUUID(), sessionId: id, message: { role: 'user', content: text }, ...extra });
const x = text => ({ type: 'response_item', payload: { id: randomUUID(), type: 'message', role: 'user', content: [{ type: 'input_text', text }] } });
async function fixture(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'agent-kit-sync-'));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const env = { HOME: root, USERPROFILE: root, CLAUDE_CONFIG_DIR: path.join(root, 'claude'), CODEX_HOME: path.join(root, 'codex') };
  const config = configuration(path.join(root, 'project'), env);
  config.versions = { claude: 'test-claude', codex: 'test-codex' };
  config.apiRequests = []; config.apiConnections = []; const threads = new Map(); config.apiThreads = threads;
  const response = thread => ({ thread: config.apiPrefix ? { ...thread, path: config.apiPrefix + thread.path } : thread });
  config.connectCodex = options => (config.apiConnections.push(options), { ready: Promise.resolve(), notify() {}, close() {}, async request(method, params, limit) {
    config.apiRequests.push({ method, params, limit });
    if (config.apiFail === method) throw Error('fake app-server unavailable');
    if (method === 'initialize') return {};
    if (method === 'thread/start') {
      const id = randomUUID(), file = path.join(config.codexSessionsDir, `rollout-${id}.jsonl`);
      const thread = { id, path: file, cwd: params.cwd, source: 'vscode', preview: '', loaded: true, status: { type: 'idle' } };
      threads.set(id, thread); await fs.mkdir(path.dirname(file), { recursive: true });
      await fs.writeFile(file, jsonl([{ type: 'session_meta', payload: { id, cwd: params.cwd, source: 'vscode', originator: 'agent-kit-session-sync', cli_version: config.versions?.codex || '9.8.7' } }]));
      return response(thread);
    }
    if (method === 'thread/loaded/list') return { data: [...threads.values()].filter(t => t.loaded).map(t => t.id) };
    const thread = threads.get(params.threadId); if (!thread) throw Error('unknown fake thread');
    if (method === 'thread/delete') {
      if (config.noDelete) throw { code: -32601, message: 'Method not found' };
      await fs.unlink(thread.path); threads.delete(thread.id); return {};
    }
    if (method === 'thread/inject_items') {
      if (config.injectDelay) await new Promise(resolve => setTimeout(resolve, config.injectDelay));
      await fs.appendFile(thread.path, jsonl(params.items.map(payload => ({ type: 'response_item', payload }))));
      if (config.dropInjectionReply) { config.dropInjectionReply = false; throw Error('reply lost after injection'); }
    }
    if (method === 'thread/name/set') thread.name = params.name;
    if (method === 'thread/goal/get') return { goal: thread.goal || null };
    if (method === 'thread/goal/set') {
      assert.equal(params.status, 'paused'); assert.equal(params.origin, undefined);
      thread.goal = { objective: params.objective }; thread.preview = params.objective;
      await fs.appendFile(thread.path, jsonl([{ type: 'event_msg', payload: { type: 'thread_goal_updated', goal: { objective: params.objective } } }]));
    }
    if (method === 'thread/goal/clear') delete thread.goal;
    if (method === 'thread/archive') {
      const folder = path.join(config.codexHome, 'archived_sessions'); await fs.mkdir(folder, { recursive: true });
      const file = path.join(folder, path.basename(thread.path)); await fs.rename(thread.path, file);
      thread.path = file; thread.archived = true; thread.loaded = false;
    }
    if (method === 'thread/unarchive') {
      const file = path.join(config.codexSessionsDir, path.basename(thread.path)); await fs.rename(thread.path, file);
      thread.path = file; thread.archived = false;
    }
    if (method === 'thread/resume') thread.loaded = true;
    if (method === 'thread/read' || method === 'thread/resume' || method === 'thread/unarchive') return response(thread);
    return {};
  } });
  await fs.mkdir(path.join(config.claudeProjectsDir, slug(config.cwd)), { recursive: true });
  await fs.mkdir(config.codexHome, { recursive: true });
  await fs.writeFile(path.join(config.codexHome, 'config.toml'), `[projects.${JSON.stringify(config.cwd)}]\ntrust_level = "trusted"\n`);
  const id = randomUUID(), file = path.join(config.claudeProjectsDir, 'project', `${id}.jsonl`);
  await fs.mkdir(path.dirname(file), { recursive: true });
  const input = (kind = 'claude', endpoint = { id, file }, event = 'Stop') => ({ cwd: config.cwd, session_id: endpoint.id, transcript_path: endpoint.file, hook_event_name: event });
  const registry = async () => JSON.parse(await fs.readFile(path.join(config.stateDir, 'pairs.json')));
  const pair = async (index = 0) => JSON.parse(await fs.readFile(path.join(config.stateDir, `${(await registry()).pairs[index].id}.json`)));
  const append = (file, records) => fs.appendFile(file, jsonl(records));
  const archive = async () => (await fs.readFile(path.join(config.stateDir, `${(await pair()).id}.events`), 'utf8')).trim().split('\n').map(JSON.parse);
  return { root, env, config, id, file, input, registry, pair, append, archive };
}

test('bidirectional sync: backfill, tools across hooks, retry/ack, compaction and no mirror loops', async t => {
  const f = await fixture(t), { config, id, file, input, pair, append } = f;
  await fs.writeFile(file, jsonl([
    c(id, 'OLD-HANDOFF'), { type: 'system', subtype: 'compact_boundary', sessionId: id },
    c(id, 'COMPACT-SUMMARY', { isCompactSummary: true }), c(id, 'NEW-TURN'),
    c(id, '', { message: { role: 'assistant', content: [{ type: 'thinking', thinking: 'PRIVATE-THINKING' }, { type: 'tool_use', id: 't1', name: 'Read', input: { file: 'example' } }] } })
  ]));
  await hook('claude', input(), config, opts);
  let p = await pair(), seed = await fs.readFile(p.codex.file, 'utf8');
  assert.doesNotMatch((await fs.readFile(p.codex.file, 'utf8')).split('\n').filter(Boolean).map(JSON.parse).filter(r => r.type === 'response_item').map(r => JSON.stringify(r.payload)).join(''), /OLD-HANDOFF/); assert.match(seed, /COMPACT-SUMMARY/); assert.match(seed, /NEW-TURN/); assert.doesNotMatch(seed, /PRIVATE/);
  await append(file, [c(id, [{ type: 'tool_result', tool_use_id: 't1', content: 'COMPLETED-TOOL' }])]);
  await hook('claude', input(), config, opts);
  assert((await f.archive()).some(e => e.text.includes('COMPLETED-TOOL')));
  const count = (await pair()).known.length;
  await hook('claude', input(), config, opts);
  assert.equal((await pair()).known.length, count); assert.equal(await fs.readFile(p.codex.file, 'utf8'), seed);
  await append(p.codex.file, [x('RETURN-HANDOFF')]);
  await hook('codex', input('codex', p.codex), config, opts);
  assert.equal((await f.registry()).pairs.length, 1);
  const output = await hook('claude', input('claude', p.claude, 'UserPromptSubmit'), config, opts);
  assert.match(output.hookSpecificOutput.additionalContext, /RETURN-HANDOFF/);
  assert.deepEqual(await hook('claude', input('claude', p.claude, 'UserPromptSubmit'), config, opts), output);
  await append(file, [c(id, output.hookSpecificOutput.additionalContext, { isMeta: true })]);
  assert.equal(await hook('claude', input('claude', p.claude, 'UserPromptSubmit'), config, opts), null);
  await append(file, [c(id, 'NEXT-DELTA')]); await hook('claude', input(), config, opts);
  const reply = await hook('codex', input('codex', p.codex, 'UserPromptSubmit'), config, opts);
  assert.match(reply.hookSpecificOutput.additionalContext, /NEXT-DELTA/); assert.doesNotMatch(reply.hookSpecificOutput.additionalContext, /OLD-HANDOFF|RETURN-HANDOFF/);
  await append(p.codex.file, [{ type: 'response_item', payload: { type: 'message', role: 'developer', content: [{ type: 'input_text', text: reply.hookSpecificOutput.additionalContext }] } }]);
  assert.equal(await hook('codex', input('codex', p.codex, 'UserPromptSubmit'), config, opts), null);
  // Independent native Codex session creates its own Claude mirror.
  const codexId = randomUUID(), codexFile = path.join(config.codexSessionsDir, `${codexId}.jsonl`);
  await fs.writeFile(codexFile, jsonl([{ type: 'session_meta', payload: { id: codexId, cwd: config.cwd, source: 'cli', originator: 'codex-tui' } }, x('REVERSE-BACKFILL'), { type: 'response_item', payload: { type: 'compaction', encrypted_content: 'SECRET' } }]));
  await hook('codex', input('codex', { id: codexId, file: codexFile }), config, opts);
  p = await pair(1); seed = await fs.readFile(p.claude.file, 'utf8');
  assert.match(seed, /REVERSE-BACKFILL/); assert.match(seed, /custom-title/); assert.doesNotMatch(seed, /SECRET/);
  await hook('claude', input('claude', p.claude, 'SessionStart'), config, opts);
  await append(p.claude.file, [c(p.claude.id, 'REVERSE-RETURN')]);
  const reverse = await hook('codex', input('codex', p.codex, 'UserPromptSubmit'), config, opts);
  assert.match(reverse.hookSpecificOutput.additionalContext, /REVERSE-RETURN/);
  assert.equal((await f.registry()).pairs.length, 2);
});

test('incremental byte reader: partial UTF-8, truncation, replacement and same-size rewrite', async t => {
  const { file } = await fixture(t);
  await fs.writeFile(file, '{"text":"one"}\n');
  let r = await incremental(file); assert.equal(r.records.length, 1);
  assert.equal((await incremental(file, r.cursor)).bytesRead, 0);
  const bytes = Buffer.from('{"text":"привет"}\n');
  await fs.appendFile(file, bytes.subarray(0, 12));
  const partial = await incremental(file, r.cursor); assert.equal(partial.records.length, 0);
  await fs.appendFile(file, bytes.subarray(12));
  r = await incremental(file, partial.cursor); assert.equal(r.records[0].text, 'привет');
  await fs.writeFile(file, '{}\n'); r = await incremental(file, r.cursor); assert.equal(r.reset, true);
  const replacement = file + '.replacement'; await fs.writeFile(replacement, '{"text":"replacement"}\n'); await fs.rename(replacement, file);
  r = await incremental(file, r.cursor); assert.equal(r.reset, true); assert.equal(r.records[0].text, 'replacement');
  await fs.writeFile(file, '{"text":"REPLACEMENT"}\n'); r = await incremental(file, r.cursor); assert.equal(r.reset, true);
  await fs.writeFile(file, '{bad}\n'); await assert.rejects(incremental(file), SyntaxError);
  await fs.writeFile(file, Buffer.from([0xff, 10])); await assert.rejects(incremental(file), /encoded data/);
});

test('eligibility caches process checks, rechecks resumed modes, and excludes brokers/sidechains/exec', async t => {
  const f = await fixture(t); let probes = 0;
  await fs.writeFile(f.file, jsonl([c(f.id, 'hello')]));
  const options = { env: {}, interactive: async () => { probes++; return true; } };
  await hook('claude', f.input(), f.config, options); await hook('claude', f.input(), f.config, options);
  assert.equal(probes, 1);
  for (const env of [{ SESSION_SYNC_BROKER: '1' }, { SESSION_SYNC_EXCLUDE: '1' }]) assert.equal(await hook('claude', f.input(), f.config, { ...options, env }), null);
  const before = (await f.pair()).known.length;
  await hook('claude', f.input('claude', undefined, 'SessionStart'), f.config, { env: {}, cliIdentity: 'other-cli', interactive: async () => false });
  await f.append(f.file, [c(f.id, 'PRINT-ONLY')]);
  assert.equal(await hook('claude', f.input(), f.config, options), null);
  assert.equal((await f.pair()).known.length, before); assert.equal(probes, 1);
  for (const source of ['exec', 'appServer', { subagent: {} }]) assert.equal(await eligible('codex', f.input(), [{ type: 'session_meta', payload: { source, originator: 'codex-tui' } }], {}, options.interactive), false);
  assert.equal(await eligible('claude', { ...f.input(), agent_id: 'child' }, [], {}, options.interactive), false);
  assert.equal(await eligible('claude', { ...f.input(), transcript_path: '/subagents/agent-123.jsonl' }, [], {}, options.interactive), false);
  assert.equal(extract('claude', [c(f.id, 'sidechain', { isSidechain: true })], f.id).length, 0);
  assert.throws(() => extract('codex', [{ type: 'event_msg', payload: { type: 'thread_rolled_back' } }], 'id'), /rollback/);
});

test('confined writes, orphan mirrors, mismatched IDs, archive recovery and lock contention fail closed', async t => {
  const f = await fixture(t);
  await assert.rejects(write(f.file, 'no'), /explicit/);
  await withConfig(f.config, async () => {
    await assert.rejects(write(path.join(f.root, 'outside.json'), '{}'), /outside configured/);
    await assert.rejects(write(f.file, 'overwrite'), /rewrite an existing session/);
  });
  await assert.rejects(withConfig({ ...f.config, stateDir: 'relative' }, () => {}), /absolute/);
  await fs.writeFile(f.file, jsonl([c('wrong-id', 'no')])); await assert.rejects(hook('claude', f.input(), f.config, opts), /ID mismatch/);
  await fs.writeFile(f.file, jsonl([{ type: 'session-sync-mirror', sessionId: f.id }])); await assert.rejects(hook('claude', f.input(), f.config, opts), /Orphan mirror/);
  await fs.writeFile(f.file, jsonl([c(f.id, 'valid')])); await hook('claude', f.input(), f.config, opts);
  let p = await f.pair(); const journal = path.join(f.config.stateDir, `${p.id}.events`);
  await fs.appendFile(journal, 'uncommitted partial tail'); await hook('claude', f.input(), f.config, opts);
  assert.equal((await fs.stat(journal)).size, p.archiveSize);
  await fs.writeFile(path.join(f.config.stateDir, 'lock'), '');
  assert.equal(await hook('claude', f.input(), f.config, opts), null); await fs.unlink(path.join(f.config.stateDir, 'lock'));
  await fs.truncate(journal, 0); await assert.rejects(hook('claude', f.input(), f.config, opts), /archive truncated/);
  await assert.rejects(hook('claude', { ...f.input(), transcript_path: path.join(f.root, 'outside.jsonl') }, f.config, opts), /outside configured/);
});

test('installer uses fake homes, keeps other hooks/config, is idempotent and removes only owned hooks', async t => {
  const f = await fixture(t), log = () => {};
  const settings = path.join(f.env.CLAUDE_CONFIG_DIR, 'settings.json'), toml = path.join(f.env.CODEX_HOME, 'config.toml');
  await fs.mkdir(f.env.CODEX_HOME, { recursive: true });
  await fs.writeFile(settings, JSON.stringify({ hooks: { Stop: [{ hooks: [{ type: 'command', command: 'keep' }] }] }, other: true }));
  await fs.writeFile(toml, 'model = "keep"\n[features]\nfast = true\n');
  assert.equal(configure({ env: f.env, log }), 2);
  assert.equal(configure({ env: f.env, log }), 0);
  assert.deepEqual(Object.keys(hookConfigs('script').claude.hooks), ['SessionStart', 'UserPromptSubmit', 'Stop']);
  let parsed = JSON.parse(await fs.readFile(settings)); assert.equal(parsed.hooks.Stop.length, 2);
  await fs.appendFile(toml, '# unrelated later edit\n');
  assert.equal(configure({ env: f.env, remove: true, log }), 2);
  assert.equal(configure({ env: f.env, remove: true, log }), 0);
  parsed = JSON.parse(await fs.readFile(settings)); assert.equal(parsed.hooks.Stop[0].hooks[0].command, 'keep'); assert.equal(parsed.other, true);
  assert.match(await fs.readFile(toml, 'utf8'), /unrelated later edit/); assert.doesNotMatch(await fs.readFile(toml, 'utf8'), /SESSION-SYNC/);
  assert((await fs.readdir(f.env.CODEX_HOME)).some(n => n.includes('backup')));
  await fs.writeFile(toml, '# BEGIN AGENT-KIT SESSION-SYNC\n');
  const before = await fs.readFile(settings, 'utf8'); assert.throws(() => configure({ env: f.env, log }), /Ambiguous/); assert.equal(await fs.readFile(settings, 'utf8'), before);
});

test('50 MiB / 4200 events: cached hook stays under one second and does not reread/rewrite the archive', async t => {
  const f = await fixture(t), rows = [];
  for (let i = 0; i < 4200; i++) rows.push(c(f.id, '', { uuid: `event-${i}`, message: { role: 'assistant', content: [{ type: 'text', text: `turn ${i}` }, { type: 'thinking', thinking: 'x'.repeat(12500) }] } }));
  await fs.writeFile(f.file, jsonl(rows));
  const cold = performance.now(); await hook('claude', f.input(), f.config, opts); const coldMs = performance.now() - cold;
  const p = await f.pair(); assert.equal(p.known.length, 256); assert.equal((await f.archive()).length, 4200);
  const journal = path.join(f.config.stateDir, `${p.id}.events`), before = await fs.stat(journal);
  const start = performance.now(); await hook('claude', f.input(), f.config, opts); const warmMs = performance.now() - start;
  assert(warmMs < 1000, `warm hook ${warmMs.toFixed(1)} ms`);
  assert.equal((await fs.stat(journal)).mtimeMs, before.mtimeMs);
  assert.equal((await incremental(f.file, (await f.pair()).claude.cursor)).bytesRead, 0);
  t.diagnostic(`50 MiB / 4200 events: cold ${coldMs.toFixed(1)} ms, warm ${warmMs.toFixed(1)} ms`);
});

test('process ancestry uses CIM on Windows and ps on Unix; unknown/print/exec modes fail closed', async () => {
  const { processInteractive } = require('../src/sync');
  for (const platform of ['win32', 'linux', 'darwin']) {
    for (const [kind, name, command, expected] of [
      ['claude', 'claude.exe', 'claude.exe', true], ['claude', 'claude.exe', 'claude.exe --print hello', false],
      ['claude', 'claude.exe', 'claude.exe -p=hello', false], ['codex', 'codex.exe', 'codex.exe exec resume id', false],
      ['codex', 'codex.exe', 'codex.exe review', false], ['codex', 'codex.exe', 'codex.exe app-server', false],
      ['codex', 'codex.exe', 'codex.exe', true], ['codex', 'node', 'node /opt/codex.js exec task', false],
      ['codex', 'node', 'node /opt/codex.js', true], ['codex', 'node', 'node /app/sync.js hook codex', false]
    ]) {
      const run = async (exe, args) => {
        if (platform === 'win32') { assert.equal(exe, 'powershell.exe'); assert.match(args.at(-1), /Get-CimInstance/); return { stdout: JSON.stringify([{ name, command }]) }; }
        assert.equal(exe, 'ps'); return { stdout: `123 1 ${name} ${command}\n1 0 init init\n` };
      };
      assert.equal(await processInteractive(kind, { platform, pid: 123, run }), expected, `${platform}: ${command}`);
    }
  }
});

test('installer handles multiline TOML safely, and CLI installation/removal stays in fake homes', async t => {
  const f = await fixture(t), { tomlBlock } = require('../src/sync/install');
  const before = 'prompt = """line one\nline two"""\n[features]\nfast = true\n';
  const installed = tomlBlock(before, '# BEGIN AGENT-KIT SESSION-SYNC\n# owned\n# END AGENT-KIT SESSION-SYNC\n');
  assert.equal(tomlBlock(installed, ''), before);
  assert.throws(() => tomlBlock('prompt = """\n# BEGIN AGENT-KIT SESSION-SYNC\n# END AGENT-KIT SESSION-SYNC\n"""', ''), /inside TOML/);
  const { spawnSync } = require('node:child_process');
  const script = path.resolve(__dirname, '../scripts/agents/sync.js');
  for (const command of ['install', 'install', 'uninstall', 'uninstall']) {
    const result = spawnSync(process.execPath, [script, command], { env: { ...process.env, ...f.env }, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);
  }
  assert.doesNotMatch(await fs.readFile(path.join(f.env.CODEX_HOME, 'config.toml'), 'utf8'), /SESSION-SYNC/);
});

test('activation requires both registrations, caches TOML mtime, and skips before transcript/process reads', async t => {
  const f = await fixture(t), toml = path.join(f.config.codexHome, 'config.toml');
  const dir = path.join(f.config.claudeProjectsDir, slug(f.config.cwd));
  const never = { env: {}, interactive: () => { throw Error('process probe must not run'); } };
  await fs.rm(toml);
  const start = performance.now();
  assert.equal(await hook('claude', f.input(), f.config, never), null);
  assert(performance.now() - start < 100);
  const literal = `[projects.'${f.config.cwd}']\ntrust_level = 'trusted'\n`;
  await fs.writeFile(toml, literal);
  assert.equal(await activated(f.config), true);
  const cache = path.join(f.config.stateDir, 'activation.json'), before = await fs.stat(cache);
  assert.equal(await activated(f.config), true); assert.equal((await fs.stat(cache)).mtimeMs, before.mtimeMs);
  await fs.rmdir(dir); assert.equal(await hook('claude', f.input(), f.config, never), null);
  await fs.mkdir(dir); assert.equal(await activated(f.config), true);
  await fs.writeFile(toml, 'prompt = """\n' + literal + '"""\n');
  assert.equal(await hook('claude', f.input(), f.config, never), null);
  await fs.writeFile(toml, `[projects.${JSON.stringify(f.config.cwd)}]\n`);
  assert.equal(await activated(f.config), true);
  assert.deepEqual(projectPaths(literal), [f.config.cwd]);
  if (process.platform === 'win32') {
    await fs.writeFile(toml, `[projects.${JSON.stringify(f.config.cwd.toUpperCase().replaceAll('\\', '/'))}]\n`);
    assert.equal(await activated(f.config), true);
  }
});

test('install and uninstall follow file symlinks and keep links and unrelated target settings intact', async t => {
  const f = await fixture(t), settings = path.join(f.config.claudeHome, 'settings.json');
  const target = path.join(f.root, 'real-settings.json'), tomlTarget = path.join(f.root, 'real-config.toml');
  const toml = path.join(f.config.codexHome, 'config.toml');
  await fs.writeFile(target, '{"keep":true}\n'); await fs.writeFile(tomlTarget, 'model = "keep"\n');
  await fs.unlink(toml);
  await fs.symlink(target, settings, 'file'); await fs.symlink(tomlTarget, toml, 'file');
  for (const remove of [false, true]) {
    configure({ env: f.env, remove, log: () => {} });
    assert((await fs.lstat(settings)).isSymbolicLink()); assert((await fs.lstat(toml)).isSymbolicLink());
    assert.equal(await fs.realpath(settings), await fs.realpath(target)); assert.equal(await fs.realpath(toml), await fs.realpath(tomlTarget));
    assert.equal(JSON.parse(await fs.readFile(target)).keep, true);
    assert.equal(Boolean(JSON.parse(await fs.readFile(target)).hooks), !remove);
  }
  assert.equal(await fs.readFile(tomlTarget, 'utf8'), 'model = "keep"\n');
});

test('mirror seed uses only the latest compact summary and recent context, within the byte budget', async t => {
  const f = await fixture(t); f.config.contextTokens = 700;
  const history = Array.from({ length: 100 }, (_, n) => c(f.id, `OLD-MONTH-${n} ` + 'x'.repeat(200)));
  history.push({ type: 'system', subtype: 'compact_boundary', uuid: randomUUID(), sessionId: f.id }, c(f.id, 'LATEST-MONTH-SUMMARY', { isCompactSummary: true }));
  for (let n = 0; n < 20; n++) history.push(c(f.id, `FRESH-${n} ` + 'я'.repeat(160)));
  await fs.writeFile(f.file, jsonl(history)); await hook('claude', f.input(), f.config, opts);
  const p = await f.pair(), seedRows = (await fs.readFile(p.codex.file, 'utf8')).trim().split('\n').map(JSON.parse);
  const messages = seedRows.filter(r => r.type === 'response_item');
  const content = messages.map(r => r.payload.content[0].text).join('\n');
  assert.match(messages[0].payload.content[0].text, /LATEST-MONTH-SUMMARY/);
  assert.match(content, /FRESH-19/); assert.doesNotMatch(content, /OLD-MONTH/);
  assert(Buffer.byteLength(content) <= f.config.contextTokens * 4);
  assert.equal(p.codex.pending.length, 0);
});

test('a month-unopened mirror receives a compact snapshot, retries it, acknowledges it, then resumes deltas', async t => {
  const f = await fixture(t); f.config.contextTokens = 800;
  await fs.writeFile(f.file, jsonl([c(f.id, 'INITIAL-SEED')])); await hook('claude', f.input(), f.config, opts);
  let p = await f.pair();
  await f.append(f.file, [...Array.from({ length: 100 }, (_, n) => c(f.id, `STALE-${n}`)),
    { type: 'system', subtype: 'compact_boundary', uuid: randomUUID(), sessionId: f.id },
    c(f.id, 'MONTH-SUMMARY', { isCompactSummary: true }),
    ...Array.from({ length: 30 }, (_, n) => c(f.id, `RECENT-${n} ` + 'x'.repeat(150)))]);
  await hook('claude', f.input(), f.config, opts);
  p = await f.pair(); assert.equal(p.codex.pending.length, 0); assert(p.codex.snapshot);
  const input = f.input('codex', p.codex, 'SessionStart');
  const output = await hook('codex', input, f.config, opts), text = output.hookSpecificOutput.additionalContext;
  assert.match(text, /MONTH-SUMMARY/); assert.match(text, /RECENT-29/); assert.doesNotMatch(text, /STALE-|INITIAL-SEED/);
  assert(Buffer.byteLength(text) <= f.config.contextTokens * 4);
  const prompt = f.input('codex', p.codex, 'UserPromptSubmit');
  assert.deepEqual(await hook('codex', prompt, f.config, opts), { hookSpecificOutput: { ...output.hookSpecificOutput, hookEventName: 'UserPromptSubmit' } });
  await f.append(p.codex.file, [{ type: 'response_item', payload: { type: 'message', role: 'developer', content: [{ type: 'input_text', text }] } }]);
  assert.equal(await hook('codex', prompt, f.config, opts), null);
  await f.append(f.file, [c(f.id, 'JUST-A-DELTA')]); await hook('claude', f.input(), f.config, opts);
  const delta = (await hook('codex', prompt, f.config, opts)).hookSpecificOutput.additionalContext;
  assert.match(delta, /JUST-A-DELTA/); assert.doesNotMatch(delta, /MONTH-SUMMARY|RECENT-/);
});

test('backlog overflow without compaction switches to the latest bounded context with an omission note', async t => {
  const f = await fixture(t); f.config.contextTokens = 600;
  await fs.writeFile(f.file, jsonl([c(f.id, 'SEED')])); await hook('claude', f.input(), f.config, opts);
  const p = await f.pair();
  await f.append(f.file, Array.from({ length: 30 }, (_, n) => c(f.id, `OVERFLOW-${n} ` + 'x'.repeat(300))));
  const output = await hook('codex', f.input('codex', p.codex, 'UserPromptSubmit'), f.config, opts);
  const text = output.hookSpecificOutput.additionalContext;
  assert.match(text, /Earlier context omitted/); assert.match(text, /OVERFLOW-29/); assert.doesNotMatch(text, /OVERFLOW-0 /);
  assert(Buffer.byteLength(text) <= f.config.contextTokens * 4);
});

test('Codex compacted records in the same file replace older context for seeds and later delivery', async t => {
  for (const type of ['compacted', 'response_item']) {
    const f = await fixture(t); f.config.contextTokens = 600;
    const id = randomUUID(), file = path.join(f.config.codexSessionsDir, id + '.jsonl');
    const compact = { type, payload: { ...(type === 'response_item' ? { type: 'compacted' } : {}), replacement_history: [
      { type: 'message', role: 'developer', content: [{ type: 'input_text', text: 'PRIVATE-INSTRUCTIONS' }] },
      { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'CODEX-SUMMARY' }] }
    ] } };
    await fs.mkdir(f.config.codexSessionsDir, { recursive: true });
    await fs.writeFile(file, jsonl([{ type: 'session_meta', payload: { id, cwd: f.config.cwd, source: 'cli', originator: 'codex-tui' } }, x('OLD-CODEX'), compact, x('FRESH-CODEX')]));
    const input = f.input('codex', { id, file }); await hook('codex', input, f.config, opts);
    const p = await f.pair(), seed = await fs.readFile(p.claude.file, 'utf8');
    assert.match(seed, /CODEX-SUMMARY/); assert.match(seed, /FRESH-CODEX/);
    assert.doesNotMatch(seed.split('\n').filter(Boolean).map(JSON.parse).filter(r => r.message).map(r => JSON.stringify(r.message)).join(''), /OLD-CODEX|PRIVATE-INSTRUCTIONS/);
    await f.append(file, [{ ...compact, payload: { ...compact.payload, replacement_history: [{ type: 'message', role: 'user', content: [{ type: 'input_text', text: 'SECOND-CODEX-SUMMARY' }] }] } }, x('LATEST-CODEX')]);
    const output = await hook('claude', f.input('claude', p.claude, 'SessionStart'), f.config, opts);
    const text = output.hookSpecificOutput.additionalContext;
    assert.match(text, /SECOND-CODEX-SUMMARY/); assert.match(text, /LATEST-CODEX/); assert.doesNotMatch(text, /FRESH-CODEX/);
    assert(Buffer.byteLength(text) <= f.config.contextTokens * 4);
  }
});

test('Claude sends one block with preview ACK; Codex keeps acknowledged chunks', async t => {
  const { LIMITS } = require('../src/sync/delivery');
  for (const target of ['claude', 'codex']) {
    const f = await fixture(t);
    await fs.writeFile(f.file, jsonl([c(f.id, 'initial seed')])); await hook('claude', f.input(), f.config, opts);
    const p = await f.pair(), source = target === 'codex' ? p.claude : p.codex;
    const summary = 'SUMMARY-BEGIN ' + 'ю😀'.repeat(24000) + ' SUMMARY-END';
    if (target === 'codex') await f.append(source.file, [
      { type: 'system', subtype: 'compact_boundary', uuid: randomUUID(), sessionId: source.id },
      c(source.id, summary, { isCompactSummary: true }), c(source.id, 'FRESH-END')
    ]);
    else await f.append(source.file, [{ type: 'compacted', payload: { message: summary, replacement_history: [] } }, x('FRESH-END')]);
    const input = f.input(target, p[target], 'UserPromptSubmit');
    let output = await hook(target, input, f.config, opts), joined = '', chunks = 0;
    while (output) {
      const text = output.hookSpecificOutput.additionalContext;
      assert(text.startsWith('[session-sync:v2:'));
      assert(Buffer.byteLength(text) <= Math.min(LIMITS[target], f.config.contextTokens * 4));
      if (target === 'claude') {
        assert(text.length > 10000);
        assert.equal((text.match(/\[session-sync:v2:/g) || []).length, 1);
        assert.match(text.slice(0, 2000), /^\[session-sync:v2:[a-f0-9]{64}\]\nFull paired-session context follows; if this is a preview, read the saved file path given by Claude Code before answering/);
        assert.match(text.slice(0, 2000), /Latest paired-session messages[\s\S]*FRESH-END/, 'preview must carry the newest messages');
      }
      assert(!text.includes('\uFFFD')); joined += text;
      assert.deepEqual(await hook(target, input, f.config, opts), output, 'unacknowledged chunk must retry unchanged');
      await f.append(p[target].file, target === 'claude' ? [c(p[target].id, text.slice(0, 2000), { isMeta: true })]
        : [{ type: 'response_item', payload: { type: 'message', role: 'developer', content: [{ type: 'input_text', text }] } }]);
      output = await hook(target, input, f.config, opts);
      assert(++chunks < 40, 'snapshot must finish rather than loop');
    }
    assert(target === 'claude' ? chunks === 1 : chunks > 1); assert.match(joined, /SUMMARY-BEGIN/); assert.match(joined, /FRESH-END/);
    assert.equal((await f.pair())[target].snapshot, null);
    assert.equal(await hook(target, input, f.config, opts), null);
    // A delta can exceed the per-hook cap while remaining below the active-context budget.
    const delta = 'DELTA-BEGIN ' + 'a'.repeat(90000) + ' DELTA-END';
    await f.append(source.file, target === 'codex' ? [c(source.id, delta), c(source.id, 'LAST-PENDING')]
      : [x(delta), x('LAST-PENDING')]);
    output = await hook(target, input, f.config, opts); joined = ''; chunks = 0;
    while (output) {
      const text = output.hookSpecificOutput.additionalContext; assert(Buffer.byteLength(text) <= LIMITS[target]); joined += text;
      if (target === 'claude') { assert.equal((text.match(/\[session-sync:v2:/g) || []).length, 1); assert.match(text.slice(0, 2000), /LAST-PENDING/); }
      await f.append(p[target].file, target === 'claude' ? [c(p[target].id, text.slice(0, 2000), { isMeta: true })]
        : [{ type: 'response_item', payload: { type: 'message', role: 'developer', content: [{ type: 'input_text', text }] } }]);
      output = await hook(target, input, f.config, opts); assert(++chunks < 25);
    }
    assert(target === 'claude' ? chunks === 1 : chunks > 1);
    assert.match(joined, /DELTA-BEGIN/); assert.match(joined, /DELTA-END/); assert.match(joined, /LAST-PENDING/);
    assert.equal((await f.pair())[target].pending.length, 0);
    assert.equal(await hook(target, input, f.config, opts), null);
  }
});

test('claude delivery with a stale snapshot position restarts from the beginning of the text', async t => {
  const { output } = require('../src/sync/delivery');
  const text = 'SNAP-BEGIN ' + 'x'.repeat(30000) + ' SNAP-END';
  const context = { materialize: async () => [{ origin: 'sync', role: 'user', text }], render: entries => entries.map(e => e.text).join('\n') };
  const endpoint = { snapshot: { key: 'snap', position: 12000, budget: 200000 }, pending: [], inflight: [] };
  const result = await output('claude', endpoint, 'unused.journal', 200000, context);
  assert.match(result, /SNAP-BEGIN/); assert.match(result, /SNAP-END/);
  assert.doesNotMatch(result, /�/);
});

test('busy locks exit silently; stale locks recover and an old owner cannot remove a newer lease', async t => {
  const f = await fixture(t), { acquire, TIMEOUT } = require('../src/sync/lease');
  await fs.writeFile(f.file, jsonl([c(f.id, 'hello')])); await hook('claude', f.input(), f.config, opts);
  const file = path.join(f.config.stateDir, 'lock');
  const release = acquire(f.config.stateDir); const lock = JSON.parse(await fs.readFile(file));
  assert.equal(lock.pid, process.pid); assert(lock.startedAt); assert(lock.token);
  assert.equal(await hook('claude', f.input(), f.config, opts), null);
  const { spawnSync } = require('node:child_process');
  const result = spawnSync(process.execPath, [path.resolve(__dirname, '../scripts/agents/sync.js'), 'hook', 'claude'], {
    env: { ...process.env, ...f.env }, input: JSON.stringify(f.input()), encoding: 'utf8'
  });
  assert.equal(result.status, 0); assert.equal(result.stdout, ''); assert.equal(result.stderr, '');
  const old = new Date(Date.now() - TIMEOUT - 1000); await fs.utimes(file, old, old);
  const newer = acquire(f.config.stateDir); assert.equal(typeof newer, 'function');
  release(); assert((await fs.stat(file)).isFile()); newer();
  await fs.writeFile(file, JSON.stringify({ pid: 999999, startedAt: +old, token: 'killed-hook' })); await fs.utimes(file, old, old);
  await hook('claude', f.input(), f.config, opts); await assert.rejects(fs.stat(file), /ENOENT/);
});

test('checkpoint keys stay bounded, unchanged hooks do not rewrite it, and reset dedup uses the archive', async t => {
  const f = await fixture(t);
  await fs.writeFile(f.file, jsonl(Array.from({ length: 500 }, (_, n) => c(f.id, `event-${n}`))));
  await hook('claude', f.input(), f.config, opts);
  const p = await f.pair(); assert(p.known.length <= 256);
  const file = path.join(f.config.stateDir, `${p.id}.json`);
  await hook('claude', f.input(), f.config, opts); const before = await fs.stat(file);
  await hook('claude', f.input(), f.config, opts); assert.equal((await fs.stat(file)).mtimeMs, before.mtimeMs);
  const replaced = f.file + '.replacement'; await fs.copyFile(f.file, replaced); await fs.rename(replaced, f.file);
  await hook('claude', f.input(), f.config, opts);
  const after = await f.pair(); assert(after.known.length <= 256); assert.equal(after.codex.pending.length, 0);
});

test('denied sessions use a bounded decision file and old legacy decision files are pruned', async t => {
  const f = await fixture(t), { remember } = require('../src/sync/process-cache');
  const state = { entries: {} };
  for (let n = 0; n < 300; n++) remember(state, `id-${n}`, { file: f.file, cliIdentity: String(n) });
  assert.equal(Object.keys(state.entries).length, 128);
  await fs.mkdir(f.config.stateDir, { recursive: true });
  const old = path.join(f.config.stateDir, `claude-${'a'.repeat(64)}.decision.json`);
  await fs.writeFile(old, '{}'); const date = new Date(Date.now() - 8 * 86400000); await fs.utimes(old, date, date);
  await fs.writeFile(f.file, jsonl([c(f.id, 'denied')]));
  let probes = 0;
  const denied = { env: {}, cliIdentity: 'denied-cli', interactive: async () => { probes++; return false; } };
  await hook('claude', f.input('claude', undefined, 'SessionStart'), f.config, denied);
  await hook('claude', f.input('claude', undefined, 'SessionStart'), f.config, denied);
  assert.equal(probes, 1); await assert.rejects(fs.stat(old), /ENOENT/);
  const decisions = JSON.parse(await fs.readFile(path.join(f.config.stateDir, 'decisions.json'))); assert.equal(Object.keys(decisions.entries).length, 1);
});

test('SessionStart reuses the CLI pid/start identity and probes again only when it changes', async t => {
  const f = await fixture(t); await fs.writeFile(f.file, jsonl([c(f.id, 'hello')]));
  let probes = 0;
  const options = { env: {}, cliIdentity: 'pid:123-start:one', interactive: async () => { probes++; return true; } };
  const input = f.input('claude', undefined, 'SessionStart');
  await hook('claude', input, f.config, options); await hook('claude', input, f.config, options); assert.equal(probes, 1);
  await hook('claude', input, f.config, { ...options, cliIdentity: 'pid:123-start:two' }); assert.equal(probes, 2);
  const { identity } = require('../src/sync/process-cache');
  assert.equal(await identity('codex', { platform: 'win32', run: async exe => {
    assert.equal(exe, 'cscript.exe'); return { stdout: JSON.stringify([{ pid: 12, start: 'birth', name: 'codex.exe', command: 'codex.exe' }]) };
  } }), '12:birth');
  assert.equal(await identity('codex', { platform: 'linux', pid: 2, run: async exe => {
    assert.equal(exe, 'ps'); return { stdout: '2 12 Fri Oct 30 10:00:00 2026 node node /app/sync.js\n12 1 Fri Oct 30 09:00:00 2026 codex codex\n' };
  } }), '12:Fri Oct 30 09:00:00 2026');
});

test('mirror versions come from source/latest client sessions, with cached --version fallback', async t => {
  const f = await fixture(t); delete f.config.versions;
  await fs.mkdir(f.config.codexSessionsDir, { recursive: true });
  await fs.writeFile(path.join(f.config.codexSessionsDir, 'version-source.jsonl'), jsonl([{ type: 'session_meta', payload: { cli_version: '9.8.7' } }]));
  await fs.writeFile(f.file, jsonl([c(f.id, 'hello', { version: '6.5.4' })]));
  await hook('claude', f.input(), f.config, opts); const p = await f.pair();
  assert.equal(JSON.parse((await fs.readFile(p.codex.file, 'utf8')).split('\n')[0]).payload.cli_version, '9.8.7');
  const { version } = require('../src/sync/versions');
  assert.equal(await version('claude', f.config, p), '6.5.4');
  await fs.rm(f.config.codexSessionsDir, { recursive: true, force: true });
  await fs.rm(path.join(f.config.stateDir, 'versions.json'), { force: true });
  let calls = 0;
  const result = await version('codex', f.config, {}, { run: async (exe, args) => { calls++; assert.equal(exe, 'codex'); assert.deepEqual(args, ['--version']); return { stdout: 'codex-cli 8.7.6' }; } });
  result.cache.codex = { value: result.value, at: Date.now() }; await fs.writeFile(result.file, JSON.stringify(result.cache));
  assert.equal(await version('codex', f.config, {}, { run: () => { throw Error('must use version cache'); } }), '8.7.6'); assert.equal(calls, 1);
});

test('Codex mirrors register through app-server with native preview and retain model context only once', async t => {
  const f = await fixture(t);
  await fs.mkdir(f.config.cwd, { recursive: true });
  if (process.platform === 'win32') f.config.cwd = f.config.cwd.toLowerCase();
  await fs.writeFile(f.file, jsonl([c(f.id, 'UNIQUE-USER-CONTEXT'), c(f.id, 'UNIQUE-ASSISTANT-CONTEXT', {
    type: 'assistant', message: { role: 'assistant', content: 'UNIQUE-ASSISTANT-CONTEXT' }
  })]));
  await hook('claude', f.input(), f.config, opts);
  const p = await f.pair(), records = (await fs.readFile(p.codex.file, 'utf8')).trim().split('\n').map(JSON.parse);
  assert.equal(records[0].type, 'session_meta');
  assert.equal(records[0].payload.cwd, await fs.realpath(f.config.cwd));
  const created = f.config.apiThreads.get(p.codex.id);
  assert.equal(p.codex.managed, 'app-server'); assert.equal(created.preview, 'UNIQUE-USER-CONTEXT \u00b7 Claude');
  assert.equal(created.source, 'vscode'); assert.equal(created.goal, undefined);
  assert.deepEqual(f.config.apiRequests.find(r => r.method === 'thread/start').params, { cwd: await fs.realpath(f.config.cwd), ephemeral: false });
  assert(!f.config.apiRequests.some(r => r.method === 'turn/start'));
  // rollout_reconstruction.rs records ResponseItems and ignores UserMessage EventMsgs in model history.
  const history = records.filter(row => row.type === 'response_item').map(row => row.payload);
  assert.deepEqual(history.map(item => item.role), ['user', 'assistant']);
  const text = history.map(item => item.content[0].text).join('\n');
  assert.equal(text.split('UNIQUE-USER-CONTEXT').length - 1, 1);
  assert.equal(text.split('UNIQUE-ASSISTANT-CONTEXT').length - 1, 1);
  assert.equal(extract('codex', records.filter(r => r.type === 'event_msg'), p.codex.id).length, 0, 'preview is never exported back');
  await hook('claude', f.input(), f.config, opts);
  assert.equal((await f.registry()).pairs.length, 1);
});

test('unavailable app-server defers creation; retry reuses its thread and does not duplicate an injection with a lost reply', async t => {
  const f = await fixture(t); f.config.apiFail = 'initialize';
  await fs.writeFile(f.file, jsonl([c(f.id, 'ORIGINAL-CONTEXT')]));
  assert.equal(await hook('claude', f.input(), f.config, opts), null);
  let p = await f.pair(); assert.equal(p.codex.seeded, false);
  assert.match(await fs.readFile(path.join(f.config.stateDir, 'sync.log'), 'utf8'), /deferred/);
  delete f.config.apiFail; f.config.dropInjectionReply = true;
  assert.equal(await hook('claude', f.input(), f.config, opts), null);
  p = await f.pair(); assert.equal(p.codex.managed, 'app-server'); assert.equal(p.codex.seeded, false);
  const id = p.codex.id;
  await f.append(f.file, [c(f.id, 'NEW-WHILE-DEFERRED')]);
  await hook('claude', f.input(), f.config, opts);
  p = await f.pair(); assert.equal(p.codex.id, id); assert.equal(p.codex.seeded, true);
  assert.equal(f.config.apiRequests.filter(r => r.method === 'thread/start').length, 1);
  assert.equal(f.config.apiRequests.filter(r => r.method === 'thread/inject_items').length, 1);
  const rows = (await fs.readFile(p.codex.file, 'utf8')).trim().split('\n').map(JSON.parse);
  assert.equal(rows.filter(r => r.type === 'response_item').length, 1);
  const output = await hook('codex', f.input('codex', p.codex, 'SessionStart'), f.config, opts);
  assert.match(output.hookSpecificOutput.additionalContext, /NEW-WHILE-DEFERRED/);
  assert.doesNotMatch(output.hookSpecificOutput.additionalContext, /ORIGINAL-CONTEXT/);
});

test('Claude mirrors always have a first user prompt, a title, and a connected assistant leaf', async t => {
  for (const content of [[], [{ type: 'response_item', payload: { type: 'message', role: 'assistant', id: 'answer', content: [{ type: 'output_text', text: 'assistant-only' }] } }]]) {
    const f = await fixture(t), id = randomUUID(), file = path.join(f.config.codexSessionsDir, `${id}.jsonl`);
    await fs.mkdir(f.config.codexSessionsDir, { recursive: true });
    await fs.writeFile(file, jsonl([{ type: 'session_meta', payload: { id, cwd: f.config.cwd, source: 'cli', originator: 'codex-tui' } }, ...content]));
    await hook('codex', f.input('codex', { id, file }), f.config, opts);
    const p = await f.pair(), rows = (await fs.readFile(p.claude.file, 'utf8')).trim().split('\n').map(JSON.parse);
    const messages = rows.filter(row => row.message);
    assert.equal(messages[0].type, 'user'); assert.match(messages[0].message.content, /\u00b7 Codex$/);
    assert.equal(messages.at(-1).type, 'assistant');
    for (let n = 1; n < messages.length; n++) assert.equal(messages[n].parentUuid, messages[n - 1].uuid);
    assert.match(rows.find(row => row.type === 'custom-title').customTitle, /\u00b7 Codex$/);
    assert(!messages.some(row => row.isSidechain || row.isMeta || row.isNonInteractiveSession));
  }
});

test('recreate-mirror defaults to dry-run, backs up only the mirror, and lets a source hook rebuild', async t => {
  const { recreate } = require('../scripts/agents/recreate-mirror');
  for (const version of [2, 3]) {
    const f = await fixture(t); await fs.writeFile(f.file, jsonl([c(f.id, 'KEEP-SOURCE')]));
    await hook('claude', f.input(), f.config, opts);
    const p = await f.pair(), original = await fs.readFile(f.file, 'utf8');
    delete p.codex.managed; delete p.codex.createdBy;
    await fs.writeFile(path.join(f.config.stateDir, `${p.id}.json`), JSON.stringify(p));
    const oldRows = (await fs.readFile(p.codex.file, 'utf8')).trim().split('\n').map(JSON.parse);
    oldRows[0].payload.session_sync = { mirror: true, pair: p.id };
    await fs.writeFile(p.codex.file, jsonl(oldRows));
    const registry = path.join(f.config.stateDir, 'pairs.json');
    if (version === 2) await fs.writeFile(registry, JSON.stringify({ version: 2, pairs: [p] }));
    const before = await fs.readFile(registry, 'utf8');
    await recreate(registry, p.id, { env: f.env, log: () => {} });
    assert.equal(await fs.readFile(registry, 'utf8'), before); assert((await fs.stat(p.codex.file)).isFile());
    await recreate(registry, p.id, { apply: true, env: f.env, log: () => {} });
    await assert.rejects(fs.stat(p.codex.file), /ENOENT/);
    assert.equal(JSON.parse(await fs.readFile(registry)).pairs.length, 0);
    assert.equal(await fs.readFile(f.file, 'utf8'), original);
    assert((await fs.readdir(path.dirname(p.codex.file))).some(name => name.includes('.bak-recreate-')));
    if (version === 3) {
      await hook('claude', f.input(), f.config, opts); const fresh = await f.pair();
      assert.notEqual(fresh.id, p.id); assert.notEqual(fresh.codex.id, p.codex.id);
      assert.match(f.config.apiThreads.get(fresh.codex.id).preview, /\u00b7 Claude$/);
    }
  }
});

test('large first injection gets up to 20s within a 30s hook and inherits user permissions', async t => {
  const f = await fixture(t); f.config.contextTokens = 200000; f.config.injectDelay = 2100;
  await fs.writeFile(f.file, jsonl([c(f.id, 'x'.repeat(510000)), { type: 'custom-title', customTitle: 'Large context' }]));
  const start = performance.now(); await hook('claude', f.input(), f.config, opts);
  const p = await f.pair(); assert.equal(p.codex.seeded, true); assert(performance.now() - start > 2000);
  const injection = f.config.apiRequests.find(r => r.method === 'thread/inject_items');
  assert(injection.limit > 2000 && injection.limit <= 20000);
  assert.equal(f.config.apiRequests.filter(r => r.method === 'thread/inject_items').length, 1);
  const params = f.config.apiRequests.find(r => r.method === 'thread/start').params;
  for (const key of ['sandbox', 'approvalPolicy', 'permissions']) assert.equal(Object.hasOwn(params, key), false);
  assert.equal(hookConfigs('script').claude.hooks.Stop[0].hooks[0].timeout, 30);
  assert.match(hookConfigs('script').codex, /timeout = 30/);
});

test('Claude title precedence and later renames update only the mirror name and preserve preview', async t => {
  const f = await fixture(t);
  await fs.writeFile(f.file, jsonl([c(f.id, 'First question'), { type: 'summary', summary: 'Summary title' },
    { type: 'custom-title', customTitle: 'Old custom' }, { type: 'custom-title', customTitle: 'Latest custom' }]));
  await hook('claude', f.input(), f.config, opts); const p = await f.pair(), thread = f.config.apiThreads.get(p.codex.id);
  assert.equal(thread.name, 'Latest custom \u00b7 Claude'); assert.equal(thread.preview, thread.name);
  const before = f.config.apiRequests.filter(r => r.method === 'thread/name/set').length;
  await hook('claude', f.input(), f.config, opts);
  assert.equal(f.config.apiRequests.filter(r => r.method === 'thread/name/set').length, before);
  await f.append(f.file, [{ type: 'custom-title', customTitle: 'Renamed' }]); await hook('claude', f.input(), f.config, opts);
  assert.equal(thread.name, 'Renamed \u00b7 Claude'); assert.equal(thread.preview, 'Latest custom \u00b7 Claude'); assert.equal(thread.goal, undefined);
  const { observe, title } = require('../src/sync/names'); const endpoint = {};
  observe(endpoint, 'claude', [c('id', 'a'.repeat(100)), { type: 'summary', summary: 'Summary' }], true);
  assert.equal(title(endpoint, 'claude'), 'Summary \u00b7 Claude');
  observe(endpoint, 'claude', [c('id', 'a'.repeat(100))], true);
  assert.equal(title(endpoint, 'claude'), 'a'.repeat(60) + ' \u00b7 Claude');
});

test('Codex names use API/index and later changes append one Claude custom-title', async t => {
  const f = await fixture(t), id = randomUUID(), file = path.join(f.config.codexSessionsDir, `${id}.jsonl`);
  await fs.mkdir(f.config.codexSessionsDir, { recursive: true });
  await fs.writeFile(file, jsonl([{ type: 'session_meta', payload: { id, cwd: f.config.cwd, source: 'cli', originator: 'codex-tui' } }, x('Original prompt')]));
  await fs.writeFile(path.join(f.config.codexHome, 'session_index.jsonl'), jsonl([{ id, thread_name: 'Index title' }]));
  f.config.apiThreads.set(id, { id, name: 'API title', path: file, cwd: f.config.cwd });
  await hook('codex', f.input('codex', { id, file }), f.config, opts); const p = await f.pair();
  let rows = (await fs.readFile(p.claude.file, 'utf8')).trim().split('\n').map(JSON.parse);
  assert.equal(rows.at(-1).customTitle, 'API title \u00b7 Codex');
  f.config.apiThreads.get(id).name = 'Changed API title';
  await fs.writeFile(path.join(f.config.codexHome, 'state_5.sqlite'), 'fake stamp change');
  await hook('codex', f.input('codex', { id, file }), f.config, opts);
  rows = (await fs.readFile(p.claude.file, 'utf8')).trim().split('\n').map(JSON.parse);
  assert.equal(rows.at(-1).customTitle, 'Changed API title \u00b7 Codex');
  const before = await fs.readFile(p.claude.file, 'utf8'); await hook('codex', f.input('codex', { id, file }), f.config, opts);
  assert.equal(await fs.readFile(p.claude.file, 'utf8'), before);
});

test('presentation is written only after archive/unload, supports paginated UI and never duplicates model items', async t => {
  const f = await fixture(t);
  await fs.writeFile(f.file, jsonl([c(f.id, 'Question'), c(f.id, '', { type: 'assistant', message: { role: 'assistant', model: 'claude-source-model', content: [
    { type: 'tool_use', id: 'call-1', name: 'Read', input: { file_path: 'example.txt' } }] } }),
    c(f.id, [{ type: 'tool_result', tool_use_id: 'call-1', content: 'file contents' }]),
    c(f.id, 'Answer', { type: 'assistant', message: { role: 'assistant', model: 'claude-source-model', content: 'Answer' } })]));
  await hook('claude', f.input(), f.config, opts); const p = await f.pair();
  const rows = (await fs.readFile(p.codex.file, 'utf8')).trim().split('\n').map(JSON.parse), events = rows.filter(r => r.type === 'event_msg');
  assert.equal(rows.filter(r => r.type === 'response_item').length, 3);
  const items = events.filter(r => r.payload.type === 'item_completed').map(r => r.payload);
  assert.deepEqual(items.map(p => p.item.type), ['UserMessage', 'CommandExecution', 'AgentMessage']);
  assert.equal(items[0].item.content[0].text, 'Question'); assert.equal(items[1].item.parsed_cmd[0].type, 'read'); assert.equal(items[1].item.aggregated_output, 'file contents');
  assert.equal(items[2].item.content[0].text, 'Answer'); assert(items.every(item => item.thread_id === p.codex.id));
  assert(events.some(r => r.payload.type === 'task_started')); assert.equal(events.at(-1).payload.type, 'task_complete');
  const archive = f.config.apiRequests.findIndex(r => r.method === 'thread/archive'), loaded = f.config.apiRequests.findIndex(r => r.method === 'thread/loaded/list');
  assert(archive >= 0 && loaded > archive); assert(f.config.apiRequests.some(r => r.method === 'thread/unarchive'));
  assert.equal(f.config.apiThreads.get(p.codex.id).loaded, false);
  assert.equal(f.config.apiThreads.get(p.codex.id).archived, false);
});

test('Claude imported tools are native completed pairs with original model and valid parent chain', async t => {
  const f = await fixture(t), id = randomUUID(), file = path.join(f.config.codexSessionsDir, `${id}.jsonl`);
  await fs.mkdir(f.config.codexSessionsDir, { recursive: true });
  await fs.writeFile(file, jsonl([{ type: 'session_meta', payload: { id, cwd: f.config.cwd, source: 'cli', originator: 'codex-tui' } },
    { type: 'turn_context', payload: { model: 'source-codex-model' } }, x('Question'),
    { type: 'response_item', payload: { type: 'function_call', id: 'call', call_id: 'call', name: 'exec_command', arguments: '{"cmd":"echo hello"}' } },
    { type: 'response_item', payload: { type: 'function_call_output', call_id: 'call', output: 'hello' } }]));
  await hook('codex', f.input('codex', { id, file }), f.config, opts); const p = await f.pair();
  const messages = (await fs.readFile(p.claude.file, 'utf8')).trim().split('\n').map(JSON.parse).filter(row => row.message);
  const call = messages.find(row => Array.isArray(row.message.content) && row.message.content[0].type === 'tool_use');
  const result = messages.find(row => Array.isArray(row.message.content) && row.message.content[0].type === 'tool_result');
  assert.equal(call.message.content[0].name, 'Bash'); assert.deepEqual(call.message.content[0].input, { command: 'echo hello' });
  assert.equal(call.message.content[0].id, result.message.content[0].tool_use_id); assert.equal(result.message.content[0].content, 'hello');
  assert.equal(call.message.model, 'source-codex-model'); assert.equal(call.version, 'test-claude');
  for (let n = 1; n < messages.length; n++) assert.equal(messages[n].parentUuid, messages[n - 1].uuid);
});

test('Codex patch imports form distinct Claude Write/Edit pairs; unknown tools become summaries', async t => {
  const f = await fixture(t), id = randomUUID(), file = path.join(f.config.codexSessionsDir, `${id}.jsonl`);
  const patch = '*** Begin Patch\n*** Add File: new.txt\n+hello\n*** Update File: old.txt\n@@\n-old\n+new\n*** End Patch';
  await fs.mkdir(f.config.codexSessionsDir, { recursive: true });
  await fs.writeFile(file, jsonl([{ type: 'session_meta', payload: { id, cwd: f.config.cwd, source: 'cli', originator: 'codex-tui' } }, x('Make changes'),
    { type: 'response_item', payload: { type: 'custom_tool_call', id: 'patch', call_id: 'patch', name: 'apply_patch', input: patch } },
    { type: 'response_item', payload: { type: 'custom_tool_call_output', call_id: 'patch', output: 'Success. Updated files.' } },
    { type: 'response_item', payload: { type: 'function_call', id: 'web', call_id: 'web', name: 'web_search', arguments: '{}' } },
    { type: 'response_item', payload: { type: 'function_call_output', call_id: 'web', output: 'Found useful information.' } }]));
  await hook('codex', f.input('codex', { id, file }), f.config, opts); const p = await f.pair();
  const rows = (await fs.readFile(p.claude.file, 'utf8')).trim().split('\n').map(JSON.parse);
  const uses = rows.flatMap(r => Array.isArray(r.message?.content) ? r.message.content.filter(b => b.type === 'tool_use') : []);
  const results = rows.flatMap(r => Array.isArray(r.message?.content) ? r.message.content.filter(b => b.type === 'tool_result') : []);
  assert.deepEqual(uses.map(b => b.name), ['Write', 'Edit']); assert.equal(new Set(uses.map(b => b.id)).size, 2);
  assert.equal(uses[0].input.content, 'hello\n'); assert.equal(uses[1].input.old_string, 'old'); assert.equal(uses[1].input.new_string, 'new');
  for (const use of uses) assert.equal(results.filter(result => result.tool_use_id === use.id).length, 1);
  assert(rows.some(row => typeof row.message?.content?.[0]?.text === 'string' && row.message.content[0].text.includes('Web operation completed.')));
  const editResult = rows.find(row => row.toolUseResult?.structuredPatch);
  assert.deepEqual(editResult.toolUseResult.structuredPatch[0].lines, ['-old', '+new']);
});

test('UI presentation preserves paginated ordinals and is idempotent after an interrupted publication', async t => {
  const f = await fixture(t), { writePresentation } = require('../src/sync/presentation');
  const file = path.join(f.root, 'archived-demo.jsonl');
  const prefix = jsonl([{ ordinal: 0, type: 'session_meta', payload: { history_mode: 'paginated' } },
    { ordinal: 1, type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'model once' }] } }]);
  await fs.writeFile(file, prefix);
  const entries = [{ role: 'user', text: 'Question', key: 'one' }, { role: 'assistant', text: 'Answer', key: 'two' }];
  await writePresentation(file, entries, f.id);
  const once = await fs.readFile(file, 'utf8'), rows = once.trim().split('\n').map(JSON.parse);
  assert.deepEqual(rows.map(row => row.ordinal), rows.map((_, n) => n));
  assert.equal(rows.filter(row => row.type === 'response_item').length, 1);
  await writePresentation(file, entries, f.id); assert.equal(await fs.readFile(file, 'utf8'), once);
});

test('publication waits for unload and can retry unarchive without repeating model or UI history', async t => {
  const f = await fixture(t); f.config.apiFail = 'thread/unarchive';
  await fs.writeFile(f.file, jsonl([c(f.id, 'One question')])); await hook('claude', f.input(), f.config, opts);
  let p = await f.pair(); assert.equal(p.codex.seeded, false); assert.equal(p.codex.presentationStage, 'unarchive');
  const before = await fs.readFile(p.codex.file, 'utf8'); delete f.config.apiFail;
  await hook('claude', f.input(), f.config, opts); p = await f.pair();
  assert.equal(p.codex.seeded, true); assert.equal(await fs.readFile(p.codex.file, 'utf8'), before);
  assert.equal(f.config.apiRequests.filter(row => row.method === 'thread/inject_items').length, 1);
});

test('app-server path normalization covers verbatim drive/UNC and malformed single-slash prefixes', async t => {
  const { plain } = require('../src/agents/codex-path');
  assert.equal(plain('\\\\?\\C:\\sessions\\rollout.jsonl'), 'C:\\sessions\\rollout.jsonl');
  assert.equal(plain('\\?\\C:\\sessions\\rollout.jsonl'), 'C:\\sessions\\rollout.jsonl');
  assert.equal(plain('\\\\?\\UNC\\server\\share\\rollout.jsonl'), '\\\\server\\share\\rollout.jsonl');
  assert.equal(plain('/tmp/session.jsonl'), '/tmp/session.jsonl'); assert.equal(plain(null), null);
  if (process.platform !== 'win32') return;
  for (const prefix of ['\\\\?\\', '\\?\\']) {
    const f = await fixture(t); f.config.apiPrefix = prefix;
    await fs.writeFile(f.file, jsonl([c(f.id, 'Windows path')])); await hook('claude', f.input(), f.config, opts);
    const p = await f.pair(); assert.equal(p.codex.seeded, true); assert.equal(p.codex.createdBy, 'agent-kit-session-sync');
    assert.equal(p.codex.file, f.config.apiThreads.get(p.codex.id).path);
    assert((await fs.stat(p.codex.file)).isFile());
  }
});

test('managed recreate trusts the validated pair, handles older metadata and prefers native delete with archive fallback', async t => {
  const { recreate } = require('../scripts/agents/recreate-mirror');
  for (const mode of ['new', 'legacy', 'archive']) {
    const f = await fixture(t); await fs.writeFile(f.file, jsonl([c(f.id, 'KEEP-NATIVE-SOURCE')]));
    await hook('claude', f.input(), f.config, opts);
    const p = await f.pair(), registry = path.join(f.config.stateDir, 'pairs.json'), pairFile = path.join(f.config.stateDir, `${p.id}.json`);
    const original = await fs.readFile(f.file, 'utf8');
    if (mode === 'legacy') {
      delete p.codex.createdBy; delete p.codex.appliedTitle; delete p.codex.presentationStage;
      // This is deliberately not a recognizable mirror header: ownership comes from the pair.
      await fs.writeFile(p.codex.file, 'old rollout without sync metadata\n');
    }
    if (process.platform === 'win32') { p.codex.file = '\\\\?\\' + p.codex.file; f.config.apiPrefix = '\\?\\'; }
    await fs.writeFile(pairFile, JSON.stringify(p));
    const before = await fs.readFile(registry, 'utf8'), calls = f.config.apiRequests.length;
    await recreate(registry, p.id, { env: f.env, log: () => {}, connect: () => { throw Error('dry-run must not connect'); } });
    assert.equal(await fs.readFile(registry, 'utf8'), before);
    f.config.noDelete = mode === 'archive';
    await recreate(registry, p.id, { apply: true, env: f.env, log: () => {}, connect: f.config.connectCodex });
    const requests = f.config.apiRequests.slice(calls);
    assert(requests.some(r => r.method === 'thread/delete' && r.params.threadId === p.codex.id));
    assert.equal(requests.some(r => r.method === 'thread/archive'), mode === 'archive');
    assert.equal(JSON.parse(await fs.readFile(registry)).pairs.length, 0);
    assert.equal(await fs.readFile(f.file, 'utf8'), original);
    if (mode === 'archive') assert.equal(f.config.apiThreads.get(p.codex.id).archived, true);
    else assert(!f.config.apiThreads.has(p.codex.id));
    await hook('claude', f.input(), f.config, opts); const replacement = await f.pair();
    assert.notEqual(replacement.codex.id, p.codex.id); assert.equal(replacement.codex.seeded, true);
  }
});

test('managed recreate rejects pair/server identity mismatches and preserves the registry on API failure', async t => {
  const { recreate } = require('../scripts/agents/recreate-mirror');
  const f = await fixture(t); await fs.writeFile(f.file, jsonl([c(f.id, 'Keep source')])); await hook('claude', f.input(), f.config, opts);
  const p = await f.pair(), registry = path.join(f.config.stateDir, 'pairs.json'), pairFile = path.join(f.config.stateDir, `${p.id}.json`);
  const before = await fs.readFile(registry, 'utf8');
  f.config.apiFail = 'thread/delete';
  await assert.rejects(recreate(registry, p.id, { apply: true, env: f.env, log: () => {}, connect: f.config.connectCodex }), /unavailable/);
  assert.equal(await fs.readFile(registry, 'utf8'), before); assert((await fs.stat(p.codex.file)).isFile());
  const wrongServer = () => {
    const client = f.config.connectCodex();
    return { ...client, async request(method, params) {
      const result = await client.request(method, params);
      return method === 'thread/read' ? { thread: { ...result.thread, id: 'unrelated-thread' } } : result;
    } };
  };
  const count = f.config.apiRequests.filter(r => r.method === 'thread/delete').length;
  await assert.rejects(recreate(registry, p.id, { apply: true, env: f.env, log: () => {}, connect: wrongServer }), /thread ID does not match/);
  assert.equal(f.config.apiRequests.filter(r => r.method === 'thread/delete').length, count);
  p.codex.createdBy = 'unrelated-client'; await fs.writeFile(pairFile, JSON.stringify(p));
  await assert.rejects(recreate(registry, p.id, { env: f.env, log: () => {} }), /ownership/);
  p.codex.createdBy = 'agent-kit-session-sync'; p.codex.id = randomUUID(); await fs.writeFile(pairFile, JSON.stringify(p));
  await assert.rejects(recreate(registry, p.id, { env: f.env, log: () => {} }), /ownership/);
});

test('service messages are absent from both injected model context and visible mirror history', async t => {
  const f = await fixture(t);
  await fs.writeFile(f.file, jsonl([
    c(f.id, 'SERVICE_TASK', { origin: { kind: 'task-notification', producer: 'background', runId: 'run' }, promptSource: 'system' }),
    c(f.id, '<command-name>/clear</command-name><local-command-stdout>SERVICE_COMMAND</local-command-stdout>'),
    c(f.id, [{ type: 'text', text: '<system-reminder>SERVICE_REMINDER</system-reminder>' }, { type: 'text', text: 'Actual user request' }],
      { origin: { kind: 'human' }, promptSource: 'typed', turnOrigin: 'human' }),
    c(f.id, '', { type: 'assistant', message: { role: 'assistant', content: 'Actual answer' } })
  ]));
  await hook('claude', f.input(), f.config, opts); const p = await f.pair();
  const injection = f.config.apiRequests.find(r => r.method === 'thread/inject_items');
  assert.deepEqual(injection.params.items.map(item => item.content[0].text.split('\n').slice(1).join('\n')), ['Actual user request', 'Actual answer']);
  const rollout = await fs.readFile(p.codex.file, 'utf8');
  assert.doesNotMatch(rollout, /SERVICE_|system-reminder|task-notification|command-name|local-command-stdout/);
  assert.equal(f.config.apiThreads.get(p.codex.id).name, 'Actual user request \u00b7 Claude');
});

test('sync uses daemon proxy without overriding originator and poisoned mirrors sync through pair ownership', async t => {
  const f = await fixture(t), inherited = { CODEX_INTERNAL_ORIGINATOR_OVERRIDE: 'claude-agent-broker' };
  await fs.writeFile(f.file, jsonl([c(f.id, 'Source')])); await hook('claude', f.input(), f.config, { ...opts, env: inherited });
  const connection = f.config.apiConnections[0];
  assert.equal(connection.mode, 'proxy'); assert.equal(connection.env.CODEX_INTERNAL_ORIGINATOR_OVERRIDE, 'claude-agent-broker');
  assert.equal(inherited.CODEX_INTERNAL_ORIGINATOR_OVERRIDE, 'claude-agent-broker');
  const p = await f.pair(), rows = (await fs.readFile(p.codex.file, 'utf8')).trim().split('\n').map(JSON.parse);
  rows[0].payload.originator = 'claude-agent-broker';
  await fs.writeFile(p.codex.file, jsonl([...rows, x('Real user reply')]));
  await hook('codex', f.input('codex', p.codex, 'SessionStart'), f.config, opts);
  const output = await hook('claude', f.input('claude', p.claude, 'UserPromptSubmit'), f.config, opts);
  assert.match(output.hookSpecificOutput.additionalContext, /Real user reply/);
});

test('Claude bash-mode pairs across hooks and injects one tag-free user-shell observation', async t => {
  const f = await fixture(t);
  await fs.writeFile(f.file, jsonl([c(f.id, '<bash-input>echo manual</bash-input>', { uuid: 'manual-input' })]));
  await hook('claude', f.input(), f.config, opts);
  let p = await f.pair(); assert.equal(p.claude.extraction.userShell.command, 'echo manual');
  await f.append(f.file, [c(f.id, '<bash-stdout>manual</bash-stdout><bash-stderr>warning</bash-stderr>', { parentUuid: 'manual-input' })]);
  await hook('claude', f.input(), f.config, opts); p = await f.pair();
  const archive = await f.archive(); assert.equal(archive.length, 1); assert.equal(archive[0].shell.command, 'echo manual');
  assert.doesNotMatch(archive[0].text, /<bash-/);
  const output = await hook('codex', f.input('codex', p.codex, 'UserPromptSubmit'), f.config, opts);
  assert.match(output.hookSpecificOutput.additionalContext, /User ran: echo manual/); assert.doesNotMatch(output.hookSpecificOutput.additionalContext, /<bash-/);
});

test('Codex user shell renders as linked Claude bash-mode records and never echoes back', async t => {
  const f = await fixture(t), id = randomUUID(), file = path.join(f.config.codexSessionsDir, `${id}.jsonl`);
  await fs.mkdir(f.config.codexSessionsDir, { recursive: true });
  const text = '<user_shell_command>\n<command>\necho manual\n</command>\n<result>\nExit code: 0\nDuration: 0.0100 seconds\nOutput:\nmanual\n</result>\n</user_shell_command>';
  await fs.writeFile(file, jsonl([{ type: 'session_meta', payload: { id, cwd: f.config.cwd, source: 'cli', originator: 'codex-tui' } },
    { type: 'response_item', payload: { type: 'message', id: 'manual', role: 'user', content: [{ type: 'input_text', text }], internal_chat_message_metadata_passthrough: { content_item_kinds: ['shell.user_command'] } } }]));
  await hook('codex', f.input('codex', { id, file }), f.config, opts); const p = await f.pair();
  const rows = (await fs.readFile(p.claude.file, 'utf8')).trim().split('\n').map(JSON.parse);
  const input = rows.find(row => row.message?.content === '<bash-input>echo manual</bash-input>');
  const output = rows.find(row => row.message?.content === '<bash-stdout>manual</bash-stdout><bash-stderr></bash-stderr>');
  assert(input && output); assert.equal(output.parentUuid, input.uuid); assert.equal(output.session_sync_key, input.session_sync_key);
  assert.deepEqual(extract('claude', rows, p.claude.id), []);
  const before = (await f.pair()).known;
  await hook('claude', f.input('claude', p.claude, 'SessionStart'), f.config, opts);
  assert.deepEqual((await f.pair()).known, before);
});
