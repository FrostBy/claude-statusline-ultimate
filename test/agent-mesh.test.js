'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');
const { eventWriter, nextEvent, ack, questionResult } = require('../src/agents/mesh');
const { scratchpad, hook, hookConfig, configure } = require('../scripts/agents/mesh-hook');
const script = path.resolve(__dirname, '../scripts/agents/broker.js');
function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-mesh-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  return root;
}
const short = { timeoutMin: 0.0004, pollMs: 5 };
function cli(root, args) {
  return spawnSync(process.execPath, [script, ...args, '--state', root], { encoding: 'utf8', env: { ...process.env, BROKER_POLL_MS: '5' }, timeout: 5000 });
}

test('events wait for any agent, consume once, support idempotent ack and skip resolved requests', async t => {
  const root = fixture(t), a = eventWriter(path.join(root, 'a'), 'a');
  const pending = nextEvent(root, { timeoutMin: 1, pollMs: 5 });
  setImmediate(() => a('question', { questions: [{ id: 'q', question: 'Which?' }] }, { requestId: 'req', summary: 'Choose' }));
  const first = await pending; assert.equal(first.type, 'question'); assert.equal(first.from, 'a');
  ack(root, first.id); ack(root, first.id); assert.equal(await nextEvent(root, short), null);
  a('approval', {}, { requestId: 'stale' }); a('request.resolved', { reason: 'cancelled' }, { requestId: 'stale' });
  assert.equal(await nextEvent(root, short), null);
  const nested = path.join(root, 'codex'); fs.mkdirSync(nested);
  const b = eventWriter(path.join(nested, 'b'), 'b'); const completion = b('turn.completed', { status: 'completed' }, { summary: 'Done' });
  const printed = cli(root, ['events', '--next', '--timeout-min', '0.01']);
  assert.equal(printed.status, 0, printed.stderr); assert.match(printed.stdout, /b turn.completed: Done/); assert(printed.stdout.includes(completion.id));
  assert.equal(cli(root, ['ack', '--event', completion.id]).status, 0);
  const again = cli(root, ['events', '--next', '--timeout-min', '0.0001']); assert.equal(again.status, 1); assert.equal(again.stdout, '');
  const restarted = eventWriter(path.join(root, 'a'), 'a'); const fresh = restarted('error', { message: 'fixture error' });
  assert(fresh.seq > first.seq); assert.equal((await nextEvent(root, short)).id, fresh.id);
});

test('one waiter owns DIR; cancellation releases the lock and dead CLI owners are reclaimed', async t => {
  const root = fixture(t), abort = new AbortController();
  const waiting = nextEvent(root, { timeoutMin: 1, pollMs: 10, signal: abort.signal });
  await assert.rejects(nextEvent(root, short), { code: 'EVENTS_LOCKED' });
  const locked = cli(root, ['events', '--next']); assert.equal(locked.status, 1); assert.match(locked.stderr, /already owns/);
  abort.abort(); await assert.rejects(waiting, { name: 'AbortError' });
  assert(!fs.existsSync(path.join(root, '.mesh-events.lock')));
  const dead = spawnSync(process.execPath, ['-e', 'process.stdout.write(String(process.pid))'], { encoding: 'utf8' });
  fs.writeFileSync(path.join(root, '.mesh-events.lock'), dead.stdout);
  assert.equal(await nextEvent(root, short), null);
});

test('broker restart invalidates old request IDs without resetting the event sequence', async t => {
  const root = fixture(t), base = path.join(root, 'worker');
  const first = eventWriter(base, 'worker')('question', {}, { requestId: 'old-epoch' });
  const emit = eventWriter(base, 'worker');
  assert.equal(await nextEvent(root, short), null);
  const second = emit('question', {}, { requestId: 'new-epoch' });
  assert(second.seq > first.seq); assert.notEqual(second.id, first.id);
  assert.equal((await nextEvent(root, short)).requestId, 'new-epoch');
});

test('typed inbox CLI validates input and writes atomic commands without changing legacy send', t => {
  const root = fixture(t); fs.writeFileSync(path.join(root, 'worker.state.json'), JSON.stringify({ agent: 'codex', pid: process.pid, status: 'running' }));
  const file = path.join(root, 'answers.json'); fs.writeFileSync(file, JSON.stringify({ answers: { q: { answers: ['yes'] } } }));
  for (const args of [['answer', '--request', 'req', '--text', 'yes'], ['answer', '--request', 'req', '--file', file], ['approve', '--request', 'req'], ['deny', '--request', 'req'], ['interrupt'], ['exit']]) {
    const result = cli(root, [...args, '--name', 'worker']); assert.equal(result.status, 0, result.stderr); assert.match(result.stdout, /^queued /);
  }
  const commands = fs.readdirSync(path.join(root, 'worker.inbox')).map(f => {
    assert(f.endsWith('.command.json')); assert(!f.startsWith('.')); return JSON.parse(fs.readFileSync(path.join(root, 'worker.inbox', f)));
  });
  assert.equal(commands.length, 6); assert.equal(new Set(commands.map(c => c.id)).size, 6);
  assert(commands.some(c => c.type === 'answer' && c.payload.result?.answers.q.answers[0] === 'yes'));
  for (const args of [['answer', '--name', 'worker', '--request', 'req'], ['approve', '--name', 'worker'], ['interrupt', '--name', '../escape'], ['events', '--next', '--timeout-min', 'NaN']]) {
    assert.equal(cli(root, args).status, 1);
  }
});

test('answers cover every question and reject invalid question IDs/options', () => {
  const params = { questions: [{ id: 'a', options: [{ label: 'yes' }] }, { id: 'b', options: null }] };
  assert.throws(() => questionResult(params, { text: 'yes' }), /multiple/);
  assert.throws(() => questionResult(params, { result: { answers: { a: { answers: ['yes'] }, c: { answers: ['x'] } } } }), /Invalid answer for b/);
  const result = { answers: { a: { answers: ['yes'] }, b: { answers: ['free text'] } } };
  assert.deepEqual(questionResult(params, { result }), result);
});

test('hook derives session scratchpad from transcript or cwd, skips subagents, and exits 2 only once per event', async t => {
  const tmp = fixture(t), input = { session_id: 'session-1', cwd: 'Z:\\project', hook_event_name: 'SessionStart', transcript_path: 'C:\\profile\\projects\\Z--project\\session-1.jsonl' };
  const dir = scratchpad(input, tmp); assert.equal(dir, path.join(tmp, 'claude', 'Z--project', 'session-1', 'scratchpad'));
  assert.equal(scratchpad({ ...input, transcript_path: undefined }, tmp), path.join(tmp, 'claude', 'z--project', 'session-1', 'scratchpad'));
  assert.equal(scratchpad({ ...input, agent_id: 'sub' }, tmp), null);
  assert.throws(() => scratchpad({ ...input, session_id: '../escape' }, tmp), /session_id/);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'worker.state.json'), JSON.stringify({ status: 'running', pid: process.pid }));
  eventWriter(path.join(dir, 'worker'), 'worker')('question', { questions: [{ id: 'q', question: 'Which?' }] }, { requestId: 'req', summary: 'Choose' });
  const env = { ...process.env, TEMP: tmp, TMP: tmp, TMPDIR: tmp, SESSION_SYNC_BROKER: '' };
  const result = spawnSync(process.execPath, [path.resolve(__dirname, '../scripts/agents/mesh-hook.js')], { input: JSON.stringify(input), env, encoding: 'utf8', timeout: 5000 });
  assert.equal(result.status, 2, result.stderr); assert.match(result.stderr, /worker question requestId=req/); assert.match(result.stderr, /answer .*--request req/);
  assert.equal(await nextEvent(dir, short), null);
  const abort = new AbortController();
  const waiting = hook({ ...input, hook_event_name: 'Stop' }, { env: {}, tmp, timeoutMin: 1, signal: abort.signal, pollMs: 5 });
  assert.equal(await hook(input, { env: {}, tmp, timeoutMin: 1 }), 0);
  abort.abort(); await assert.rejects(waiting, { name: 'AbortError' });
  assert.equal(await hook({ ...input, agent_id: 'sub' }, { env: {}, tmp }), 0);
  assert.equal(await hook(input, { env: { SESSION_SYNC_BROKER: '1' }, tmp }), 0);
  fs.writeFileSync(path.join(dir, 'worker.state.json'), JSON.stringify({ status: 'done', pid: process.pid }));
  assert.equal(await hook(input, { env: {}, tmp, ...short, write() {} }), 0);
  fs.writeFileSync(path.join(dir, 'worker.state.json'), JSON.stringify({ status: 'running', pid: process.pid }));
  const writes = []; assert.equal(await hook(input, { env: {}, tmp, ...short, write: text => writes.push(text) }), 2);
  assert.match(writes[0], /deadline.*Stop rearms/);
  eventWriter(path.join(dir, 'other'), 'other')('turn.completed', { status: 'completed' }, { summary: 'Second event' });
  assert.equal(await hook({ ...input, hook_event_name: 'Stop' }, { env: {}, tmp, ...short, write() {} }), 2);
});

test('hook exits immediately without live brokers, including an absent scratchpad', async t => {
  const tmp = fixture(t), input = { session_id: 'empty-session', cwd: tmp, hook_event_name: 'Stop' };
  const dir = scratchpad(input, tmp), signal = AbortSignal.abort();
  const options = { env: {}, tmp, signal, write() { assert.fail('No wake without a live broker'); } };
  assert.equal(await hook(input, options), 0); assert(!fs.existsSync(dir));
  fs.mkdirSync(dir, { recursive: true });
  eventWriter(path.join(dir, 'worker'), 'worker')('turn.completed', { status: 'completed' });
  for (const state of [{ status: 'done', pid: process.pid }, { status: 'error', pid: process.pid }, { status: 'running', pid: 0 }]) {
    fs.writeFileSync(path.join(dir, 'worker.state.json'), JSON.stringify(state));
    assert.equal(await hook(input, options), 0);
    assert(!fs.existsSync(path.join(dir, '.mesh-events.lock')));
    assert(!fs.existsSync(path.join(dir, '.mesh-events.cursor.json')));
  }
});

test('hook waits 235 minutes by default, leaving five minutes before its configured timeout', async () => {
  const file = path.resolve(__dirname, '../scripts/agents/mesh-hook.js');
  const localRequire = require('node:module').createRequire(file), exported = { exports: {} };
  let waitOptions;
  require('node:vm').runInNewContext(fs.readFileSync(file, 'utf8'), {
    module: exported, __filename: file, process,
    require: name => name === '../../src/agents/mesh' ? {
      hasLiveBroker: () => true, nextEvent: async (dir, options) => { waitOptions = options; return null; }
    } : localRequire(name)
  });
  assert.equal(await exported.exports.hook({ session_id: 'session', cwd: os.tmpdir(), hook_event_name: 'Stop' }, { env: {}, write() {} }), 2);
  assert.equal(waitOptions.timeoutMin, 235);
  for (const event of ['SessionStart', 'Stop']) {
    const groups = hookConfig().hooks[event];
    assert.equal(groups[0].hooks[0].timeout, 14400);
    assert.equal(groups[0].hooks[0].timeout - waitOptions.timeoutMin * 60, 300);
  }
  const end = hookConfig().hooks.SessionEnd[0].hooks[0];
  assert.equal(end.timeout, 5); assert.equal(end.asyncRewake, undefined); assert.equal(end.async, undefined);
});

test('mesh hook installation in a fake profile is idempotent, backs up, and preserves foreign hooks', t => {
  const root = fixture(t), file = path.join(root, 'settings.json'), env = { CLAUDE_CONFIG_DIR: root };
  const foreign = { type: 'command', command: 'echo foreign' };
  const before = { model: 'fixture', hooks: { Stop: [{ matcher: '*', hooks: [foreign] }], UserPromptSubmit: [{ hooks: [foreign] }] } };
  fs.writeFileSync(file, JSON.stringify(before));
  assert.equal(configure({ env, log() {} }), 1); const first = fs.readFileSync(file, 'utf8');
  assert.equal(configure({ env, log() {} }), 0); assert.equal(fs.readFileSync(file, 'utf8'), first);
  assert.equal(fs.readdirSync(root).filter(f => f.includes('backup')).length, 1);
  const settings = JSON.parse(first); assert.equal(settings.hooks.Stop.length, 2);
  assert.deepEqual(settings.hooks.SessionStart, hookConfig().hooks.SessionStart);
  assert.deepEqual(settings.hooks.SessionEnd, hookConfig().hooks.SessionEnd);
  settings.hooks.Stop.push({ hooks: [{ type: 'command', command: 'echo added later' }] }); fs.writeFileSync(file, JSON.stringify(settings));
  assert.equal(configure({ env, remove: true, log() {} }), 1); assert.equal(configure({ env, remove: true, log() {} }), 0);
  const after = JSON.parse(fs.readFileSync(file, 'utf8')); assert.equal(after.model, 'fixture'); assert.equal(after.hooks.Stop.length, 2); assert(!after.hooks.SessionStart);
  assert(!after.hooks.SessionEnd);
  assert.deepEqual(after.hooks.UserPromptSubmit, before.hooks.UserPromptSubmit);
  fs.writeFileSync(file, '{bad'); assert.throws(() => configure({ env, log() {} })); assert.equal(fs.readFileSync(file, 'utf8'), '{bad');
});

test('idle and normal exits stay in events without waking Claude; errors and unexpected death wake once', async t => {
  const tmp = fixture(t), input = { session_id: 'lifecycle', cwd: tmp, hook_event_name: 'Stop' };
  const dir = scratchpad(input, tmp); fs.mkdirSync(dir, { recursive: true });
  const base = path.join(dir, 'worker'), file = `${base}.state.json`, emit = eventWriter(base, 'worker');
  for (const reason of ['idle', 'exit', 'owner', 'error', 'unexpected']) {
    fs.writeFileSync(file, JSON.stringify({ pid: process.pid, status: 'idle' }));
    const writes = [];
    const waiting = hook(input, { tmp, env: {}, timeoutMin: 1, pollMs: 5, write: text => writes.push(text) });
    if (reason === 'unexpected') fs.writeFileSync(file, JSON.stringify({ pid: 999999999, status: 'running' }));
    else {
      emit('agent.exited', { reason, pid: process.pid, code: reason === 'error' ? 1 : 0 });
      fs.writeFileSync(file, JSON.stringify({ pid: process.pid, status: 'done' }));
    }
    assert.equal(await waiting, ['error', 'unexpected'].includes(reason) ? 2 : 0);
    assert.equal(writes.length, ['error', 'unexpected'].includes(reason) ? 1 : 0);
  }
  const count = fs.readFileSync(`${base}.events.jsonl`, 'utf8').trim().split('\n').length;
  assert.equal(await hook(input, { tmp, env: {}, ...short, write() { assert.fail('Duplicate death'); } }), 0);
  assert.equal(fs.readFileSync(`${base}.events.jsonl`, 'utf8').trim().split('\n').length, count);
});

test('SessionStart quietly retires old dead brokers; a death after it wakes Stop once', async t => {
  const tmp = fixture(t), input = { session_id: 'old-death', cwd: tmp, hook_event_name: 'SessionStart' };
  const dir = scratchpad(input, tmp); fs.mkdirSync(dir, { recursive: true });
  const old = new Date(Date.now() - 86400000);
  for (const metadata of [{}, { lastTurnAt: old.toISOString() }, { updatedAt: old.toISOString() }]) {
    const name = `old-${Object.keys(metadata)[0] || 'mtime'}`, file = path.join(dir, `${name}.state.json`);
    fs.writeFileSync(file, JSON.stringify({ pid: 999999999, status: 'running', ...metadata }));
    fs.utimesSync(file, old, old);
  }
  // A death event an older hook already recorded is retired too.
  fs.writeFileSync(path.join(dir, 'recorded.state.json'), JSON.stringify({ pid: 999999998, status: 'done' }));
  fs.writeFileSync(path.join(dir, 'recorded.events.jsonl'), JSON.stringify({ v: 1, id: 'recorded:1', seq: 1, at: old.toISOString(), from: 'recorded',
    type: 'agent.exited', summary: 'Broker process died unexpectedly', payload: { reason: 'unexpected', pid: 999999998 } }) + '\n');
  const options = { tmp, env: {}, ...short, write() { assert.fail('Old broker must not wake Claude'); } };
  assert.equal(await hook(input, options), 0);
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, '.mesh-events.cursor.json'), 'utf8'))['recorded.events.jsonl'], 1);
  for (const file of fs.readdirSync(dir).filter(f => f.endsWith('.state.json'))) {
    assert.equal(JSON.parse(fs.readFileSync(path.join(dir, file))).status, 'done');
  }
  assert.deepEqual(fs.readdirSync(dir).filter(f => f.endsWith('.events.jsonl')), ['recorded.events.jsonl']);
  assert.equal(await hook({ ...input, hook_event_name: 'Stop' }, options), 0);

  const file = path.join(dir, 'fresh.state.json');
  fs.writeFileSync(file, JSON.stringify({ pid: 999999999, status: 'running', lastTurnAt: old.toISOString() }));
  // Make the fresh state unambiguously newer than SessionStart even on coarse filesystems.
  const fresh = new Date(Date.now() + 1000); fs.utimesSync(file, fresh, fresh);
  const writes = [];
  assert.equal(await hook({ ...input, hook_event_name: 'Stop' }, { ...options, write: text => writes.push(text) }), 2);
  assert.match(writes[0], /agent.exited.*unexpectedly/);
  assert.equal(await hook({ ...input, hook_event_name: 'Stop' }, options), 0);
});

test('a broker observed alive before a waiter returns still wakes on death between hooks', async t => {
  const tmp = fixture(t), input = { session_id: 'between-hooks', cwd: tmp, hook_event_name: 'SessionStart' };
  const dir = scratchpad(input, tmp); fs.mkdirSync(dir, { recursive: true });
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore', windowsHide: true });
  t.after(() => child.kill());
  const file = path.join(dir, 'worker.state.json');
  fs.writeFileSync(file, JSON.stringify({ pid: child.pid, status: 'idle' }));
  const old = new Date(Date.now() - 86400000); fs.utimesSync(file, old, old);
  const options = { tmp, env: {}, ...short, write() {} };
  assert.equal(await hook(input, options), 2);
  const exited = new Promise(resolve => child.once('exit', resolve)); child.kill(); await exited;
  const writes = [];
  assert.equal(await hook({ ...input, hook_event_name: 'Stop' }, { ...options, write: text => writes.push(text) }), 2);
  assert.match(writes[0], /agent.exited.*unexpectedly/);
  assert.equal(await hook({ ...input, hook_event_name: 'Stop' }, options), 0);
});

test('SessionEnd queues exit only for live brokers in this scratchpad and its child directories', async t => {
  const tmp = fixture(t), input = { session_id: 'ending', cwd: tmp, hook_event_name: 'SessionEnd' }, dir = scratchpad(input, tmp);
  const nested = path.join(dir, 'agents'); fs.mkdirSync(nested, { recursive: true });
  const other = scratchpad({ ...input, session_id: 'other' }, tmp); fs.mkdirSync(other, { recursive: true });
  const bases = [path.join(dir, 'a'), path.join(nested, 'b'), path.join(dir, 'dead'), path.join(other, 'foreign')];
  for (const [i, base] of bases.entries()) fs.writeFileSync(`${base}.state.json`, JSON.stringify({ status: 'running', pid: i === 2 ? 999999999 : process.pid }));
  assert.equal(await hook(input, { tmp, env: {}, write() { assert.fail('SessionEnd must not wake'); } }), 0);
  for (const base of bases.slice(0, 2)) {
    const files = fs.readdirSync(`${base}.inbox`); assert.equal(files.length, 1);
    assert.equal(fs.readFileSync(path.join(`${base}.inbox`, files[0]), 'utf8'), '/exit');
  }
  for (const base of bases.slice(2)) assert(!fs.existsSync(`${base}.inbox`));
});
