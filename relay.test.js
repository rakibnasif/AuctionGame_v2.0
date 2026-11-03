'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { server } = require('../server.js');

let port;
test.before(() => new Promise(r => server.listen(0, '127.0.0.1', () => { port = server.address().port; r(); })));
test.after(() => { server.closeAllConnections?.(); server.close(); });

const sleep = ms => new Promise(r => setTimeout(r, ms));
function client(room) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws?room=${room}`);
    const got = [];
    ws.onmessage = e => got.push(JSON.parse(e.data));
    ws.onopen = () => resolve({ ws, got, send: m => ws.send(JSON.stringify(m)) });
    ws.onerror = () => reject(new Error('connect failed'));
  });
}

test('serves the game, health and ICE config', async () => {
  const home = await fetch(`http://127.0.0.1:${port}/`);
  assert.equal(home.status, 200);
  assert.match(await home.text(), /BIDARENA/);
  assert.equal(await (await fetch(`http://127.0.0.1:${port}/healthz`)).text(), 'ok');
  const ice = await (await fetch(`http://127.0.0.1:${port}/ice`)).json();
  assert.ok(Array.isArray(ice.iceServers) && ice.iceServers.length >= 1);
  assert.equal((await fetch(`http://127.0.0.1:${port}/../server.js`)).status, 404);
});

test('welcome message reports how many peers are already in the room', async () => {
  const a = await client('WELC1'); await sleep(50);
  assert.equal(a.got[0].t, '_welcome'); assert.equal(a.got[0].peers, 0);
  const b = await client('WELC1'); await sleep(50);
  assert.equal(b.got[0].peers, 1);
  a.ws.close(); b.ws.close();
});

test('messages are relayed to everyone else in the same room only', async () => {
  const a = await client('ROOM1'), b = await client('ROOM1'), c = await client('ROOM2');
  a.send({ t: 'hello', room: 'ROOM1', from: 'A' }); b.send({ t: 'x', room: 'ROOM1', from: 'B' }); c.send({ t: 'y', room: 'ROOM2', from: 'C' });
  await sleep(100);
  assert.ok(b.got.some(m => m.t === 'hello'));
  assert.ok(a.got.some(m => m.t === 'x'));
  assert.ok(!a.got.some(m => m.t === 'hello'), 'sender does not get its own message');
  assert.ok(!a.got.some(m => m.t === 'y') && !c.got.some(m => m.t === 'hello'), 'rooms are isolated');
  [a, b, c].forEach(x => x.ws.close());
});

test('direct messages go only to the addressed player; spoofing is ignored', async () => {
  const a = await client('DIRCT'), b = await client('DIRCT'), c = await client('DIRCT');
  a.send({ t: 'hb', from: 'A' }); b.send({ t: 'hb', from: 'B' }); c.send({ t: 'hb', from: 'C' }); await sleep(80);
  a.send({ t: 'sig', to: 'B', from: 'A', d: { s: 1 } }); await sleep(80);
  assert.ok(b.got.some(m => m.t === 'sig'));
  assert.ok(!c.got.some(m => m.t === 'sig'));
  a.send({ t: 'fake', from: 'SOMEONE_ELSE' }); await sleep(80);
  assert.ok(!b.got.some(m => m.t === 'fake'));
  [a, b, c].forEach(x => x.ws.close());
});

test('a player who drops without saying goodbye is announced as left (after a grace period)', async () => {
  const a = await client('LEAVE'), b = await client('LEAVE');
  a.send({ t: 'hb', from: 'A' }); b.send({ t: 'hb', from: 'B' }); await sleep(80);
  b.ws.close();
  await sleep(4600);
  const l = a.got.find(m => m.t === 'leave');
  assert.ok(l && l.from === 'B');
  a.ws.close();
});

test('a quick reconnect cancels the leave announcement', async () => {
  const a = await client('BLIPS'), b = await client('BLIPS');
  a.send({ t: 'hb', from: 'A' }); b.send({ t: 'hb', from: 'B' }); await sleep(80);
  b.ws.close(); await sleep(500);
  const b2 = await client('BLIPS'); b2.send({ t: 'hb', from: 'B' });
  await sleep(4600);
  assert.ok(!a.got.some(m => m.t === 'leave'));
  a.ws.close(); b2.ws.close();
});

test('invalid room codes are refused', async () => {
  await assert.rejects(client('bad!'));
  await assert.rejects(client('TOOLONGCODE'));
});

test('large messages (a full game state) pass through intact', async () => {
  const a = await client('BIGGY'), b = await client('BIGGY');
  a.send({ t: 'hb', from: 'A' }); await sleep(50);
  const blob = 'x'.repeat(120000);
  a.send({ t: 'state', from: 'A', S: { blob } }); await sleep(200);
  const m = b.got.find(x => x.t === 'state');
  assert.ok(m && m.S.blob.length === 120000);
  a.ws.close(); b.ws.close();
});
