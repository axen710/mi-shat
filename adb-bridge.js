/*
 * Puente ADB local para Mi Chat Admin.
 * Conexión saliente TLS a Socket.IO; no abre puertos entrantes en el PC.
 * Requiere que Android haya autorizado/emparejado manualmente este ordenador.
 */
const { spawn } = require('child_process');
const WebSocket = require('ws');
const crypto = require('crypto');
const QRCode = require('qrcode');

const SERVER_URL = String(process.env.MICHAT_SERVER_URL || '').replace(/\/$/, '');
const ADMIN_TOKEN = String(process.env.MICHAT_ADMIN_TOKEN || '');
const ADB_BIN = String(process.env.ADB_PATH || 'adb');
if (!/^https:\/\//i.test(SERVER_URL) || !ADMIN_TOKEN) {
  console.error('Configura MICHAT_SERVER_URL (https://tu-servidor) y MICHAT_ADMIN_TOKEN.');
  process.exit(1);
}

let ws, connected = false, reconnectTimer;
const pending = new Map();
function sendEvent(name, data) {
  if (ws && ws.readyState === WebSocket.OPEN && connected) ws.send('42' + JSON.stringify([name, data]));
}
function parseArgs(input) {
  const out = []; let cur = '', quote = '', escaped = false;
  for (let i = 0; i < input.length; i++) {
    const ch = input[i], next = input[i + 1];
    if (escaped) { cur += ch; escaped = false; continue; }
    if (ch === '\\' && (next === '\\' || next === '"' || next === "'" || (!quote && /\s/.test(next || '')))) { escaped = true; continue; }
    if (quote) { if (ch === quote) quote = ''; else cur += ch; continue; }
    if (ch === '"' || ch === "'") { quote = ch; continue; }
    if (/\s/.test(ch)) { if (cur) { out.push(cur); cur = ''; } continue; }
    cur += ch;
  }
  if (escaped) cur += '\\';
  if (quote) throw new Error('Comillas sin cerrar.');
  if (cur) out.push(cur);
  return out;
}
function runAdb(command, serial) {
  return new Promise(resolve => {
    let args;
    try { args = parseArgs(command); } catch (e) { return resolve({ code: 2, stderr: e.message }); }
    if (!args.length) return resolve({ code: 2, stderr: 'Comando vacío.' });
    const allowed = new Set(['devices','shell','install','uninstall','push','pull','logcat','reboot','get-state','get-serialno','tcpip','connect','disconnect','pair','forward','reverse','bugreport','mdns']);
    if (!allowed.has(args[0])) return resolve({ code: 2, stderr: 'Comando ADB no permitido: ' + args[0] });
    if (args[0] !== 'devices' && args[0] !== 'pair' && args[0] !== 'connect' && args[0] !== 'disconnect' && !serial) {
      return resolve({ code: 2, stderr: 'Selecciona primero un dispositivo autorizado.' });
    }
    if (serial && !['devices','pair','connect','disconnect'].includes(args[0])) args = ['-s', serial, ...args];
    // adb shell recibe el resto como una única cadena para conservar sus argumentos.
    if (args[0] === 'shell' && args.length > 2) args = [...args.slice(0, 1), args.slice(1).join(' ')];
    const child = spawn(ADB_BIN, args, { windowsHide: true, shell: false });
    let stdout = '', stderr = '', settled = false;
    const finish = result => { if (settled) return; settled = true; clearTimeout(timer); resolve(result); };
    const timer = setTimeout(() => { child.kill(); finish({ code: 124, stdout, stderr: stderr + '\nTiempo máximo de ejecución alcanzado (120 s).' }); }, 120000);
    child.stdout.on('data', b => { stdout = (stdout + b.toString()).slice(-30000); });
    child.stderr.on('data', b => { stderr = (stderr + b.toString()).slice(-12000); });
    child.on('error', e => finish({ code: 127, stdout, stderr, error: e.message + ' Comprueba que ADB esté instalado y en PATH.' }));
    child.on('close', code => finish({ code: Number.isInteger(code) ? code : -1, stdout, stderr }));
  });
}
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
async function adbText(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(ADB_BIN, args, { windowsHide: true, shell: false });
    let stdout = '', stderr = '', done = false;
    const timer = setTimeout(() => { child.kill(); if (!done) { done = true; reject(new Error('Tiempo de espera agotado ejecutando adb ' + args.join(' '))); } }, 15000);
    child.stdout.on('data', b => stdout += b.toString());
    child.stderr.on('data', b => stderr += b.toString());
    child.on('error', e => { if (!done) { done = true; clearTimeout(timer); reject(new Error(e.message + ' Comprueba ADB en PATH.')); } });
    child.on('close', code => { if (!done) { done = true; clearTimeout(timer); resolve({ code: Number.isInteger(code) ? code : -1, stdout, stderr }); } });
  });
}
function mdnsEntries(output, serviceType) {
  return String(output || '').split(/\r?\n/).map(line => {
    const m = line.match(/^\s*(\S+)\s+(_adb-tls-(?:pairing|connect)\._tcp)\s+(\S+)(?:\s|$)/i);
    return m ? { name: m[1], type: m[2], endpoint: m[3] } : null;
  }).filter(x => x && x.type.toLowerCase() === serviceType.toLowerCase());
}
async function waitForMdns(name, type, timeoutMs, preferredHost = '') {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    const r = await adbText(['mdns', 'services']);
    const found = mdnsEntries(r.stdout + '\n' + r.stderr, type).find(x => x.name.includes(name) && (!preferredHost || x.endpoint.split(':')[0] === preferredHost));
    if (found) return found;
    await delay(1500);
  }
  throw new Error('No se detectó el servicio de emparejamiento por Wi-Fi. Comprueba que el móvil y el PC estén en la misma red y vuelve a escanear el QR.');
}
async function pairUsingQr(requestId) {
  const suffix = crypto.randomBytes(8).toString('base64url').replace(/[-_]/g, 'A').slice(0, 10);
  const serviceName = 'studio-' + suffix;
  const password = crypto.randomBytes(8).toString('base64url').replace(/[-_]/g, 'B').slice(0, 10);
  const payload = `WIFI:T:ADB;S:${serviceName};P:${password};;`;
  const qrDataUrl = await QRCode.toDataURL(payload, { errorCorrectionLevel: 'M', margin: 2, width: 300 });
  sendEvent('adbBridgeQrStatus', { requestId, status: 'ready', qrDataUrl, message: 'QR listo. En Android abre Depuración inalámbrica → Vincular dispositivo con código QR y escanéalo.' });
  console.log('Esperando que el móvil escanee el QR de vinculación…');
  const pairing = await waitForMdns(serviceName, '_adb-tls-pairing._tcp', 120000);
  sendEvent('adbBridgeQrStatus', { requestId, status: 'pairing', message: 'Móvil detectado. Completando vinculación segura…' });
  const pairEndpoint = pairing.endpoint;
  const pair = await adbText(['pair', pairEndpoint, password]);
  if (pair.code !== 0 || !/successfully paired|already paired/i.test(pair.stdout + pair.stderr)) {
    throw new Error((pair.stdout + '\n' + pair.stderr).trim() || 'ADB no pudo completar el emparejamiento.');
  }
  sendEvent('adbBridgeQrStatus', { requestId, status: 'connecting', message: 'Vinculación correcta. Buscando el puerto de depuración…' });
  const pairingHost = pairEndpoint.replace(/^\[|\]:\d+$|:\d+$/g, '');
  const connectService = await waitForMdns('', '_adb-tls-connect._tcp', 45000, pairingHost).catch(async () => {
    // ADB puede no listar el nombre del dispositivo de inmediato; se vuelve a consultar.
    throw new Error('El móvil se vinculó, pero no apareció el servicio de conexión. Deja activada la depuración inalámbrica y vuelve a intentarlo.');
  });
  const connect = await adbText(['connect', connectService.endpoint]);
  if (connect.code !== 0 || /unable|failed|cannot/i.test(connect.stdout + connect.stderr)) {
    throw new Error((connect.stdout + '\n' + connect.stderr).trim() || 'No se pudo conectar al móvil después de vincularlo.');
  }
  const devices = await adbText(['devices', '-l']);
  sendEvent('adbBridgeQrStatus', { requestId, status: 'success', message: 'Móvil vinculado y conectado.', stdout: devices.stdout, stderr: devices.stderr, code: devices.code });
}

function connect() {
  clearTimeout(reconnectTimer);
  const endpoint = SERVER_URL.replace(/^http/i, 'ws') + '/socket.io/?EIO=4&transport=websocket';
  console.log('Conectando al servidor Mi Chat…');
  ws = new WebSocket(endpoint, { handshakeTimeout: 15000 });
  ws.on('message', async raw => {
    const msg = raw.toString();
    if (msg.startsWith('0')) { ws.send('40'); return; }
    if (msg === '2') { ws.send('3'); return; }
    if (msg.startsWith('40')) {
      connected = true;
      sendEvent('adbBridgeAuthenticate', ADMIN_TOKEN);
      console.log('Canal seguro establecido; autenticando puente ADB.');
      return;
    }
    if (!msg.startsWith('42')) return;
    let packet; try { packet = JSON.parse(msg.slice(2)); } catch { return; }
    const [event, data] = packet || [];
    if (event === 'adbBridgeAuthenticated') { console.log('Puente ADB autenticado. Mantén esta ventana abierta.'); return; }
    if (event === 'adbBridgeAuthenticationError') { console.error(data || 'Autenticación rechazada.'); ws.close(); return; }
    if (event === 'adbBridgeCommand') {
      const requestId = String(data?.requestId || '');
      if (!requestId) return;
      console.log('Orden ADB recibida:', String(data.command || '').slice(0, 120));
      const result = await runAdb(String(data.command || ''), String(data.serial || ''));
      sendEvent('adbBridgeResult', { requestId, ...result });
    }
    if (event === 'adbBridgeQrPair') {
      const requestId = String(data?.requestId || '');
      if (!requestId) return;
      pairUsingQr(requestId).catch(error => sendEvent('adbBridgeQrStatus', { requestId, status: 'error', message: error.message }));
    }
  });
  ws.on('close', () => { connected = false; console.log('Conexión cerrada; reintentando en 5 segundos…'); reconnectTimer = setTimeout(connect, 5000); });
  ws.on('error', e => { connected = false; console.error('Conexión ADB:', e.message); });
}
process.on('SIGINT', () => { clearTimeout(reconnectTimer); if (ws) ws.close(); process.exit(0); });
connect();
