/*
 * Puente ADB local para Mi Chat Admin.
 * Conexión saliente TLS a Socket.IO; no abre puertos entrantes en el PC.
 * Requiere que Android haya autorizado/emparejado manualmente este ordenador.
 */
const { spawn } = require('child_process');
const WebSocket = require('ws');
const crypto = require('crypto');

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
    const allowed = new Set(['devices','shell','install','uninstall','push','pull','logcat','reboot','get-state','get-serialno','tcpip','connect','disconnect','pair','forward','reverse','bugreport']);
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
  });
  ws.on('close', () => { connected = false; console.log('Conexión cerrada; reintentando en 5 segundos…'); reconnectTimer = setTimeout(connect, 5000); });
  ws.on('error', e => { connected = false; console.error('Conexión ADB:', e.message); });
}
process.on('SIGINT', () => { clearTimeout(reconnectTimer); if (ws) ws.close(); process.exit(0); });
connect();
