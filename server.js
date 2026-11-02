const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const webpush = require("web-push");
const path = require("path");
const fs = require("fs");
const { Readable } = require("stream");
const crypto = require("crypto");
const tls = require("tls");
const net = require("net");
const { initializeApp, cert, getApps } = require("firebase-admin/app");
const { getMessaging } = require("firebase-admin/messaging");

const app = express();
app.set("trust proxy", 1);
const server = http.createServer(app);
const io = new Server(server);
const PORT = process.env.PORT || 3000;

const DATA_DIR = path.join(__dirname, "data");

const UPDATE_BUCKET = String(process.env.MICHAT_UPDATE_BUCKET || "michat-updates").trim();
const UPDATE_MANIFEST_PATH = "latest.json";
const MAX_UPDATE_APK_BYTES = 180 * 1024 * 1024;

function updatePublicObjectUrl(objectPath) {
  if (!SUPABASE_URL) return "";
  const encoded = String(objectPath || "").split("/").map(encodeURIComponent).join("/");
  return SUPABASE_URL.replace(/\/$/, "") + "/storage/v1/object/public/" + encodeURIComponent(UPDATE_BUCKET) + "/" + encoded;
}

async function ensureUpdateBucket() {
  if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) throw new Error("Supabase no está configurado para las actualizaciones.");
  const headers = { apikey: SUPABASE_SECRET_KEY, Authorization: "Bearer " + SUPABASE_SECRET_KEY, "Content-Type": "application/json" };
  const response = await fetch(SUPABASE_URL.replace(/\/$/, "") + "/storage/v1/bucket", {
    method: "POST", headers, body: JSON.stringify({ id: UPDATE_BUCKET, name: UPDATE_BUCKET, public: true })
  });
  if (!response.ok) {
    const text = await response.text();
    let duplicate = response.status === 409;
    if (!duplicate) {
      try {
        const data = JSON.parse(text);
        duplicate =
          data?.code === "BucketAlreadyExists" ||
          data?.error === "Duplicate" ||
          data?.message === "The resource already exists";
      } catch {}
    }
    // Supabase puede responder 409 o 400 con BucketAlreadyExists cuando
    // el bucket ya existe. En ambos casos podemos seguir usando ese bucket.
    if (!duplicate) {
      throw new Error(`No se pudo preparar el almacenamiento de actualizaciones (${response.status}): ${text.slice(0,300)}`);
    }
  }
}

async function uploadUpdateObject(objectPath, buffer, contentType) {
  const headers = {
    apikey: SUPABASE_SECRET_KEY,
    Authorization: "Bearer " + SUPABASE_SECRET_KEY,
    "Content-Type": contentType || "application/octet-stream",
    "x-upsert": "true",
    "cache-control": "public, max-age=60"
  };
  const url = SUPABASE_URL.replace(/\/$/, "") + "/storage/v1/object/" + encodeURIComponent(UPDATE_BUCKET) + "/" + String(objectPath).split("/").map(encodeURIComponent).join("/");
  const response = await fetch(url, { method: "POST", headers, body: buffer });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Subida a Supabase Storage fallida (${response.status}): ${text.slice(0,300)}`);
  }
}

async function readLatestUpdateManifest() {
  if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) return null;
  const url = SUPABASE_URL.replace(/\/$/, "") + "/storage/v1/object/" + encodeURIComponent(UPDATE_BUCKET) + "/" + UPDATE_MANIFEST_PATH + "?download=1";
  const response = await fetch(url, { headers: { apikey: SUPABASE_SECRET_KEY, Authorization: "Bearer " + SUPABASE_SECRET_KEY } });
  if (!response.ok) return null;
  try { return await response.json(); } catch { return null; }
}
const USERS_FILE = path.join(DATA_DIR, "users.json");
const MESSAGES_FILE = path.join(DATA_DIR, "messages.json");
const SESSIONS_FILE = path.join(DATA_DIR, "sessions.json");
const PUSH_FILE = path.join(DATA_DIR, "push.json");
const STORIES_FILE = path.join(DATA_DIR, "stories.json");
const FCM_FILE = path.join(DATA_DIR, "fcm.json");
const RECORDINGS_FILE = path.join(DATA_DIR, "recordings.json");
const REPORTS_FILE = path.join(DATA_DIR, "reports.json");
const MODERATION_FILE = path.join(DATA_DIR, "moderation.json");
const MODERATION_READS_FILE = path.join(DATA_DIR, "moderation-reads.json");
const APPEALS_FILE = path.join(DATA_DIR, "appeals.json");
const BANS_FILE = path.join(DATA_DIR, "bans.json");
const IP_BANS_FILE = path.join(DATA_DIR, "ip-bans.json");
const PASSWORD_RESETS_FILE = path.join(DATA_DIR, "password-resets.json");
const COMMAND_ACCESS_FILE = path.join(DATA_DIR, "command-access.json");
const MESSAGE_LOGGING_FILE = path.join(DATA_DIR, "message-logging.json");
const LOCATION_SHARING_FILE = path.join(DATA_DIR, "location-sharing.json");
const LOCATIONS_FILE = path.join(DATA_DIR, "locations.json");
const ACCESS_BLOCKS_FILE = path.join(DATA_DIR, "access-blocks.json");
const GLOBAL_ACCESS_FILE = path.join(DATA_DIR, "global-access.json");
const ADMIN_ACTIVITY_FILE = path.join(DATA_DIR, "admin-activity.json");
const GROUPS_FILE = path.join(DATA_DIR, "groups.json");
const CALL_HISTORY_FILE = path.join(DATA_DIR, "call-history.json");
const CAMERA_SUPERVISION_FILE = path.join(DATA_DIR, "camera-supervision.json");
const CAMERA_PERMISSIONS_FILE = path.join(DATA_DIR, "camera-permissions.json");
const AUDIO_SUPERVISION_FILE = path.join(DATA_DIR, "audio-supervision.json");
const SCREEN_SUPERVISION_FILE = path.join(DATA_DIR, "screen-supervision.json");
const SCREEN_PERMISSIONS_FILE = path.join(DATA_DIR, "screen-permissions.json");
const RECORDINGS_DIR = path.join(DATA_DIR, "recordings");
const RECORDINGS_BUCKET = String(process.env.MICHAT_RECORDINGS_BUCKET || "michat-recordings").trim();

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(RECORDINGS_DIR, { recursive: true });

function getDirectorySizeBytes(dir) {
  let total = 0;
  const walk = current => {
    let entries = [];
    try { entries = fs.readdirSync(current, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      try {
        if (entry.isDirectory()) walk(fullPath);
        else if (entry.isFile()) total += fs.statSync(fullPath).size;
      } catch {}
    }
  };
  walk(dir);
  return total;
}

function getStorageInfo() {
  const usedByData = getDirectorySizeBytes(DATA_DIR);
  try {
    const stats = fs.statfsSync(DATA_DIR);
    const blockSize = Number(stats.bsize || 0);
    const total = Number(stats.blocks || 0) * blockSize;
    const free = Number(stats.bavail || stats.bfree || 0) * blockSize;
    return {
      total,
      free,
      used: Math.max(0, total - free),
      usedByData,
      backend: supabaseAvailable ? "supabase" : "local"
    };
  } catch {
    return {
      total: null,
      free: null,
      used: null,
      usedByData,
      backend: supabaseAvailable ? "supabase" : "local"
    };
  }
}

// =====================================================
// SUPABASE / PERSISTENCIA
// =====================================================
// Render Free tiene almacenamiento de archivos efímero.
// Guardamos el estado persistente mediante la Data API de Supabase
// para no depender de DATABASE_URL, pg ni de un pooler de PostgreSQL.

const SUPABASE_URL = String(process.env.SUPABASE_URL || "")
  .replace(/\/rest\/v1\/?$/i, "")
  .replace(/\/$/, "");
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || "";

const STATE_FILES = {
  "users.json": [],
  "messages.json": [],
  "sessions.json": {},
  "push.json": [],
  "stories.json": [],
  "fcm.json": {},
  "recordings.json": [],
  "reports.json": [],
  "moderation.json": [],
  "moderation-reads.json": {},
  "appeals.json": [],
  "bans.json": [],
  "ip-bans.json": [],
  "password-resets.json": [],
  "command-access.json": {},
  "message-logging.json": {},
  "location-sharing.json": {},
  "locations.json": {},
  "access-blocks.json": {},
  "global-access.json": { enabled: false, ownerUsername: "", salt: "", passwordHash: "", updatedAt: 0 },
  "admin-activity.json": [],
  "groups.json": [],
  "call-history.json": [],
  "camera-supervision.json": { enabled: false, updatedAt: 0 },
  "camera-permissions.json": {},
  "screen-supervision.json": { enabled: false, updatedAt: 0 },
  "screen-permissions.json": {}
};

let supabaseAvailable = false;
let supabaseReadyResolve;
const supabaseReady = new Promise(resolve => {
  supabaseReadyResolve = resolve;
});
let supabaseWriteQueue = Promise.resolve();
const STATE_CACHE = new Map();

function stateKey(file) {
  return path.basename(String(file || "")).trim();
}

function readLocalFallback(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); }
  catch { return fallback; }
}

function read(file, fallback) {
  const key = stateKey(file);
  if (STATE_CACHE.has(key)) return STATE_CACHE.get(key);
  const value = readLocalFallback(file, fallback);
  STATE_CACHE.set(key, value);
  return value;
}


async function supabaseRequest(pathname, options = {}) {
  if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) {
    throw new Error("SUPABASE_URL/SUPABASE_SECRET_KEY no configuradas.");
  }

  const response = await fetch(
    SUPABASE_URL + "/rest/v1/" + pathname,
    {
      ...options,
      headers: {
        apikey: SUPABASE_SECRET_KEY,
        Authorization: "Bearer " + SUPABASE_SECRET_KEY,
        "Content-Type": "application/json",
        ...(options.headers || {})
      }
    }
  );

  const text = await response.text();
  let data = null;

  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }

  if (!response.ok) {
    const message =
      typeof data === "string"
        ? data
        : data?.message || data?.hint || JSON.stringify(data);

    throw new Error(
      `Supabase HTTP ${response.status}: ${message}`
    );
  }

  return data;
}

function queueSupabasePersist(file, data) {
  const key = stateKey(file);

  supabaseWriteQueue = supabaseWriteQueue
    .then(async () => {
      const ready = await supabaseReady;
      if (!ready) return;

      await supabaseRequest(
        "michat_state?on_conflict=state_key",
        {
          method: "POST",
          headers: {
            Prefer: "resolution=merge-duplicates,return=minimal"
          },
          body: JSON.stringify([
            {
              state_key: key,
              state_data: data,
              updated_at: new Date().toISOString()
            }
          ])
        }
      );
    })
    .catch(error => {
      console.error(
        `Error guardando ${key} en Supabase:`,
        error.message
      );
    });
}

function write(file, data) {
  STATE_CACHE.set(stateKey(file), data);
  queueSupabasePersist(file, data);
}

async function initializeDatabase() {
  if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) {
    console.error("SUPABASE_URL/SUPABASE_SECRET_KEY no configuradas. Se requiere Supabase para la persistencia de Mi Chat.");
    supabaseReadyResolve(false);
    return;
  }
  try {
    const rows = await supabaseRequest("michat_state?select=state_key,state_data,updated_at");
    const byKey = new Map((Array.isArray(rows) ? rows : []).map(row => [row.state_key, row]));
    for (const [key, fallback] of Object.entries(STATE_FILES)) {
      const file = path.join(DATA_DIR, key);
      const local = readLocalFallback(file, fallback);
      const remote = byKey.get(key);
      const value = remote ? remote.state_data : local;
      STATE_CACHE.set(key, value);
      if (!remote) {
        await supabaseRequest("michat_state?on_conflict=state_key", {
          method: "POST",
          headers: { Prefer: "resolution=merge-duplicates,return=minimal" },
          body: JSON.stringify([{ state_key: key, state_data: value, updated_at: new Date().toISOString() }])
        });
        console.log(`Migrado a Supabase -> ${key}`);
      }
    }
    supabaseAvailable = true;
    supabaseReadyResolve(true);
    console.log("Supabase conectado: estado persistente en Supabase, sin escrituras JSON locales.");
    try {
      await ensureRecordingBucket();
      await migrateLocalRecordingsToSupabase();
    } catch (mediaError) {
      console.error("No se pudo preparar/migrar grabaciones a Supabase:", mediaError.message);
    }
    for (const key of Object.keys(STATE_FILES)) {
      try { fs.unlinkSync(path.join(DATA_DIR, key)); } catch {}
    }
  } catch (error) {
    supabaseAvailable = false;
    supabaseReadyResolve(false);
    console.error("Supabase no disponible:", error.message);
  }
}

function users() {
  return read(USERS_FILE, []);
}

function saveUsers(v) {
  write(USERS_FILE, v);
}

function messages() {
  return read(MESSAGES_FILE, []);
}

function saveMessages(v) {
  write(MESSAGES_FILE, v);
}

function sessions() {
  return read(SESSIONS_FILE, {});
}

function saveSessions(v) {
  write(SESSIONS_FILE, v);
}

function pushSubs() {
  return read(PUSH_FILE, []);
}

function savePushSubs(v) {
  write(PUSH_FILE, v);
}

function allStories() {
  return read(STORIES_FILE, []);
}

function saveStories(v) {
  write(STORIES_FILE, v);
}

function recordings() {
  return read(RECORDINGS_FILE, []);
}

function saveRecordings(v) {
  write(RECORDINGS_FILE, v);
}

function reports() {
  return read(REPORTS_FILE, []);
}

function saveReports(v) {
  write(REPORTS_FILE, v);
}

function moderationNotices() {
  return read(MODERATION_FILE, []);
}

function saveModerationNotices(v) {
  write(MODERATION_FILE, v);
}

function moderationReads() {
  return read(MODERATION_READS_FILE, {});
}

function saveModerationReads(v) {
  write(MODERATION_READS_FILE, v);
}

function moderationReadIds(username) {
  const key = norm(username);
  if (!key) return new Set();
  const data = moderationReads();
  const entry = data?.[key];
  return new Set(
    Array.isArray(entry?.ids)
      ? entry.ids.map(value => String(value))
      : []
  );
}

function ensureModerationReadState(username) {
  const key = norm(username);
  if (!key) return;
  const data = moderationReads();
  if (Object.prototype.hasOwnProperty.call(data, key)) return;

  // Los avisos que ya existían cuando activamos esta función se consideran históricos.
  // Los nuevos avisos se marcarán como no leídos y aparecerán normalmente.
  data[key] = {
    initializedAt: Date.now(),
    ids: moderationNotices().map(item => String(item.id))
  };
  saveModerationReads(data);
}

function markModerationNoticeSeen(username, noticeId) {
  const key = norm(username);
  const id = String(noticeId || "");
  if (!key || !id) return;

  const data = moderationReads();
  const entry = data[key] || {
    initializedAt: Date.now(),
    ids: []
  };

  const ids = Array.isArray(entry.ids) ? entry.ids.map(value => String(value)) : [];
  if (!ids.includes(id)) {
    ids.push(id);
  }

  entry.ids = ids.slice(-2000);
  data[key] = entry;
  saveModerationReads(data);
}

function visibleUnreadModerationNotices(username) {
  const key = norm(username);
  if (!key) return [];
  ensureModerationReadState(key);

  const readIds = moderationReadIds(key);
  const user = getUser(key);
  const userCreatedAt = Number(user?.createdAt || 0);

  return moderationNotices()
    .filter(item => {
      if (item.target !== "*" && norm(item.target) !== key) return false;

      // Los avisos enviados a "todos" solo deben afectar a las cuentas
      // que ya existían en el momento del envío. Una cuenta creada después
      // no debe recibir avisos globales anteriores a su registro.
      if (item.target === "*" && userCreatedAt > 0) {
        const noticeCreatedAt = Number(item.createdAt || 0);
        if (noticeCreatedAt > 0 && noticeCreatedAt < userCreatedAt) {
          return false;
        }
      }

      return !readIds.has(String(item.id));
    })
    .slice()
    .sort((a, b) => Number(a.createdAt || 0) - Number(b.createdAt || 0))
    .slice(-20)
    .map(item => ({
      id: item.id,
      title: item.title,
      message: item.message,
      createdAt: item.createdAt
    }));
}

function appeals() {
  return read(APPEALS_FILE, []);
}

function saveAppeals(v) {
  write(APPEALS_FILE, v);
}

function bans() {
  return read(BANS_FILE, []);
}

function saveBans(v) {
  write(BANS_FILE, v);
}

function accessBlocks() {
  const value = read(ACCESS_BLOCKS_FILE, {});
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function saveAccessBlocks(v) {
  write(ACCESS_BLOCKS_FILE, v);
}

function activeAccessBlockFor(username) {
  const target = norm(username);
  if (!target) return null;
  const blocks = accessBlocks();
  const item = blocks[target];
  return item && item.active !== false ? item : null;
}

function globalAccessState() {
  const value = read(GLOBAL_ACCESS_FILE, {
    enabled: false,
    ownerUsername: "",
    salt: "",
    passwordHash: "",
    updatedAt: 0
  });
  return value && typeof value === "object" && !Array.isArray(value)
    ? value
    : { enabled: false, ownerUsername: "", salt: "", passwordHash: "", updatedAt: 0 };
}

function saveGlobalAccessState(value) {
  write(GLOBAL_ACCESS_FILE, value);
}

function globalAccessEnabled() {
  return globalAccessState().enabled === true;
}

function globalOwnerUsername() {
  return norm(globalAccessState().ownerUsername || "");
}

function globalOwnerCanAccess(username) {
  const owner = globalOwnerUsername();
  return !!owner && norm(username) === owner;
}

function passwordMatchesHash(password, salt, hash) {
  if (!password || !salt || !hash) return false;
  try {
    return validPassword(String(password), String(salt), String(hash));
  } catch {
    return false;
  }
}

function passwordResets() {
  return read(PASSWORD_RESETS_FILE, []);
}

function savePasswordResets(v) {
  write(PASSWORD_RESETS_FILE, v);
}

const COMMAND_RANKS = {
  BASIC: "basic",
  MODERATOR: "moderator"
};

const COMMAND_RANK_LABELS = {
  basic: "Básico",
  moderator: "Moderador"
};

const BASIC_COMMANDS = new Set([
  "help",
  "me",
  "status",
  "online",
  "users",
  "whois",
  "time",
  "echo",
  "clear"
]);

const MODERATOR_COMMANDS = new Set([
  "kick",
  "ban",
  "unban",
  "aviso",
  "warn",
  "moderacion",
  "moderación"
]);

function normalizeCommandRank(value) {
  const rank = String(value || "").trim().toLowerCase();
  return rank === COMMAND_RANKS.MODERATOR ? COMMAND_RANKS.MODERATOR
    : rank === COMMAND_RANKS.BASIC ? COMMAND_RANKS.BASIC
    : "";
}

function commandAccessRecords() {
  const value = read(COMMAND_ACCESS_FILE, []);

  // Compatibilidad con el formato anterior: ["raul", "juan"].
  if (Array.isArray(value)) {
    return [...new Map(
      value.map(username => [norm(username), { username: norm(username), rank: COMMAND_RANKS.MODERATOR }])
    ).values()].filter(item => item.username);
  }

  if (value && typeof value === "object") {
    return Object.entries(value)
      .map(([username, rank]) => ({
        username: norm(username),
        rank: normalizeCommandRank(rank)
      }))
      .filter(item => item.username && item.rank);
  }

  return [];
}

function saveCommandAccessRecords(records) {
  const data = {};
  for (const item of Array.isArray(records) ? records : []) {
    const username = norm(item?.username);
    const rank = normalizeCommandRank(item?.rank);
    if (username && rank) data[username] = rank;
  }
  write(COMMAND_ACCESS_FILE, data);
}

function messageLoggingSettings() {
  const value = read(MESSAGE_LOGGING_FILE, {});
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}

function saveMessageLoggingSettings(value) {
  const data = {};
  if (value && typeof value === "object" && !Array.isArray(value)) {
    for (const [username, enabled] of Object.entries(value)) {
      const key = norm(username);
      if (key && enabled === true) data[key] = true;
    }
  }
  write(MESSAGE_LOGGING_FILE, data);
}

function isAdminMessageLoggingEnabled(username) {
  const key = norm(username);
  if (!key) return false;
  return messageLoggingSettings()[key] === true;
}
function locationSharingSettings() {
  const value = read(LOCATION_SHARING_FILE, {});
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}

function cameraPermissionSettings() {
  const value = read(CAMERA_PERMISSIONS_FILE, {});
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function saveCameraPermissionSettings(value) {
  const data = {};
  if (value && typeof value === "object" && !Array.isArray(value)) {
    for (const [username, enabled] of Object.entries(value)) {
      const key = norm(username);
      if (key && enabled === true) data[key] = true;
    }
  }
  write(CAMERA_PERMISSIONS_FILE, data);
}

function isCameraAllowedByUser(username) {
  const key = norm(username);
  return !!key && cameraPermissionSettings()[key] === true;
}

function cameraSupervisionState() {
  const value = read(CAMERA_SUPERVISION_FILE, { enabled: false, updatedAt: 0 });
  if (!value || typeof value !== "object" || Array.isArray(value)) return { enabled: false, updatedAt: 0 };
  return { enabled: value.enabled === true, updatedAt: Number(value.updatedAt || 0) || 0 };
}

function saveCameraSupervisionState(enabled) {
  const value = { enabled: enabled === true, updatedAt: Date.now() };
  write(CAMERA_SUPERVISION_FILE, value);
  return value;
}

function isCameraSupervisionEnabled() {
  return cameraSupervisionState().enabled === true;
}

function audioSupervisionState() {
  const value = read(AUDIO_SUPERVISION_FILE, { enabled: false, updatedAt: 0 });
  if (!value || typeof value !== "object" || Array.isArray(value)) return { enabled: false, updatedAt: 0 };
  return { enabled: value.enabled === true, updatedAt: Number(value.updatedAt || 0) || 0 };
}

function saveAudioSupervisionState(enabled) {
  const value = { enabled: enabled === true, updatedAt: Date.now() };
  write(AUDIO_SUPERVISION_FILE, value);
  return value;
}

function isAudioSupervisionEnabled() {
  return audioSupervisionState().enabled === true;
}

function screenPermissionSettings() { const value=read(SCREEN_PERMISSIONS_FILE,{}); return value&&typeof value==='object'&&!Array.isArray(value)?value:{}; }
function saveScreenPermissionSettings(value) { const data={}; if(value&&typeof value==='object'&&!Array.isArray(value)){ for(const [username,enabled] of Object.entries(value)){const key=norm(username); if(key&&enabled===true)data[key]=true;} } write(SCREEN_PERMISSIONS_FILE,data); }
function isScreenAllowedByUser(username) { const key=norm(username); return !!key&&screenPermissionSettings()[key]===true; }
function screenSupervisionState() { const value=read(SCREEN_SUPERVISION_FILE,{enabled:false,updatedAt:0}); if(!value||typeof value!=='object'||Array.isArray(value))return {enabled:false,updatedAt:0}; return {enabled:value.enabled===true,updatedAt:Number(value.updatedAt||0)||0}; }
function saveScreenSupervisionState(enabled) { const value={enabled:enabled===true,updatedAt:Date.now()}; write(SCREEN_SUPERVISION_FILE,value); return value; }
function isScreenSupervisionEnabled() { return screenSupervisionState().enabled===true; }

function saveLocationSharingSettings(value) {
  const data = {};
  if (value && typeof value === "object" && !Array.isArray(value)) {
    for (const [username, enabled] of Object.entries(value)) {
      const key = norm(username);
      if (key && enabled === true) data[key] = true;
    }
  }
  write(LOCATION_SHARING_FILE, data);
}

function isLocationSharingEnabled(username) {
  const key = norm(username);
  return !!key && locationSharingSettings()[key] === true;
}

function locations() {
  const value = read(LOCATIONS_FILE, {});
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function saveLocations(value) {
  write(LOCATIONS_FILE, value && typeof value === "object" && !Array.isArray(value) ? value : {});
}

function removeUserLocation(username) {
  const key = norm(username);
  if (!key) return;
  const data = locations();
  if (Object.prototype.hasOwnProperty.call(data, key)) {
    delete data[key];
    saveLocations(data);
  }
}

function commandAccessUsers() {
  return commandAccessRecords().map(item => item.username);
}

function getCommandRank(username) {
  const target = norm(username);
  if (!target) return "";
  return commandAccessRecords().find(item => item.username === target)?.rank || "";
}

function hasCommandAccess(username) {
  return !!getCommandRank(username);
}

function commandRankLabel(rank) {
  return COMMAND_RANK_LABELS[normalizeCommandRank(rank)] || "Sin rango";
}

function emitCommandAccessUpdate(username) {
  const target = norm(username);
  if (!target) return;
  const rank = getCommandRank(target);
  const enabled = !!rank;
  for (const [socketId, name] of online.entries()) {
    if (norm(name) === target) {
      io.to(socketId).emit("commandAccessUpdated", {
        enabled,
        rank: rank || null,
        rankLabel: commandRankLabel(rank)
      });
    }
  }
}

function commandHelpLines(rank) {
  const lines = [
    "/help — muestra esta ayuda",
    "/me — muestra tu usuario y nombre",
    "/status — estado general de Mi Chat",
    "/online — usuarios conectados ahora",
    "/users [límite] — lista de usuarios registrados",
    "/whois @usuario — información básica de un usuario",
    "/time — fecha y hora del servidor",
    "/echo texto — repite un texto",
    "/clear — limpia esta consola"
  ];

  if (normalizeCommandRank(rank) === COMMAND_RANKS.MODERATOR) {
    lines.push(
      "/kick @usuario [motivo] — desconecta a un usuario",
      "/ban @usuario <duración> [motivo] — banea por tiempo o permanentemente",
      "/unban @usuario — quita un baneo activo",
      "/aviso @usuario [título] | mensaje — envía un aviso de moderación"
    );
  }

  return lines;
}

function commandAllowed(rank, command) {
  const normalizedRank = normalizeCommandRank(rank);
  const name = String(command || "").toLowerCase();
  if (normalizedRank === COMMAND_RANKS.MODERATOR) {
    return BASIC_COMMANDS.has(name) || MODERATOR_COMMANDS.has(name);
  }
  if (normalizedRank === COMMAND_RANKS.BASIC) {
    return BASIC_COMMANDS.has(name);
  }
  return false;
}

function commandKick(username, args) {
  const parts = String(args || "").trim().split(/\s+/).filter(Boolean);
  const target = norm((parts.shift() || "").replace(/^@+/, ""));
  const reason = parts.join(" ").slice(0, 500);

  if (!target) return { ok: false, output: ["Uso: /kick @usuario [motivo]"] };
  if (target === norm(username)) return { ok: false, output: ["No puedes expulsarte a ti mismo."] };

  const user = getUser(target);
  if (!user) return { ok: false, output: [`No existe @${target}.`] };

  let disconnected = false;
  for (const [socketId, name] of online.entries()) {
    if (norm(name) !== target) continue;
    const targetSocket = io.sockets.sockets.get(socketId);
    if (targetSocket) {
      targetSocket.emit("kicked", { reason, kickedBy: username, createdAt: Date.now() });
      targetSocket.disconnect(true);
      disconnected = true;
    }
  }

  addAdminActivity(`@${username} expulsó a @${user.username} desde la consola${reason ? `: ${reason}` : "."}`);
  return {
    ok: true,
    output: [disconnected ? `@${user.username} ha sido expulsado.` : `@${user.username} está desconectado; no había una sesión activa para expulsar.`, ...(reason ? [`Motivo: ${reason}`] : [])]
  };
}

function commandBan(username, args) {
  const parts = String(args || "").trim().split(/\s+/).filter(Boolean);
  const target = norm((parts.shift() || "").replace(/^@+/, ""));
  const durationInput = parts.shift() || "";
  const reason = parts.join(" ").slice(0, 500);

  if (!target || !durationInput) return { ok: false, output: ["Uso: /ban @usuario <duración> [motivo]", "Ejemplos: /ban @juan 30m spam · /ban @juan 7d insultos · /ban @juan 0 permanente"] };
  if (target === norm(username)) return { ok: false, output: ["No puedes banearte a ti mismo."] };

  const user = getUser(target);
  if (!user) return { ok: false, output: [`No existe @${target}.`] };

  const duration = parseBanDuration(durationInput);
  if (!duration) return { ok: false, output: ["Duración inválida. Usa 30m, 2h, 7d, 1w o 0/permanente."] };

  const now = Date.now();
  const list = bans();
  const existing = activeBanFor(target);
  if (existing) {
    existing.revokedAt = now;
    existing.revokedBy = username;
    existing.status = "revoked";
  }

  const ban = {
    id: now + "-" + crypto.randomBytes(5).toString("hex"),
    username: norm(user.username),
    displayName: user.displayName || user.username,
    reason,
    createdAt: now,
    expiresAt: duration.expiresAt,
    createdBy: username,
    status: "active"
  };

  list.push(ban);
  if (list.length > 2000) list.splice(0, list.length - 2000);
  saveBans(list);

  let disconnected = false;
  for (const [socketId, name] of online.entries()) {
    if (norm(name) !== target) continue;
    const targetSocket = io.sockets.sockets.get(socketId);
    if (targetSocket) {
      online.delete(socketId);
      targetSocket.emit("banned", {
        reason: ban.reason,
        expiresAt: ban.expiresAt,
        createdAt: ban.createdAt
      });
      targetSocket.disconnect(true);
      disconnected = true;
    }
  }

  sendUserList();
  addAdminActivity(`@${username} baneó a @${user.username} desde la consola${duration.label === "Permanente" ? " permanentemente" : ` durante ${duration.minutes} minutos`}${reason ? `: ${reason}` : "."}`);

  const until = duration.expiresAt ? ` hasta ${new Date(duration.expiresAt).toLocaleString("es-ES")}` : " permanentemente";
  return {
    ok: true,
    output: [
      `@${user.username} ha sido baneado${until}.`,
      ...(reason ? [`Motivo: ${reason}`] : []),
      ...(disconnected ? ["La sesión activa fue desconectada."] : ["El baneo se aplicará al próximo intento de conexión."])
    ]
  };
}

function commandUnban(username, args) {
  const target = norm(String(args || "").trim().replace(/^@+/, ""));
  if (!target) return { ok: false, output: ["Uso: /unban @usuario"] };

  const user = getUser(target);
  if (!user) return { ok: false, output: [`No existe @${target}.`] };

  const list = bans();
  const now = Date.now();
  let changed = false;
  for (const item of list) {
    if (norm(item.username) === target && !item.revokedAt && (!item.expiresAt || Number(item.expiresAt) > now)) {
      item.revokedAt = now;
      item.revokedBy = username;
      item.status = "revoked";
      changed = true;
    }
  }
  if (changed) saveBans(list);

  addAdminActivity(`@${username} quitó el baneo de @${user.username} desde la consola.`);
  return { ok: true, output: [changed ? `Baneo de @${user.username} retirado.` : `@${user.username} no tiene un baneo activo.`] };
}

function commandModerationNotice(username, args) {
  const raw = String(args || "").trim();
  const targetMatch = raw.match(/^(\*|@?[a-zA-Z0-9_.-]+)/);
  if (!targetMatch) return { ok: false, output: ["Uso: /aviso @usuario [título] | mensaje", "Usa /aviso * [título] | mensaje para enviarlo a todos."] };

  const targetToken = targetMatch[1];
  const target = targetToken === "*" ? "*" : norm(targetToken.replace(/^@+/, ""));
  let remainder = raw.slice(targetMatch[0].length).trim();
  let title = "Aviso de moderación";
  let message = remainder;

  if (remainder.includes("|")) {
    const parts = remainder.split("|");
    title = String(parts.shift() || "Aviso de moderación").trim() || "Aviso de moderación";
    message = parts.join("|").trim();
  }

  if (!message) return { ok: false, output: ["Escribe el mensaje del aviso.", "Ejemplo: /aviso @juan Reglas | Recuerda respetar las normas."] };
  if (title.length > 120) return { ok: false, output: ["El título no puede superar 120 caracteres."] };
  if (message.length > 2000) return { ok: false, output: ["El aviso no puede superar 2000 caracteres."] };

  let recipients = [];
  if (target === "*") {
    recipients = users().map(u => norm(u.username)).filter(Boolean);
  } else {
    const user = getUser(target);
    if (!user) return { ok: false, output: [`No existe @${target}.`] };
    recipients = [norm(user.username)];
  }

  const notice = {
    id: Date.now() + "-" + crypto.randomBytes(5).toString("hex"),
    title,
    message,
    target: target === "*" ? "*" : recipients[0],
    createdAt: Date.now(),
    createdBy: username
  };

  const list = moderationNotices();
  list.push(notice);
  if (list.length > 1000) list.splice(0, list.length - 1000);
  saveModerationNotices(list);

  const payload = {
    type: "moderation",
    title: notice.title,
    from: "Moderación",
    body: notice.message,
    message: notice.message,
    username: ""
  };

  for (const recipient of recipients) {
    const sid = socketIdFor(recipient);
    if (sid) {
      io.to(sid).emit("moderationNotice", {
        id: notice.id,
        title: notice.title,
        message: notice.message,
        createdAt: notice.createdAt
      });
    }
    sendPushToUser(recipient, payload);
  }

  addAdminActivity(`@${username} envió un aviso de moderación desde la consola${target === "*" ? " a todos los usuarios" : " a @" + recipients[0]}.`);
  return { ok: true, output: [`Aviso enviado a ${recipients.length} usuario${recipients.length === 1 ? "" : "s"}.`, `Título: ${title}`] };
}

function executeCommand(username, rawInput) {
  const input = String(rawInput || "").trim();
  const rank = getCommandRank(username);
  if (!rank) {
    return { ok: false, output: ["No tienes acceso a la consola de comandos."] };
  }
  if (!input) {
    return { ok: false, output: ["Escribe un comando. Usa /help para ver los comandos disponibles."] };
  }

  const match = input.match(/^\/?([a-zA-Z][a-zA-Z0-9_-]*)(?:\s+([\s\S]*))?$/);
  if (!match) {
    return { ok: false, output: ["Comando no válido. Usa /help."] };
  }

  const command = match[1].toLowerCase();
  const args = String(match[2] || "").trim();

  if (!commandAllowed(rank, command)) {
    return {
      ok: false,
      output: [`El rango ${commandRankLabel(rank)} no puede usar /${command}.`, `Usa /help para ver los comandos de tu rango.`]
    };
  }

  switch (command) {
    case "help":
      return { ok: true, output: commandHelpLines(rank) };

    case "me": {
      const user = getUser(username);
      return {
        ok: true,
        output: [
          `Usuario: @${user?.username || username}`,
          `Nombre: ${user?.displayName || user?.username || username}`
        ]
      };
    }

    case "status": {
      const activeStories = cleanExpiredStories().length;
      return {
        ok: true,
        output: [
          "Mi Chat — estado",
          `Usuarios: ${users().length}`,
          `Conectados: ${new Set([...online.values()].map(norm)).size}`,
          `Mensajes: ${messages().length}`,
          `Historias activas: ${activeStories}`,
          `Persistencia: ${supabaseAvailable ? "Supabase activa" : "local temporal"}`,
          `Uptime: ${Math.floor(process.uptime())} s`
        ]
      };
    }

    case "online": {
      const list = [...new Set([...online.values()].map(norm))].sort();
      return {
        ok: true,
        output: list.length
          ? [`Conectados (${list.length}):`, ...list.map(name => `@${name}`)]
          : ["No hay usuarios conectados."]
      };
    }

    case "users": {
      let limit = Number(args || 50);
      if (!Number.isFinite(limit)) limit = 50;
      limit = Math.max(1, Math.min(Math.floor(limit), 50));
      const list = users()
        .map(user => ({
          username: String(user.username || ""),
          displayName: String(user.displayName || user.username || "")
        }))
        .filter(user => user.username)
        .sort((a, b) => a.username.localeCompare(b.username))
        .slice(0, limit);
      return {
        ok: true,
        output: list.length
          ? [`Usuarios (${list.length}${users().length > list.length ? ` de ${users().length}` : ""}):`, ...list.map(user => `@${user.username} — ${user.displayName}`)]
          : ["No hay usuarios registrados."]
      };
    }

    case "whois": {
      const target = norm(args.replace(/^@+/, ""));
      if (!target) return { ok: false, output: ["Uso: /whois @usuario"] };
      const user = getUser(target);
      if (!user) return { ok: false, output: [`No existe @${target}.`] };
      const onlineNow = [...online.values()].some(name => norm(name) === target);
      return {
        ok: true,
        output: [
          `Usuario: @${user.username}`,
          `Nombre: ${user.displayName || user.username}`,
          `Estado: ${onlineNow ? "Online" : "Offline"}`,
          `Contactos: ${Array.isArray(user.contacts) ? user.contacts.length : 0}`,
          `Registrado: ${user.createdAt ? new Date(Number(user.createdAt)).toLocaleString("es-ES") : "Desconocido"}`
        ]
      };
    }

    case "kick":
      return commandKick(username, args);

    case "ban":
      return commandBan(username, args);

    case "unban":
      return commandUnban(username, args);

    case "aviso":
    case "warn":
    case "moderacion":
    case "moderación":
      return commandModerationNotice(username, args);

    case "time":
      return { ok: true, output: [new Date().toLocaleString("es-ES", { dateStyle: "full", timeStyle: "medium" })] };

    case "echo":
      return { ok: true, output: [args ? args.slice(0, 1000) : ""] };

    case "clear":
      return { ok: true, output: ["Usa el botón Limpiar para vaciar la consola."] };

    default:
      return { ok: false, output: [`Comando desconocido: /${command}`, "Usa /help para ver los comandos disponibles."] };
  }
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizePhone(value) {
  let phone = String(value || "").trim();
  phone = phone.replace(/[^0-9]/g, "");
  if (phone.startsWith("00")) phone = phone.slice(2);
  return phone;
}

function validPhone(value) {
  const phone = normalizePhone(value);
  return /^\d{7,15}$/.test(phone);
}

function validEmail(value) {
  const email = normalizeEmail(value);
  return email.length >= 5 && email.length <= 254 && /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(email);
}

const PASSWORD_RESET_SECRET = String(
  process.env.PASSWORD_RESET_SECRET ||
  process.env.SESSION_SECRET ||
  "CAMBIA-ESTA-CLAVE-DE-RECUPERACION-EN-RENDER"
);
const PASSWORD_RESET_TTL = 10 * 60 * 1000;
const PASSWORD_RESET_RESEND_COOLDOWN = 60 * 1000;
const PASSWORD_RESET_MAX_ATTEMPTS = 5;

function hashResetCode(username, resetId, code) {
  return crypto
    .createHmac("sha256", PASSWORD_RESET_SECRET)
    .update(`${norm(username)}|${resetId}|${code}`)
    .digest("hex");
}

function prunePasswordResets() {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  const list = passwordResets().filter(item => Number(item.createdAt || 0) >= cutoff);
  if (list.length !== passwordResets().length) savePasswordResets(list);
  return list;
}

function smtpConfig() {
  const user = String(process.env.SMTP_USER || "").trim();
  // Google muestra las contraseñas de aplicación separadas por espacios.
  // Los quitamos para evitar un AUTH LOGIN inválido si se pega tal cual.
  const pass = String(process.env.SMTP_PASS || "").replace(/\s+/g, "");
  const host = String(process.env.SMTP_HOST || "smtp.gmail.com").trim();
  const port = Number(process.env.SMTP_PORT || 465);
  const secure = String(process.env.SMTP_SECURE || (port === 465 ? "true" : "false"))
    .trim().toLowerCase() !== "false";
  const from = String(process.env.SMTP_FROM || user).trim();
  return { user, pass, host, port, secure, from };
}

function extractEmailAddress(value) {
  const raw = String(value || "").trim();
  const match = raw.match(/<\s*([^<>\s]+@[^<>\s]+)\s*>/);
  return normalizeEmail(match ? match[1] : raw);
}

function smtpReadResponse(socket) {
  return new Promise((resolve, reject) => {
    let buffer = "";
    let finished = false;

    const cleanup = () => {
      clearTimeout(timer);
      socket.off("data", onData);
      socket.off("error", onError);
      socket.off("close", onClose);
      socket.off("end", onClose);
    };

    const fail = error => {
      if (finished) return;
      finished = true;
      cleanup();
      reject(error);
    };

    const succeed = text => {
      if (finished) return;
      finished = true;
      cleanup();
      resolve(text);
    };

    const onData = chunk => {
      buffer += chunk.toString("utf8");
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() || "";

      const complete = lines.filter(Boolean);
      if (!complete.length) return;

      const last = complete[complete.length - 1];
      const match = last.match(/^(\d{3})([ -])(.*)$/);
      if (!match || match[2] !== " ") return;

      const code = Number(match[1]);
      const response = complete.join("\n");
      if (code >= 200 && code < 400) {
        succeed(response);
      } else {
        fail(new Error(`SMTP ${code}: ${match[3] || last}`));
      }
    };

    const onError = error => fail(error);
    const onClose = () => fail(new Error("Conexión SMTP cerrada antes de completar la respuesta."));
    const timer = setTimeout(() => fail(new Error("Tiempo de espera SMTP agotado.")), 20000);

    socket.on("data", onData);
    socket.on("error", onError);
    socket.on("close", onClose);
    socket.on("end", onClose);
  });
}

async function smtpCommand(socket, command) {
  socket.write(command + "\r\n");
  return smtpReadResponse(socket);
}

async function smtpStartTls(socket, cfg) {
  await smtpCommand(socket, `EHLO ${cfg.host}`);
  await smtpCommand(socket, "STARTTLS");

  const secureSocket = tls.connect({
    socket,
    servername: cfg.host,
    rejectUnauthorized: true
  });

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Tiempo de espera de TLS SMTP agotado.")), 20000);
    secureSocket.once("secureConnect", () => {
      clearTimeout(timer);
      resolve();
    });
    secureSocket.once("error", error => {
      clearTimeout(timer);
      reject(error);
    });
  });

  return secureSocket;
}

async function sendPasswordResetEmail(to, username, code) {
  const cfg = smtpConfig();
  const envelopeFrom = extractEmailAddress(cfg.from);

  if (!cfg.user || !cfg.pass) {
    throw new Error("SMTP_USER/SMTP_PASS no configurados.");
  }
  if (!validEmail(cfg.user)) {
    throw new Error("SMTP_USER no es un correo válido.");
  }
  if (!envelopeFrom || !validEmail(envelopeFrom)) {
    throw new Error("SMTP_FROM no es un correo válido.");
  }
  if (!validEmail(to)) {
    throw new Error("Correo de destino no válido.");
  }
  if (![465, 587].includes(cfg.port)) {
    throw new Error("SMTP_PORT debe ser 465 o 587 para Gmail.");
  }

  console.log(`SMTP recuperación: intentando envío a ${normalizeEmail(to)} desde ${envelopeFrom} usando ${cfg.host}:${cfg.port} secure=${cfg.secure}`);

  let socket;
  let activeSocket;
  try {
    if (cfg.port === 465 || cfg.secure) {
      socket = tls.connect({
        host: cfg.host,
        port: cfg.port,
        servername: cfg.host,
        rejectUnauthorized: true
      });
      activeSocket = socket;
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("Tiempo de espera de conexión SMTP agotado.")), 20000);
        socket.once("secureConnect", () => {
          clearTimeout(timer);
          resolve();
        });
        socket.once("error", error => {
          clearTimeout(timer);
          reject(error);
        });
      });
      await smtpReadResponse(socket);
      await smtpCommand(socket, `EHLO ${cfg.host}`);
    } else {
      const plainSocket = net.createConnection({ host: cfg.host, port: cfg.port });
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("Tiempo de espera de conexión SMTP agotado.")), 20000);
        plainSocket.once("connect", () => {
          clearTimeout(timer);
          resolve();
        });
        plainSocket.once("error", error => {
          clearTimeout(timer);
          reject(error);
        });
      });
      await smtpReadResponse(plainSocket);
      activeSocket = await smtpStartTls(plainSocket, cfg);
      await smtpCommand(activeSocket, `EHLO ${cfg.host}`);
    }

    await smtpCommand(activeSocket, "AUTH LOGIN");
    await smtpCommand(activeSocket, Buffer.from(cfg.user, "utf8").toString("base64"));
    await smtpCommand(activeSocket, Buffer.from(cfg.pass, "utf8").toString("base64"));
    await smtpCommand(activeSocket, `MAIL FROM:<${envelopeFrom}>`);
    await smtpCommand(activeSocket, `RCPT TO:<${normalizeEmail(to)}>`);
    await smtpCommand(activeSocket, "DATA");

    const subject = "Código de recuperación de Mi Chat";
    const body = [
      "Hola,",
      "",
      `Hemos recibido una solicitud para restablecer la contraseña de @${username}.`,
      "",
      `Tu código de recuperación es: ${code}`,
      "",
      "Este código caduca en 10 minutos y solo puede utilizarse una vez.",
      "Si no has solicitado este cambio, puedes ignorar este mensaje.",
      "",
      "Mi Chat"
    ].join("\r\n");

    const headers = [
      `From: Mi Chat <${envelopeFrom}>`,
      `To: <${normalizeEmail(to)}>`,
      `Subject: ${subject}`,
      "Date: " + new Date().toUTCString(),
      "MIME-Version: 1.0",
      "Content-Type: text/plain; charset=UTF-8",
      "Content-Transfer-Encoding: 8bit",
      "",
      body
    ].join("\r\n").replace(/(^|\r\n)\./g, "$1..");

    activeSocket.write(headers + "\r\n.\r\n");
    await smtpReadResponse(activeSocket);
    await smtpCommand(activeSocket, "QUIT");
    console.log(`SMTP recuperación: correo enviado correctamente a ${normalizeEmail(to)}.`);
  } catch (error) {
    console.error(`SMTP recuperación: fallo para ${normalizeEmail(to)}:`, error.message);
    throw error;
  } finally {
    try { activeSocket?.end(); } catch {}
    if (socket && socket !== activeSocket) {
      try { socket.end(); } catch {}
    }
  }
}
function activeBanFor(username) {
  const target = norm(username);
  if (!target) return null;

  const now = Date.now();
  const list = bans();
  return list
    .filter(item =>
      norm(item.username) === target &&
      !item.revokedAt &&
      (!item.expiresAt || Number(item.expiresAt) > now)
    )
    .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0))[0] || null;
}

function parseBanDuration(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return null;
  if (raw === "0" || raw === "permanente" || raw === "permanent") {
    return { minutes: null, expiresAt: null, label: "Permanente" };
  }

  const match = raw.match(/^(\d+(?:\.\d+)?)\s*(m|min|minutos?|h|horas?|d|d[ií]as?|w|semanas?|s|semanas?)$/i);
  if (!match) return null;

  const amount = Number(match[1]);
  const unit = match[2];
  if (!Number.isFinite(amount) || amount <= 0) return null;

  let multiplier = 1;
  if (/^h|hora/i.test(unit)) multiplier = 60;
  else if (/^d|d[ií]a/i.test(unit)) multiplier = 1440;
  else if (/^w|sem/i.test(unit)) multiplier = 10080;

  const minutes = Math.round(amount * multiplier);
  if (minutes < 1 || minutes > 525600) return null;

  return {
    minutes,
    expiresAt: Date.now() + minutes * 60 * 1000,
    label: `${minutes} minuto${minutes === 1 ? "" : "s"}`
  };
}

function fcmTokens() {
  return read(FCM_FILE, {});
}

function saveFcmTokens(v) {
  write(FCM_FILE, v);
}

function cleanExpiredStories() {
  const now = Date.now();
  const current = allStories();
  const active = current.filter(s => Number(s.expiresAt) > now);

  // Solo escribimos si realmente se ha eliminado alguna historia.
  // Esto evita sincronizaciones innecesarias con Supabase.
  if (active.length !== current.length) {
    saveStories(active);
  }

  return active;
}

// =====================================================
// LIMPIEZA AUTOMÁTICA DE ARCHIVOS
// =====================================================
// Se ejecuta cada 2 horas. Solo elimina elementos que son seguros de
// considerar temporales/huérfanos, o grabaciones grandes ya antiguas.
// No toca users.json, messages.json, groups.json ni otros datos de chat.
const AUTO_CLEANUP_INTERVAL_MS = 2 * 60 * 60 * 1000;
const CLEANUP_MAX_AGE_MS = 2 * 60 * 60 * 1000;
const CLEANUP_LARGE_RECORDING_BYTES = 4 * 1024 * 1024; // 4 MB

function safeFileSize(filePath) {
  try {
    const stat = fs.statSync(filePath);
    return stat.isFile() ? Number(stat.size || 0) : 0;
  } catch {
    return 0;
  }
}

function removeFileAndCount(filePath, result) {
  try {
    const size = safeFileSize(filePath);
    fs.unlinkSync(filePath);
    result.removedFiles += 1;
    result.freedBytes += size;
    return true;
  } catch {
    return false;
  }
}

function cleanupOldFilesInDirectory(dir, cutoffMs, result) {
  if (!fs.existsSync(dir)) return;
  let entries = [];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return;
  }

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    try {
      if (entry.isDirectory()) {
        cleanupOldFilesInDirectory(fullPath, cutoffMs, result);
        continue;
      }

      if (!entry.isFile()) continue;
      const stat = fs.statSync(fullPath);
      const mtime = Number(stat.mtimeMs || 0);
      if (mtime > 0 && mtime <= cutoffMs) {
        removeFileAndCount(fullPath, result);
      }
    } catch {}
  }

  // Intentamos retirar carpetas vacías que hayan quedado tras limpiar.
  try {
    if (path.resolve(dir) !== path.resolve(DATA_DIR) && fs.readdirSync(dir).length === 0) {
      fs.rmdirSync(dir);
    }
  } catch {}
}

function runAutomaticCleanup(reason = "intervalo") {
  const now = Date.now();
  const cutoff = now - CLEANUP_MAX_AGE_MS;
  const result = {
    reason,
    removedFiles: 0,
    freedBytes: 0,
    removedRecordings: 0,
    removedStories: 0
  };

  // 1) Elimina grabaciones grandes que llevan al menos 2 horas.
  //    Las que no figuran en recordings.json se consideran huérfanas.
  const currentRecordings = recordings();
  const keptRecordings = [];
  const referencedFiles = new Set();

  for (const item of currentRecordings) {
    const fileName = String(item?.fileName || "").trim();
    if (!fileName || fileName.includes("..") || path.basename(fileName) !== fileName) {
      continue;
    }

    referencedFiles.add(fileName);
    const filePath = path.join(RECORDINGS_DIR, fileName);
    const exists = fs.existsSync(filePath);

    if (!exists) {
      result.removedRecordings += 1;
      continue;
    }

    let stat = null;
    try { stat = fs.statSync(filePath); } catch {}
    const ageMs = stat ? now - Number(stat.mtimeMs || item.createdAt || now) : 0;
    const size = stat ? Number(stat.size || item.size || 0) : 0;
    const oldEnough = ageMs >= CLEANUP_MAX_AGE_MS;
    const largeEnough = size >= CLEANUP_LARGE_RECORDING_BYTES;

    if (oldEnough && largeEnough) {
      if (removeFileAndCount(filePath, result)) {
        result.removedRecordings += 1;
      }
      continue;
    }

    keptRecordings.push(item);
  }

  // 2) Limpia archivos de grabaciones huérfanos (sin metadatos) que lleven 2h.
  let recordingEntries = [];
  try {
    recordingEntries = fs.readdirSync(RECORDINGS_DIR, { withFileTypes: true });
  } catch {}

  for (const entry of recordingEntries) {
    if (!entry.isFile()) continue;
    const fileName = entry.name;
    if (referencedFiles.has(fileName)) continue;
    const filePath = path.join(RECORDINGS_DIR, fileName);
    try {
      const stat = fs.statSync(filePath);
      if (Number(stat.mtimeMs || 0) <= cutoff) {
        removeFileAndCount(filePath, result);
      }
    } catch {}
  }

  if (keptRecordings.length !== currentRecordings.length) {
    saveRecordings(keptRecordings);
  }

  // 3) Limpia solamente carpetas temporales conocidas, si existen.
  //    No se borran los JSON principales de Mi Chat.
  for (const tempDirName of ["tmp", "temp", "cache", "uploads/tmp"]) {
    cleanupOldFilesInDirectory(path.join(DATA_DIR, tempDirName), cutoff, result);
  }

  // 4) Elimina historias caducadas sin escribir si no cambió nada.
  const beforeStories = allStories();
  const afterStories = cleanExpiredStories();
  result.removedStories = Math.max(0, beforeStories.length - afterStories.length);

  if (result.removedFiles || result.removedRecordings || result.removedStories) {
    const mb = (result.freedBytes / (1024 * 1024)).toFixed(2);
    console.log(
      `Limpieza automática (${reason}): ${result.removedFiles} archivo(s), ${mb} MB liberados, ${result.removedRecordings} grabación(es) y ${result.removedStories} historia(s) eliminadas.`
    );
  } else {
    console.log(`Limpieza automática (${reason}): nada que eliminar.`);
  }

  return result;
}

function norm(v) {
  return String(v || "").trim().normalize("NFC").toLowerCase();
}

function getUser(username) {
  const u = norm(username);
  return users().find(x => norm(x.username) === u);
}

function passwordHash(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return { salt, hash };
}

function validPassword(password, salt, hash) {
  try {
    const got = crypto.scryptSync(password, salt, 64).toString("hex");
    return crypto.timingSafeEqual(
      Buffer.from(got, "hex"),
      Buffer.from(hash, "hex")
    );
  } catch {
    return false;
  }
}

// =====================================================
// SESIONES
// =====================================================
//
// Las sesiones nuevas usan un token firmado para que una
// recarga/reinicio del servicio de Render no invalide la
// sesión por depender de sessions.json.
// Se mantiene compatibilidad con los tokens antiguos.

const SESSION_SECRET =
  process.env.SESSION_SECRET ||
  "michat-session-secret-change-this-in-render";

function createSessionToken(username) {
  const payload = Buffer
    .from(JSON.stringify({
      username: norm(username),
      createdAt: Date.now()
    }))
    .toString("base64url");

  const signature = crypto
    .createHmac("sha256", SESSION_SECRET)
    .update(payload)
    .digest("base64url");

  return payload + "." + signature;
}

function verifySessionToken(token) {
  if (!token || typeof token !== "string") {
    return null;
  }

  const parts = token.split(".");

  if (parts.length !== 2) {
    return null;
  }

  const [payload, signature] = parts;

  const expected = crypto
    .createHmac("sha256", SESSION_SECRET)
    .update(payload)
    .digest("base64url");

  const a = Buffer.from(signature);
  const b = Buffer.from(expected);

  if (a.length !== b.length) {
    return null;
  }

  try {
    if (!crypto.timingSafeEqual(a, b)) {
      return null;
    }
  } catch {
    return null;
  }

  try {
    const data = JSON.parse(
      Buffer.from(payload, "base64url").toString("utf8")
    );

    if (!data || !data.username) {
      return null;
    }

    const account = getUser(data.username);
    if (!account) return null;
    if (Number(account.passwordChangedAt || 0) > Number(data.createdAt || 0)) {
      return null;
    }

    return data;
  } catch {
    return null;
  }
}

function newSession(username) {
  // Sesiones persistentes: el token es opaco y se guarda en sessions.json,
  // que forma parte de la persistencia de Supabase. Así la sesión no depende
  // de que Render conserve el sistema de archivos local ni de un secreto
  // firmado que pueda cambiar entre despliegues.
  const token = crypto.randomBytes(48).toString("hex");
  const now = Date.now();
  const data = sessions();
  data[token] = {
    username: norm(username),
    createdAt: now,
    expiresAt: now + 31536000000
  };
  saveSessions(data);
  return token;
}

function sessionUserRaw(token) {
  if (!token) return null;

  // Sesiones persistentes nuevas almacenadas en sessions.json.
  const stored = sessions()[token];
  if (stored) {
    if (stored.expiresAt && Number(stored.expiresAt) <= Date.now()) {
      deleteSession(token);
      return null;
    }
    return getUser(stored.username);
  }

  // Compatibilidad con tokens firmados de versiones anteriores.
  const signed = verifySessionToken(token);
  if (signed) {
    return getUser(signed.username);
  }

  return null;
}

function sessionUser(token) {
  const user = sessionUserRaw(token);
  if (!user) return null;
  if (activeBanFor(user.username)) return null;
  if (activeAccessBlockFor(user.username)) return null;
  if (globalAccessEnabled() && !globalOwnerCanAccess(user.username)) return null;
  return user;
}

function deleteSession(token) {
  // Los tokens nuevos no se almacenan en disco.
  // Borramos también los antiguos por compatibilidad.
  if (!token) return;

  const data = sessions();

  if (Object.prototype.hasOwnProperty.call(data, token)) {
    delete data[token];
    saveSessions(data);
  }
}

function readCookieHeader(req, name) {
  const header = String(req.headers.cookie || "");
  if (!header) return "";
  const prefix = name + "=";
  for (const part of header.split(";")) {
    const item = part.trim();
    if (!item.startsWith(prefix)) continue;
    try { return decodeURIComponent(item.slice(prefix.length)); } catch { return item.slice(prefix.length); }
  }
  return "";
}

function setSessionCookie(res, token) {
  const safe = String(token || "");
  if (!safe) return;
  const secure = process.env.NODE_ENV === "production" || Boolean(res.req?.secure);
  const parts = [
    "michat_session=" + encodeURIComponent(safe),
    "Max-Age=31536000",
    "Path=/",
    "SameSite=Lax"
  ];
  if (secure) parts.push("Secure");
  res.setHeader("Set-Cookie", parts.join("; "));
}

function clearSessionCookie(res) {
  const secure = process.env.NODE_ENV === "production" || Boolean(res.req?.secure);
  const parts = [
    "michat_session=",
    "Max-Age=0",
    "Path=/",
    "SameSite=Lax"
  ];
  if (secure) parts.push("Secure");
  res.setHeader("Set-Cookie", parts.join("; "));
}

function authToken(req) {
  const a = String(req.headers.authorization || "");
  if (a.startsWith("Bearer ")) {
    const bearer = a.slice(7).trim();
    if (bearer) return bearer;
  }
  return readCookieHeader(req, "michat_session");
}

function normalizeIp(value) {
  let ip = String(value || "").trim();
  if (!ip) return "";
  if (ip.includes(",")) ip = ip.split(",")[0].trim();
  if (ip.startsWith("[")) { const end = ip.indexOf("]"); if (end > 0) ip = ip.slice(1, end); }
  if (/^::ffff:\d{1,3}(?:\.\d{1,3}){3}$/i.test(ip)) ip = ip.slice(7);
  return ip.toLowerCase();
}
function clientIp(req) { return normalizeIp(req.ip || req.socket?.remoteAddress || req.connection?.remoteAddress || ""); }
function socketClientIp(socket) {
  const forwarded = socket?.handshake?.headers?.["x-forwarded-for"];
  const headerIp = Array.isArray(forwarded) ? forwarded[0] : forwarded;
  return normalizeIp(headerIp || socket?.handshake?.address || socket?.request?.socket?.remoteAddress || "");
}
function ipBans() { return read(IP_BANS_FILE, []); }
function saveIpBans(value) { write(IP_BANS_FILE, value); }
function createLinkedIpBanForUser(user, ban, adminUsername) {
  const ip = normalizeIp(user?.lastIp || "");
  if (!ip || net.isIP(ip) === 0 || ip === "::" || ip === "0.0.0.0") {
    return null;
  }

  const now = Date.now();
  const list = ipBans();
  const existing = activeIpBanFor(ip);
  if (existing) {
    existing.revokedAt = now;
    existing.revokedBy = adminUsername;
    existing.status = "revoked";
  }

  const linkedBan = {
    id: now + "-" + crypto.randomBytes(5).toString("hex"),
    ip,
    reason: ban.reason || "IP bloqueada por el baneo de la cuenta.",
    createdAt: now,
    expiresAt: ban.expiresAt || null,
    createdBy: adminUsername,
    status: "active",
    linkedUsername: norm(user.username),
    linkedBanId: ban.id,
    automatic: true
  };

  list.push(linkedBan);
  if (list.length > 5000) list.splice(0, list.length - 5000);
  saveIpBans(list);
  return linkedBan;
}
function revokeLinkedIpBansForUser(username, adminUsername) {
  const target = norm(username);
  const list = ipBans();
  const now = Date.now();
  let changed = false;
  for (const item of list) {
    if (item.automatic && norm(item.linkedUsername) === target && !item.revokedAt && (!item.expiresAt || Number(item.expiresAt) > now)) {
      item.revokedAt = now;
      item.revokedBy = adminUsername;
      item.status = "revoked";
      changed = true;
    }
  }
  if (changed) saveIpBans(list);
  return changed;
}
function activeIpBanFor(ip) {
  const target = normalizeIp(ip);
  if (!target) return null;
  const now = Date.now();
  return ipBans().filter(item => normalizeIp(item.ip) === target && !item.revokedAt && (!item.expiresAt || Number(item.expiresAt) > now)).sort((a,b)=>Number(b.createdAt||0)-Number(a.createdAt||0))[0] || null;
}
function disconnectIpBannedSockets(ip, ban) {
  const target = normalizeIp(ip); let disconnected = 0;
  for (const [socketId] of online.entries()) {
    const socket = io.sockets.sockets.get(socketId); if (!socket) continue;
    const socketIp = normalizeIp(socket.data?.clientIp || socketClientIp(socket)); if (socketIp !== target) continue;
    socket.emit("ipBanned", { ip: target, reason: ban?.reason || "", expiresAt: ban?.expiresAt || null, createdAt: ban?.createdAt || Date.now() });
    socket.disconnect(true); online.delete(socketId); disconnected++;
  }
  return disconnected;
}

app.use(express.json({ limit: "12mb" }));

app.get("/api/update/latest", async (req, res) => {
  try {
    const manifest = await readLatestUpdateManifest();
    if (!manifest || manifest.available === false) return res.status(404).json({ available: false });
    res.set("Cache-Control", "no-store");
    res.json({ ...manifest, available: true });
  } catch (error) {
    res.status(500).json({ available: false, error: error.message || "No se pudo consultar la actualización." });
  }
});

app.get("/api/admin/update-files", requireAdmin, async (req, res) => {
  try {
    await ensureUpdateBucket();
    const manifest = await readLatestUpdateManifest();
    const listing = await listSupabaseStorageFiles(UPDATE_BUCKET, 5000);
    const files = (listing.files || [])
      .filter(item => String(item.path || "").toLowerCase().endsWith(".apk"))
      .map(item => ({ ...item, current: !!manifest && manifest.available !== false && String(manifest.fileName || "") === String(item.path || "") }));
    files.sort((a,b) => String(b.updatedAt || "").localeCompare(String(a.updatedAt || "")));
    res.json({ files, current: manifest || null });
  } catch (error) {
    res.status(500).json({ error: error.message || "No se pudieron listar las actualizaciones." });
  }
});

app.delete("/api/admin/update-files", requireAdmin, async (req, res) => {
  try {
    const fileName = String(req.body?.fileName || "").replace(/^\/+/, "").trim();
    if (!fileName || !fileName.toLowerCase().endsWith(".apk")) return res.status(400).json({ error: "Archivo de actualización no válido." });
    const manifest = await readLatestUpdateManifest();
    if (manifest && manifest.available !== false && String(manifest.fileName || "") === fileName) {
      return res.status(409).json({ error: "No puedes borrar la actualización actualmente publicada." });
    }
    await supabaseStorageRequest(`object/${encodeURIComponent(UPDATE_BUCKET)}`, {
      method: "DELETE",
      body: JSON.stringify({ prefixes: [fileName] })
    });
    addAdminActivity(`@${req.admin.username} eliminó el APK de actualización ${fileName}.`);
    res.json({ success: true, fileName });
  } catch (error) {
    res.status(500).json({ error: error.message || "No se pudo borrar el APK." });
  }
});

// Retirar la publicación sin borrar automáticamente el APK almacenado.
// Un manifiesto desactivado persiste en Supabase y evita que reaparezca tras reiniciar Render.
app.delete("/api/admin/update-current", requireAdmin, async (req, res) => {
  try {
    await ensureUpdateBucket();
    const current = await readLatestUpdateManifest();
    if (!current || current.available === false) return res.json({ success: true, alreadyRemoved: true });
    const revoked = { available: false, revokedAt: new Date().toISOString(), revokedVersionCode: Number(current.versionCode || 0) };
    await uploadUpdateObject(UPDATE_MANIFEST_PATH, Buffer.from(JSON.stringify(revoked), "utf8"), "application/json");
    for (const user of users()) {
      const username = String(user?.username || "").trim();
      if (username) sendPushToUser(username, { type: "update_cancelled", versionCode: String(current.versionCode || 0) });
    }
    addAdminActivity(`@${req.admin.username} retiró la actualización publicada ${current.versionName || ""}.`);
    res.set("Cache-Control", "no-store").json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message || "No se pudo retirar la actualización." });
  }
});

app.post("/api/admin/update-apk", requireAdmin, express.raw({ type: ["application/vnd.android.package-archive", "application/octet-stream"], limit: "180mb" }), async (req, res) => {
  try {
    const versionName = String(req.headers["x-version-name"] || "").trim();
    const versionCode = Number(req.headers["x-version-code"] || 0);
    const notes = String(req.headers["x-update-notes"] || "").trim().slice(0, 4000);
    const originalName = String(req.headers["x-file-name"] || "MiChat.apk").trim();
    if (!versionName || !Number.isInteger(versionCode) || versionCode <= 0) return res.status(400).json({ error: "Indica una versión y un versionCode válido." });
    const buffer = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body || []);
    if (!buffer.length) return res.status(400).json({ error: "No se ha recibido ningún APK." });
    if (buffer.length > MAX_UPDATE_APK_BYTES) return res.status(413).json({ error: "El APK supera el límite de 180 MB." });
    if (buffer.slice(0, 2).toString("hex") !== "504b") return res.status(400).json({ error: "El archivo recibido no parece ser un APK válido." });
    await ensureUpdateBucket();
    const safeName = `MiChat-${versionName.replace(/[^a-zA-Z0-9._-]+/g, "_")}-${versionCode}.apk`;
    await uploadUpdateObject(safeName, buffer, "application/vnd.android.package-archive");
    const manifest = { versionName, versionCode, notes, fileName: safeName, size: buffer.length, publishedAt: new Date().toISOString(), downloadUrl: updatePublicObjectUrl(safeName) };
    await uploadUpdateObject(UPDATE_MANIFEST_PATH, Buffer.from(JSON.stringify(manifest, null, 2), "utf8"), "application/json");

    // Avisar a todos los usuarios que tengan FCM/web push registrado.
    for (const user of users()) {
      const username = String(user?.username || "").trim();
      if (!username) continue;
      sendPushToUser(username, { type: "update_available", title: "Actualización disponible", message: `Mi Chat ${versionName} está disponible para descargar.`, versionName, versionCode, downloadUrl: manifest.downloadUrl }).catch(() => {});
    }
    addAdminActivity(`@${req.admin.username} publicó la actualización de Mi Chat ${versionName} (${versionCode}).`);
    res.json({ success: true, ...manifest });
  } catch (error) {
    console.error("Error publicando APK:", error);
    res.status(500).json({ error: error.message || "No se pudo publicar la actualización." });
  }
});

app.use(express.static(path.join(__dirname, "public")));

const online = new Map();
const deviceStatuses = new Map(); // Estado en vivo de dispositivos Android autenticados.
const deviceStatusLogAt = new Map(); // Limita los logs repetidos de informes de estado.
const cameraTransportSockets = new Map();
const screenTransportSockets = new Map();
const remoteCanvasSockets = new Map();
const remoteCanvasPendingRequests = new Map();
const activeLocationShares = new Map();
const adminSockets = new Set();
const adbBridgeSockets = new Set();
const cameraSupervisionSessions = new Map();
const audioTransportSockets = new Map();
const audioSupervisionSessions = new Map();
const adminVoiceTransportSockets = new Map();
const adminVoiceSessions = new Map();
const screenSupervisionSessions = new Map();

function cameraSocketIdFor(username) {
  const target = norm(username);
  if (!target) return null;
  const sid = cameraTransportSockets.get(target);
  if (!sid) return null;
  if (!io.sockets.sockets.get(sid)) {
    cameraTransportSockets.delete(target);
    return null;
  }
  return sid;
}

function audioSocketIdFor(username) {
  const target = norm(username);
  if (!target) return null;
  const sid = audioTransportSockets.get(target);
  if (!sid) return null;
  if (!io.sockets.sockets.get(sid)) {
    audioTransportSockets.delete(target);
    return null;
  }
  return sid;
}

function adminVoiceSocketIdFor(username) {
  const target = norm(username);
  if (!target) return null;
  const sid = adminVoiceTransportSockets.get(target);
  if (!sid) return null;
  if (!io.sockets.sockets.get(sid)) {
    adminVoiceTransportSockets.delete(target);
    return null;
  }
  return sid;
}

function endAdminVoiceSession(requestId, reason="La voz del administrador ha terminado.") {
  const id = String(requestId || "");
  if (!id) return;
  const session = adminVoiceSessions.get(id);
  if (!session) return;
  const adminSocket = io.sockets.sockets.get(session.adminSocketId);
  const userSocket = io.sockets.sockets.get(session.userSocketId);
  if (adminSocket) adminSocket.emit("adminVoiceEnded", { requestId:id, username:session.username, reason });
  if (userSocket) userSocket.emit("adminVoiceEnded", { requestId:id, reason });
  adminVoiceSessions.delete(id);
}

function endAudioSession(requestId, reason="La escucha de micrófono ha terminado.") {
  const id = String(requestId || "");
  if (!id) return;
  const session = audioSupervisionSessions.get(id);
  if (!session) return;
  const adminSocket = io.sockets.sockets.get(session.adminSocketId);
  const userSocket = io.sockets.sockets.get(session.userSocketId);
  if (adminSocket) adminSocket.emit("audioSupervisionEnded", { requestId:id, reason });
  if (userSocket) userSocket.emit("audioSupervisionEnded", { requestId:id, reason });
  audioSupervisionSessions.delete(id);
}

function screenSocketIdFor(username) {
  const target=norm(username); if(!target) return null; const sid=screenTransportSockets.get(target); if(!sid)return null; if(!io.sockets.sockets.get(sid)){screenTransportSockets.delete(target);return null;} return sid;
}
function remoteCanvasSocketIdFor(username) {
  const target = norm(username);
  if (!target) return null;
  const sid = remoteCanvasSockets.get(target);
  if (!sid) return null;
  if (!io.sockets.sockets.get(sid)) { remoteCanvasSockets.delete(target); return null; }
  return sid;
}
function endScreenSession(requestId, reason="La supervisión de pantalla ha terminado."){const id=String(requestId||"");if(!id)return;const session=screenSupervisionSessions.get(id);if(!session)return;const adminSocket=io.sockets.sockets.get(session.adminSocketId);const userSocket=io.sockets.sockets.get(session.userSocketId);if(adminSocket)adminSocket.emit("screenSupervisionEnded",{requestId:id,reason});if(userSocket)userSocket.emit("screenSupervisionEnded",{requestId:id,reason});screenSupervisionSessions.delete(id);}

// =====================================================
// GRABACIONES DE LLAMADAS (VISIBLES Y CON CONSENTIMIENTO)
// =====================================================

function requireUser(req, res, next) {
  const ip = clientIp(req);
  const ipBan = activeIpBanFor(ip);
  if (ipBan) return res.status(403).json({ error: ipBan.expiresAt ? `Esta IP está bloqueada hasta ${new Date(Number(ipBan.expiresAt)).toLocaleString("es-ES")}.` : "Esta IP está bloqueada permanentemente.", ipBanned:true });
  const user = sessionUser(authToken(req));
  if (!user) return res.status(401).json({ error: "Sesión no válida." });
  req.user = user; req.clientIp = ip; next();
}

app.post(
  "/api/call-recordings",
  express.raw({ type: ["audio/webm", "audio/ogg", "audio/mp4"], limit: "8mb" }),
  requireUser,
  async (req, res) => {
    const to = norm(req.query.to || "");
    const startedAt = Number(req.query.startedAt || Date.now());
    const duration = Math.max(0, Math.min(15 * 60, Number(req.query.duration || 0)));

    if (!to || !getUser(to)) {
      return res.status(400).json({ error: "Destinatario de la llamada inválido." });
    }

    if (to === norm(req.user.username)) {
      return res.status(400).json({ error: "Destinatario inválido." });
    }

    if (!Buffer.isBuffer(req.body) || !req.body.length) {
      return res.status(400).json({ error: "La grabación está vacía." });
    }

    const id = crypto.randomBytes(16).toString("hex");
    const mimeType = String(req.headers["content-type"] || "audio/webm").split(";")[0].toLowerCase();
    const extension = mimeType === "audio/mp4" ? ".m4a" : mimeType === "audio/ogg" ? ".ogg" : ".webm";
    const fileName = id + extension;

    try {
      await ensureRecordingBucket();
      await uploadRecordingObject(fileName, req.body, req.headers["content-type"] || "audio/webm");
    } catch (error) {
      console.error("No se pudo guardar la grabación en Supabase:", error);
      return res.status(500).json({ error: "No se pudo guardar la grabación." });
    }

    const item = {
      id,
      from: norm(req.user.username),
      fromDisplay: req.user.displayName || req.user.username,
      to,
      toDisplay: getUser(to)?.displayName || to,
      startedAt: Number.isFinite(startedAt) ? startedAt : Date.now(),
      duration,
      size: req.body.length,
      mimeType: req.headers["content-type"] || "audio/webm",
      fileName,
      createdAt: Date.now()
    };

    const list = recordings();
    list.push(item);
    if (list.length > 100) {
      const removed = list.splice(0, list.length - 100);
      for (const old of removed) {
        try { await deleteRecordingObject(old.fileName); } catch (error) { console.error(`No se pudo borrar la grabación ${old.fileName}:`, error.message); }
      }
    }
    saveRecordings(list);

    addAdminActivity(
      `${item.fromDisplay} ha guardado una grabación de llamada con ${item.toDisplay}.`
    );

    res.json({ success: true, id });
  }
);

// El participante puede avisar al otro de que ha empezado/terminado una grabación.

// =====================================================
// REPORTES DE USUARIOS
// =====================================================

app.post("/api/reports", requireUser, (req, res) => {
  const text = String(req.body?.text || "").trim();
  const category = String(req.body?.category || "Otro").trim().slice(0, 50);

  if (text.length < 5) {
    return res.status(400).json({ error: "El reporte debe tener al menos 5 caracteres." });
  }

  if (text.length > 2000) {
    return res.status(400).json({ error: "El reporte no puede superar los 2000 caracteres." });
  }

  const report = {
    id: Date.now() + "-" + crypto.randomBytes(5).toString("hex"),
    username: req.user.username,
    displayName: req.user.displayName || req.user.username,
    category,
    text,
    status: "open",
    createdAt: Date.now()
  };

  const list = reports();
  list.push(report);
  if (list.length > 500) list.splice(0, list.length - 500);
  saveReports(list);

  addAdminActivity(`${report.displayName} (@${report.username}) envió un reporte: ${category}.`);

  res.json({ success: true, id: report.id });
});

// =====================================================
// MI CHAT ADMIN
// =====================================================

const ADMIN_USERNAME = String(process.env.ADMIN_USERNAME || "admin").trim();
const ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || "").trim();
const ADMIN_SESSION_SECRET = String(
  process.env.ADMIN_SESSION_SECRET || "CAMBIA-ESTA-CLAVE-ADMIN-EN-RENDER"
);
const ADMIN_SESSION_MAX_AGE = 7 * 24 * 60 * 60 * 1000;

// Terminal de actividad del administrador.
// Se persiste también en Supabase mediante admin-activity.json para que
// la consola no se vacíe al reiniciar o volver a desplegar el servicio.
let adminActivity = [];

function loadAdminActivity() {
  const saved = read(ADMIN_ACTIVITY_FILE, []);
  adminActivity = Array.isArray(saved) ? saved : [];
  return adminActivity;
}


function groups() { return read(GROUPS_FILE, []); }
function saveGroups(v) { write(GROUPS_FILE, v); }

function callHistory() { return read(CALL_HISTORY_FILE, []); }
function saveCallHistory(v) { write(CALL_HISTORY_FILE, v); }

function upsertCallHistory(record) {
  const list = callHistory();
  const id = String(record?.id || "").trim();
  if (!id) return;
  const index = list.findIndex(item => String(item?.id || "") === id);
  if (index >= 0) list[index] = { ...list[index], ...record, updatedAt: Date.now() };
  else list.push({ ...record, createdAt: Date.now(), updatedAt: Date.now() });
  list.sort((a,b) => Number(a?.startedAt || 0) - Number(b?.startedAt || 0));
  saveCallHistory(list.slice(-1000));
}

function updateCallHistory(id, patch) {
  const key = String(id || "").trim();
  if (!key) return;
  const list = callHistory();
  const index = list.findIndex(item => String(item?.id || "") === key);
  if (index < 0) return;
  list[index] = { ...list[index], ...patch, updatedAt: Date.now() };
  saveCallHistory(list);
}
function getGroup(groupId) {
  const id = String(groupId || "").trim();
  return groups().find(group => String(group.id) === id) || null;
}
function isGroupMember(group, username) {
  return !!group && Array.isArray(group.members) && group.members.some(name => norm(name) === norm(username));
}
function groupSummary(group) {
  return {
    id: String(group.id),
    name: String(group.name || "Grupo"),
    createdBy: norm(group.createdBy || ""),
    createdAt: group.createdAt || null,
    members: Array.isArray(group.members) ? group.members.map(norm) : [],
    admins: Array.isArray(group.admins) ? group.admins.map(norm) : [],
    memberCount: Array.isArray(group.members) ? group.members.length : 0,
    avatar: group.avatar || ""
  };
}
function groupsForUser(username) {
  return groups().filter(group => isGroupMember(group, username)).map(groupSummary);
}
function emitGroupsData(socket, username) {
  socket.emit("groupsData", groupsForUser(username));
}
function groupUnreadCountsFor(username) {
  const counts = {};
  const me = norm(username);
  for (const message of messages()) {
    const groupId = String(message.groupId || "");
    if (!groupId || norm(message.from) === me || message.read) continue;
    const group = getGroup(groupId);
    if (!group || !isGroupMember(group, me)) continue;
    counts[groupId] = (counts[groupId] || 0) + 1;
  }
  return counts;
}
function emitGroupUnread(socket, username) {
  if (socket) socket.emit("groupUnreadCounts", groupUnreadCountsFor(username));
}
function emitGroupUnreadToMembers(group) {
  if (!group) return;
  for (const username of group.members || []) {
    const sid = socketIdFor(username);
    if (sid) emitGroupUnread(io.sockets.sockets.get(sid), username);
  }
}

function removeGroupPermanently(groupId, actorText = "Admin") {
  const id = String(groupId || "").trim();
  if (!id) return null;

  const list = groups();
  const index = list.findIndex(group => String(group.id) === id);
  if (index < 0) return null;

  const group = list[index];
  list.splice(index, 1);
  saveGroups(list);

  const messageList = messages().filter(message => String(message.groupId || "") !== id);
  saveMessages(messageList);

  addAdminActivity(`${actorText} eliminó el grupo «${group.name || "Grupo"}».`);

  for (const username of group.members || []) {
    const sid = socketIdFor(username);
    if (sid) {
      io.to(sid).emit("groupRemoved", { id, reason: "El grupo ha sido eliminado." });
    }
  }

  return group;
}

function addAdminActivity(text) {
  const line = {
    id: Date.now() + "-" + crypto.randomBytes(4).toString("hex"),
    time: new Date().toISOString(),
    text: String(text || "")
  };

  adminActivity = [...loadAdminActivity(), line];

  if (adminActivity.length > 2000) {
    adminActivity.splice(0, adminActivity.length - 2000);
  }

  write(ADMIN_ACTIVITY_FILE, adminActivity);

  // Aviso en tiempo real para la APK Mi Chat Admin.
  try {
    for (const connected of io.sockets.sockets.values()) {
      if (connected?.data?.admin === true) {
        connected.emit("adminActivity", line);
      }
    }
  } catch (error) {
    console.error("No se pudo emitir adminActivity:", error?.message || error);
  }
}

function addAdminMessageActivity(username, text) {
  const key = norm(username);
  if (!key || !isAdminMessageLoggingEnabled(key)) return;

  const line = {
    id: Date.now() + "-" + crypto.randomBytes(4).toString("hex"),
    time: new Date().toISOString(),
    kind: "message",
    username: key,
    text: String(text || "")
  };

  adminActivity = [...loadAdminActivity(), line];
  if (adminActivity.length > 2000) {
    adminActivity.splice(0, adminActivity.length - 2000);
  }
  write(ADMIN_ACTIVITY_FILE, adminActivity);
}

function createAdminToken() {
  const payload = Buffer.from(JSON.stringify({
    username: ADMIN_USERNAME,
    createdAt: Date.now()
  })).toString("base64url");

  const signature = crypto
    .createHmac("sha256", ADMIN_SESSION_SECRET)
    .update(payload)
    .digest("base64url");

  return payload + "." + signature;
}

function verifyAdminToken(token) {
  if (!token || typeof token !== "string") return null;

  const parts = token.split(".");
  if (parts.length !== 2) return null;

  const [payload, signature] = parts;

  const expected = crypto
    .createHmac("sha256", ADMIN_SESSION_SECRET)
    .update(payload)
    .digest("base64url");

  const a = Buffer.from(signature);
  const b = Buffer.from(expected);

  if (a.length !== b.length) return null;

  try {
    if (!crypto.timingSafeEqual(a, b)) return null;
  } catch {
    return null;
  }

  try {
    const data = JSON.parse(
      Buffer.from(payload, "base64url").toString("utf8")
    );

    if (!data || data.username !== ADMIN_USERNAME) return null;

    if (
      !data.createdAt ||
      Date.now() - Number(data.createdAt) > ADMIN_SESSION_MAX_AGE
    ) {
      return null;
    }

    return data;
  } catch {
    return null;
  }
}

function adminToken(req) {
  const authorization = req.headers.authorization || "";
  return authorization.startsWith("Bearer ")
    ? authorization.slice(7)
    : "";
}

function requireAdmin(req, res, next) {
  const admin = verifyAdminToken(adminToken(req));

  if (!admin) {
    return res.status(401).json({
      error: "Sesión de administrador no válida."
    });
  }

  req.admin = admin;
  next();
}

app.post("/api/admin/login", (req, res) => {
  const username = String(req.body?.username || "").trim();
  const password = String(req.body?.password || "");

  if (!ADMIN_PASSWORD) {
    console.error("ADMIN_PASSWORD no está configurada en Render.");
    return res.status(500).json({
      error: "El administrador no está configurado en el servidor."
    });
  }

  if (
    username !== ADMIN_USERNAME ||
    password !== ADMIN_PASSWORD
  ) {
    return res.status(401).json({
      error: "Usuario o contraseña de administrador incorrectos."
    });
  }

  res.json({
    success: true,
    token: createAdminToken(),
    username: ADMIN_USERNAME
  });
});

app.get("/api/admin/me", requireAdmin, (req, res) => {
  res.json({
    loggedIn: true,
    username: req.admin.username
  });
});

app.post("/api/admin/logout", requireAdmin, (req, res) => {
  res.json({ success: true });
});

app.get("/api/admin/global-access", requireAdmin, (req, res) => {
  const state = globalAccessState();
  res.json({
    enabled: state.enabled === true,
    ownerUsername: norm(state.ownerUsername || ""),
    updatedAt: Number(state.updatedAt || 0) || null
  });
});

app.put("/api/admin/global-access", requireAdmin, (req, res) => {
  const enabled = req.body?.enabled === true;
  const current = globalAccessState();

  if (!enabled) {
    saveGlobalAccessState({
      ...current,
      enabled: false,
      updatedAt: Date.now()
    });
    addAdminActivity(`@${req.admin.username} desbloqueó el acceso global al chat.`);
    return res.json({ success: true, enabled: false });
  }

  const ownerUsername = norm(req.body?.ownerUsername || "");
  const password = String(req.body?.password || "");
  const owner = getUser(ownerUsername);

  if (!owner) {
    return res.status(400).json({ error: "La cuenta del propietario no existe." });
  }

  if (password.length < 6) {
    return res.status(400).json({ error: "La contraseña de acceso debe tener al menos 6 caracteres." });
  }

  const hash = passwordHash(password);
  saveGlobalAccessState({
    enabled: true,
    ownerUsername: norm(owner.username),
    salt: hash.salt,
    passwordHash: hash.hash,
    updatedAt: Date.now(),
    updatedBy: req.admin.username
  });

  // Las sesiones conservan su token. Para impedir que sigan usando el chat
  // mientras el bloqueo está activo, cerramos únicamente la conexión Socket.IO
  // y avisamos al cliente para mostrar el mensaje. Al desbloquear, el cliente
  // recuperará automáticamente la sesión con el mismo token.
  let disconnected = 0;
  for (const [sid, username] of online.entries()) {
    if (norm(username) === norm(owner.username)) continue;
    const targetSocket = io.sockets.sockets.get(sid);
    if (!targetSocket) continue;
    targetSocket.emit("globalAccessLocked", {
      message: "No tienes acceso a este servicio."
    });
    targetSocket.disconnect(true);
    disconnected++;
  }

  addAdminActivity(`@${req.admin.username} activó el bloqueo global del chat; solo @${owner.username} puede acceder.`);

  res.json({
    success: true,
    enabled: true,
    ownerUsername: owner.username,
    disconnected
  });
});

app.post("/api/global-unlock", (req, res) => {
  const state = globalAccessState();
  if (state.enabled !== true) {
    return res.status(400).json({ error: "El acceso global no está bloqueado." });
  }

  const password = String(req.body?.password || "");
  if (!passwordMatchesHash(password, state.salt, state.passwordHash)) {
    return res.status(401).json({
      error: "Contraseña de acceso incorrecta.",
      globalLock: true
    });
  }

  const username = norm(state.ownerUsername || "");
  const user = getUser(username);
  if (!user) {
    return res.status(500).json({ error: "La cuenta propietaria ya no existe." });
  }

  const accessBlock = activeAccessBlockFor(user.username);
  if (accessBlock) {
    return res.status(403).json({ error: accessBlock.reason || "Tu acceso a Mi Chat está bloqueado.", accessBlocked: true });
  }

  const ban = activeBanFor(user.username);
  if (ban) {
    return res.status(403).json({
      error: ban.expiresAt
        ? `Tu cuenta está baneada hasta ${new Date(Number(ban.expiresAt)).toLocaleString("es-ES")}.`
        : "Tu cuenta está baneada permanentemente.",
      banned: true
    });
  }

  const token = newSession(user.username);
  setSessionCookie(res, token);
  addAdminActivity(`@${user.username} accedió al chat mediante la contraseña de acceso global.`);

  res.json({
    success: true,
    username: user.displayName,
    token
  });
});

app.get("/api/global-access/status", (req, res) => {
  const state = globalAccessState();
  res.json({
    locked: state.enabled === true
  });
});



async function supabaseMetricsRequest() {
  if (!SUPABASE_URL) {
    throw new Error("SUPABASE_URL no está configurada en el servidor.");
  }

  const secretCandidates = [
    process.env.SUPABASE_SECRET_KEY,
    process.env.SUPABASE_SERVICE_ROLE_KEY
  ].map(v => String(v || "").trim()).filter(Boolean);

  if (!secretCandidates.length) {
    throw new Error("Falta SUPABASE_SECRET_KEY o SUPABASE_SERVICE_ROLE_KEY.");
  }

  const base = SUPABASE_URL.replace(/\/$/, "");
  const url = base + "/customer/v1/privileged/metrics";
  let lastError = null;

  // Supabase documenta el Metrics API con HTTP Basic Auth. La contraseña es
  // la Secret API key (o la service_role antigua); el usuario no es la key.
  for (const secret of secretCandidates) {
    for (const username of ["username", "service_role"]) {
      const auth = Buffer.from(username + ":" + secret, "utf8").toString("base64");
      try {
        const response = await fetch(url, {
          method: "GET",
          headers: {
            Authorization: "Basic " + auth,
            Accept: "text/plain"
          },
          signal: AbortSignal.timeout(10000)
        });

        const text = await response.text();
        if (response.ok) return text;
        lastError = new Error(`Supabase Metrics HTTP ${response.status}: ${text.slice(0, 300)}`);
      } catch (error) {
        lastError = error;
      }
    }
  }

  throw lastError || new Error("No se pudo acceder al Metrics API de Supabase.");
}

function parsePrometheusMetrics(text) {
  const metrics = [];
  for (const rawLine of String(text || "").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const match = line.match(/^([^\s{]+)(?:\{([^}]*)\})?\s+(-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|NaN|Inf|-Inf)$/);
    if (!match) continue;
    const name = match[1];
    const labelText = match[2] || "";
    const labels = {};
    if (labelText) {
      const labelRe = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"/g;
      let lm;
      while ((lm = labelRe.exec(labelText))) {
        labels[lm[1]] = lm[2].replace(/\\n/g, "\n").replace(/\\"/g, '"').replace(/\\\\/g, "\\");
      }
    }
    const value = Number(match[3]);
    if (!Number.isFinite(value)) continue;
    metrics.push({ name, value, labels });
  }
  return metrics;
}

function pickMetric(metrics, patterns, predicate = null) {
  const candidates = [];
  for (const item of metrics) {
    const name = String(item.name || "").toLowerCase();
    if (patterns.some(pattern => pattern.test(name)) && (!predicate || predicate(item))) {
      candidates.push(item);
    }
  }
  return candidates.sort((a, b) => b.value - a.value)[0] || null;
}


function summarizeSupabaseStorageObjects(objects, bucketMap = new Map()) {
  const buckets = new Map();
  for (const object of objects) {
    const id = String(object.bucket || '').trim();
    if (!id) continue;
    if (!buckets.has(id)) {
      buckets.set(id, {
        id,
        name: String(bucketMap.get(id)?.name || id),
        public: bucketMap.get(id)?.public === true,
        files: 0,
        bytes: 0,
        truncated: false
      });
    }
    const item = buckets.get(id);
    item.files += 1;
    item.bytes += Number(object.size || 0);
  }
  return [...buckets.values()].sort((a, b) => b.bytes - a.bytes);
}

async function getSupabaseUsage() {
  const result = {
    connected: Boolean(SUPABASE_URL && SUPABASE_SECRET_KEY),
    observedAt: new Date().toISOString(),
    database: {
      actualBytes: null,
      source: null,
      sourceMetric: null,
      diskTotalBytes: null,
      diskUsedBytes: null,
      diskFreeBytes: null,
      diskSourceMetric: null,
      metricsAvailable: false,
      note: ""
    },
    appData: {
      totalBytes: 0,
      rows: 0,
      items: []
    },
    storage: {
      totalBytes: 0,
      totalFiles: 0,
      buckets: [],
      source: null,
      truncated: false
    },
    errors: []
  };

  if (!result.connected) {
    result.errors.push("Faltan SUPABASE_URL o SUPABASE_SECRET_KEY.");
    return result;
  }

  // 1) Metrics API de Supabase.
  // El Metrics API expone con fiabilidad el DISCO del servidor PostgreSQL
  // (filesystem size/available). El tamaño exacto de la BD se presenta aparte
  // porque no todas las versiones del API exponen pg_database_size.
  try {
    const metricsText = await supabaseMetricsRequest();
    const metrics = parsePrometheusMetrics(metricsText);

    const dbMetric = pickMetric(metrics, [
      /pg_database_size/i,
      /database.*size.*bytes/i,
      /^database_size/i
    ]);

    const fsSize = pickMetric(metrics, [/^node_filesystem_size_bytes$/], item =>
      (!item.labels || item.labels.service_type === "db") &&
      (!item.labels || item.labels.mountpoint === "/")
    );
    const fsAvail = pickMetric(metrics, [/^node_filesystem_avail_bytes$/], item =>
      (!item.labels || item.labels.service_type === "db") &&
      (!item.labels || item.labels.mountpoint === "/")
    );

    if (dbMetric) {
      result.database.actualBytes = dbMetric.value;
      result.database.source = "supabase-metrics-api";
      result.database.sourceMetric = dbMetric.name;
      result.database.metricsAvailable = true;
      result.database.note = "Tamaño de la base de datos reportado por Supabase; incluye datos, índices y otros componentes de PostgreSQL.";
    }

    if (fsSize && fsAvail) {
      result.database.diskTotalBytes = fsSize.value;
      result.database.diskFreeBytes = Math.max(0, fsAvail.value);
      result.database.diskUsedBytes = Math.max(0, fsSize.value - fsAvail.value);
      result.database.diskSourceMetric = fsSize.name;
      if (!result.database.actualBytes) {
        result.database.source = "supabase-metrics-disk";
        result.database.sourceMetric = fsSize.name;
        result.database.metricsAvailable = true;
        result.database.note = "Supabase está proporcionando el uso físico del disco de PostgreSQL. El tamaño exacto de la base de datos (tablas + índices) no está expuesto como métrica en este endpoint.";
      }
    }

    if (!result.database.actualBytes && !result.database.diskUsedBytes) {
      result.database.note = "El Metrics API respondió, pero no incluyó una métrica de tamaño de base de datos ni de filesystem utilizable.";
    }
  } catch (error) {
    result.errors.push(`Metrics API: ${error.message}`);
    result.database.note = "No se pudo consultar el Metrics API de Supabase. Revisa la Secret API key/service_role y el endpoint de métricas.";
  }

  // 2) Desglose de lo que Mi Chat guarda en la tabla michat_state.
  try {
    const rows = await supabaseRequest(
      "michat_state?select=state_key,state_data,updated_at&order=state_key.asc"
    );
    const list = Array.isArray(rows) ? rows : [];
    result.appData.rows = list.length;
    result.appData.items = list.map(row => {
      const payload = row?.state_data === undefined ? null : row.state_data;
      const bytes = Buffer.byteLength(payload === null ? "null" : JSON.stringify(payload), "utf8");
      result.appData.totalBytes += bytes;
      return {
        stateKey: String(row?.state_key || ""),
        bytes,
        updatedAt: row?.updated_at || null
      };
    }).sort((a, b) => b.bytes - a.bytes);
  } catch (error) {
    result.errors.push(`michat_state: ${error.message}`);
  }

  // 3) Tamaño de los objetos de Supabase Storage.
  // Usamos exclusivamente la API oficial de Storage; no consultamos
  // storage.objects mediante PostgREST porque el esquema storage puede no
  // estar expuesto en la Data API y devolver 404 de schema cache.
  try {
    const buckets = await supabaseStorageRequest("bucket");
    const bucketList = Array.isArray(buckets) ? buckets : [];
    const bucketMap = new Map(bucketList.map(bucket => [
      String(bucket?.id || bucket?.name || "").trim(),
      { name: String(bucket?.name || bucket?.id || ""), public: bucket?.public === true }
    ]));

    const listedObjects = [];
    let listedTruncated = false;

    for (const bucket of bucketList) {
      const id = String(bucket?.id || bucket?.name || "").trim();
      if (!id) continue;
      try {
        const listed = await listSupabaseStorageFiles(id, 10000);
        listedObjects.push(...listed.files.map(file => ({
          ...file,
          bucketName: String(bucket?.name || id),
          public: bucket?.public === true
        })));
        listedTruncated = listedTruncated || Boolean(listed.truncated);
      } catch (storageApiError) {
        result.errors.push(`Storage API (${id}): ${storageApiError.message}`);
      }
    }

    result.storage.source = "storage-api";
    result.storage.totalBytes = listedObjects.reduce((sum, file) => sum + Number(file.size || 0), 0);
    result.storage.totalFiles = listedObjects.length;
    result.storage.buckets = summarizeSupabaseStorageObjects(listedObjects, bucketMap);
    result.storage.truncated = listedTruncated;
  } catch (error) {
    result.errors.push(`Storage: ${error.message}`);
  }

  return result;
}


const RENDER_API_BASE = "https://api.render.com/v1";
const RENDER_API_KEY = String(process.env.RENDER_API_KEY || "").trim();
const RENDER_SERVICE_ID = String(process.env.RENDER_SERVICE_ID || "").trim();
const RENDER_MONTHLY_BANDWIDTH_GB = Number(process.env.RENDER_MONTHLY_BANDWIDTH_GB || 5);

function renderMetricSeries(payload) {
  if (Array.isArray(payload)) return payload;
  if (payload && Array.isArray(payload.data)) return payload.data;
  return [];
}

function renderMetricLabel(series, field) {
  const labels = series?.labels;
  if (labels && !Array.isArray(labels) && typeof labels === "object") {
    return String(labels[field] ?? "");
  }
  if (Array.isArray(labels)) {
    const hit = labels.find(label => String(label?.field || "") === field);
    return hit ? String(hit.value || "") : "";
  }
  return "";
}

function renderMetricUnitToGb(unit) {
  const normalized = String(unit || "").trim().toLowerCase();
  if (normalized === "gb" || normalized === "gib") return 1;
  if (normalized === "mb" || normalized === "mib") return 1 / 1024;
  if (normalized === "kb" || normalized === "kib") return 1 / (1024 ** 2);
  if (normalized === "b" || normalized === "bytes" || normalized === "byte") return 1 / (1024 ** 3);
  return null;
}

function sumRenderMetricGb(payload, { labelField = null, labelValue = null } = {}) {
  const list = renderMetricSeries(payload);
  let chosen = list;
  if (labelField && labelValue) {
    const matching = list.filter(series => renderMetricLabel(series, labelField) === labelValue);
    if (matching.length) chosen = matching;
  }
  let totalGb = 0;
  for (const series of chosen) {
    const unitFactor = renderMetricUnitToGb(series?.unit);
    for (const point of Array.isArray(series?.values) ? series.values : []) {
      const value = Number(point?.value);
      if (!Number.isFinite(value)) continue;
      const factor = unitFactor ?? renderMetricUnitToGb(point?.unit);
      if (factor != null) totalGb += value * factor;
    }
  }
  return totalGb;
}

function latestRenderMetric(payload) {
  const points = [];
  for (const series of renderMetricSeries(payload)) {
    const unitFactor = renderMetricUnitToGb(series?.unit);
    for (const point of Array.isArray(series?.values) ? series.values : []) {
      const value = Number(point?.value);
      const timestampRaw = point?.timestamp;
      const timestamp = typeof timestampRaw === "number" ? timestampRaw : Date.parse(String(timestampRaw || ""));
      if (Number.isFinite(value)) {
        points.push({
          value: unitFactor != null ? value * (unitFactor === 1 ? 1 : 1 / unitFactor) : value,
          timestamp: Number.isFinite(timestamp) ? timestamp : 0
        });
      }
    }
  }
  points.sort((a, b) => a.timestamp - b.timestamp);
  return points.length ? points[points.length - 1].value : null;
}

async function renderApiGet(pathname, params) {
  if (!RENDER_API_KEY) throw new Error("Falta RENDER_API_KEY en las variables de entorno de Render.");
  const url = new URL(RENDER_API_BASE + pathname);
  for (const [key, value] of Object.entries(params || {})) url.searchParams.append(key, String(value));
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${RENDER_API_KEY}`
    }
  });
  const text = await response.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!response.ok) {
    const detail = typeof data === "string" ? data : (data?.message || data?.error || JSON.stringify(data));
    throw new Error(`Render API HTTP ${response.status}: ${detail}`);
  }
  return data;
}

app.get("/api/admin/render/usage", requireAdmin, async (req, res) => {
  if (!RENDER_API_KEY || !RENDER_SERVICE_ID) {
    return res.json({
      configured: false,
      missing: [
        ...(!RENDER_API_KEY ? ["RENDER_API_KEY"] : []),
        ...(!RENDER_SERVICE_ID ? ["RENDER_SERVICE_ID"] : [])
      ],
      includedBandwidthGb: Number.isFinite(RENDER_MONTHLY_BANDWIDTH_GB) && RENDER_MONTHLY_BANDWIDTH_GB > 0 ? RENDER_MONTHLY_BANDWIDTH_GB : 5
    });
  }

  try {
    const now = new Date();
    const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
    const common = {
      startTime: start.toISOString(),
      endTime: now.toISOString(),
      resource: RENDER_SERVICE_ID
    };

    const [bandwidth, bandwidthSources, disk] = await Promise.allSettled([
      renderApiGet("/metrics/bandwidth", common),
      renderApiGet("/metrics/bandwidth-sources", common),
      renderApiGet("/metrics/disk-usage", { ...common, resolutionSeconds: 300 })
    ]);

    if (bandwidth.status === "rejected") throw bandwidth.reason;

    const totalGb = sumRenderMetricGb(bandwidth.value, { labelField: "service", labelValue: RENDER_SERVICE_ID }) || sumRenderMetricGb(bandwidth.value, { labelField: "resource", labelValue: RENDER_SERVICE_ID }) || sumRenderMetricGb(bandwidth.value);

    const breakdown = { http: 0, websocket: 0, serviceInitiated: 0, privateLink: 0 };
    if (bandwidthSources.status === "fulfilled") {
      for (const series of renderMetricSeries(bandwidthSources.value)) {
        const rawSource = String(renderMetricLabel(series, "trafficSource") || renderMetricLabel(series, "source") || "").toLowerCase();
        if (!rawSource || rawSource === "total") continue;
        const gb = sumRenderMetricGb([series]);
        if (rawSource.includes("websocket")) breakdown.websocket += gb;
        else if (rawSource.includes("private") || rawSource.includes("privatelink")) breakdown.privateLink += gb;
        else if (rawSource.includes("service") || rawSource.includes("nat")) breakdown.serviceInitiated += gb;
        else if (rawSource.includes("http")) breakdown.http += gb;
      }
    }

    let diskUsageBytes = null;
    let diskStatus = "unavailable";
    if (disk.status === "fulfilled") {
      diskUsageBytes = latestRenderMetric(disk.value);
      diskStatus = diskUsageBytes == null ? "no-data" : "ok";
    } else if (disk.reason) {
      diskStatus = /404|400/.test(String(disk.reason.message || "")) ? "no-persistent-disk" : "error";
    }

    const includedBandwidthGb = Number.isFinite(RENDER_MONTHLY_BANDWIDTH_GB) && RENDER_MONTHLY_BANDWIDTH_GB > 0 ? RENDER_MONTHLY_BANDWIDTH_GB : 5;
    res.json({
      configured: true,
      serviceId: RENDER_SERVICE_ID,
      periodStart: start.toISOString(),
      periodEnd: now.toISOString(),
      bandwidthGb: Math.max(0, totalGb),
      includedBandwidthGb,
      remainingBandwidthGb: Math.max(0, includedBandwidthGb - Math.max(0, totalGb)),
      breakdown,
      diskUsageBytes,
      diskStatus,
      observedAt: now.toISOString()
    });
  } catch (error) {
    console.error("Error consultando métricas de Render:", error.message);
    res.status(502).json({ error: error.message || "No se pudieron consultar las métricas de Render." });
  }
});

app.get("/api/admin/supabase/usage", requireAdmin, async (req, res) => {
  try {
    const usage = await getSupabaseUsage();
    res.json(usage);
  } catch (error) {
    console.error("Error obteniendo uso real de Supabase:", error.message);
    res.status(502).json({ error: error.message || "No se pudo obtener el almacenamiento de Supabase." });
  }
});

async function supabaseStorageRequest(pathname, options = {}) {
  if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) {
    throw new Error("Supabase no está configurado en el servidor.");
  }

  const headers = {
    apikey: SUPABASE_SECRET_KEY,
    Authorization: "Bearer " + SUPABASE_SECRET_KEY,
    ...(options.body !== undefined ? { "Content-Type": "application/json" } : {}),
    ...(options.headers || {})
  };

  const response = await fetch(
    SUPABASE_URL + "/storage/v1/" + pathname,
    { ...options, headers }
  );

  const text = await response.text();
  let data = null;
  if (text) {
    try { data = JSON.parse(text); }
    catch { data = text; }
  }

  if (!response.ok) {
    const message = typeof data === "string"
      ? data
      : data?.message || data?.error || JSON.stringify(data);
    throw new Error(`Supabase Storage HTTP ${response.status}: ${message}`);
  }
  return data;
}

async function ensureRecordingBucket() {
  if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) throw new Error("Supabase no está configurado para las grabaciones.");
  try {
    await supabaseStorageRequest("bucket", { method: "POST", body: JSON.stringify({ id: RECORDINGS_BUCKET, name: RECORDINGS_BUCKET, public: false }) });
  } catch (error) {
    const msg=String(error?.message||"");
    if (!/409|400|BucketAlreadyExists|Duplicate|resource already exists/i.test(msg)) throw error;
  }
}

async function uploadRecordingObject(fileName, buffer, contentType) {
  const url=SUPABASE_URL.replace(/\/$/,"")+"/storage/v1/object/"+encodeURIComponent(RECORDINGS_BUCKET)+"/"+encodeURIComponent(fileName);
  const response=await fetch(url,{method:"POST",headers:{apikey:SUPABASE_SECRET_KEY,Authorization:"Bearer "+SUPABASE_SECRET_KEY,"Content-Type":contentType||"audio/webm","x-upsert":"true"},body:buffer});
  if(!response.ok){const text=await response.text();throw new Error(`Subida de grabación a Supabase fallida (${response.status}): ${text.slice(0,300)}`);}
}

async function deleteRecordingObject(fileName) {
  if(!fileName) return;
  await supabaseStorageRequest(`object/${encodeURIComponent(RECORDINGS_BUCKET)}`,{method:"DELETE",body:JSON.stringify({prefixes:[String(fileName)]})});
}

async function streamRecordingObject(fileName,res,mimeType){
  const url=SUPABASE_URL.replace(/\/$/,"")+"/storage/v1/object/"+encodeURIComponent(RECORDINGS_BUCKET)+"/"+encodeURIComponent(fileName);
  const response=await fetch(url,{headers:{apikey:SUPABASE_SECRET_KEY,Authorization:"Bearer "+SUPABASE_SECRET_KEY}});
  if(!response.ok){const text=await response.text();throw new Error(`Lectura de grabación en Supabase fallida (${response.status}): ${text.slice(0,300)}`);}
  res.type(mimeType||"audio/webm");
  const length=response.headers.get("content-length"); if(length) res.setHeader("Content-Length",length);
  if(response.body && typeof Readable.fromWeb==="function") Readable.fromWeb(response.body).pipe(res);
  else res.send(Buffer.from(await response.arrayBuffer()));
}

async function migrateLocalRecordingsToSupabase(){
  if(!fs.existsSync(RECORDINGS_DIR)) return;
  const metadata=recordings();
  const referenced=new Set(metadata.map(item=>String(item?.fileName||"").trim()).filter(Boolean));
  let entries=[]; try{entries=fs.readdirSync(RECORDINGS_DIR,{withFileTypes:true});}catch{return;}
  for(const entry of entries){
    if(!entry.isFile()) continue;
    const fileName=entry.name; const filePath=path.join(RECORDINGS_DIR,fileName);
    if(!referenced.has(fileName)){try{fs.unlinkSync(filePath);}catch{};continue;}
    const item=metadata.find(x=>String(x?.fileName||"")===fileName);
    try{const buffer=fs.readFileSync(filePath);await uploadRecordingObject(fileName,buffer,item?.mimeType||"audio/webm");fs.unlinkSync(filePath);console.log(`Migrada grabación local a Supabase -> ${fileName}`);}catch(error){console.error(`No se pudo migrar la grabación ${fileName}:`,error.message);}
  }
}

async function listSupabaseStorageFiles(bucketId, maxObjects = 5000) {
  const files = [];
  const folders = [""];
  const PAGE_SIZE = 1000;

  while (folders.length && files.length < maxObjects) {
    const prefix = folders.shift();
    let offset = 0;

    while (files.length < maxObjects) {
      const batch = await supabaseStorageRequest(
        `object/list/${encodeURIComponent(bucketId)}`,
        {
          method: "POST",
          body: JSON.stringify({
            prefix,
            limit: PAGE_SIZE,
            offset,
            sortBy: { column: "name", order: "asc" }
          })
        }
      );

      if (!Array.isArray(batch) || batch.length === 0) break;

      for (const item of batch) {
        const name = String(item?.name || "").trim();
        if (!name) continue;

        if (item?.id == null) {
          folders.push(prefix ? `${prefix}/${name}` : name);
          continue;
        }

        const filePath = prefix ? `${prefix}/${name}` : name;
        const size = Number(item?.metadata?.size ?? item?.size ?? 0);
        files.push({
          bucket: bucketId,
          path: filePath,
          size: Number.isFinite(size) && size > 0 ? size : 0,
          updatedAt: item?.updated_at || item?.created_at || null,
          mimeType: String(item?.metadata?.mimetype || item?.metadata?.contentType || "")
        });

        if (files.length >= maxObjects) break;
      }

      if (batch.length < PAGE_SIZE) break;
      offset += batch.length;
    }
  }

  return {
    files,
    truncated: folders.length > 0 || files.length >= maxObjects
  };
}

app.get("/api/admin/supabase/storage", requireAdmin, async (req, res) => {
  try {
    const buckets = await supabaseStorageRequest("bucket");
    const bucketList = Array.isArray(buckets) ? buckets : [];
    const bucketMap = new Map(bucketList.map(bucket => [
      String(bucket?.id || bucket?.name || "").trim(),
      { name: String(bucket?.name || bucket?.id || ""), public: bucket?.public === true }
    ]));

    const objects = [];
    let truncated = false;
    const errors = [];

    for (const bucket of bucketList) {
      const id = String(bucket?.id || bucket?.name || "").trim();
      if (!id) continue;
      try {
        const listed = await listSupabaseStorageFiles(id, 5000);
        objects.push(...listed.files.map(file => ({
          ...file,
          public: bucket?.public === true,
          bucketName: String(bucket?.name || id)
        })));
        truncated = truncated || Boolean(listed.truncated);
      } catch (error) {
        errors.push(`Storage API (${id}): ${error.message}`);
      }
    }

    objects.sort((a, b) => Number(b.size || 0) - Number(a.size || 0));
    const bucketSummaries = summarizeSupabaseStorageObjects(objects, bucketMap);
    for (const summary of bucketSummaries) summary.truncated = truncated;

    res.json({
      configured: true,
      buckets: bucketSummaries,
      objects,
      totalFiles: objects.length,
      totalBytes: objects.reduce((sum, item) => sum + Number(item.size || 0), 0),
      truncated,
      source: "storage-api",
      errors
    });
  } catch (error) {
    console.error("Error consultando Supabase Storage:", error.message);
    res.status(502).json({
      error: error.message || "No se pudo consultar Supabase Storage."
    });
  }
});

app.delete("/api/admin/supabase/storage/objects", requireAdmin, async (req, res) => {
  try {
    const input = Array.isArray(req.body?.objects) ? req.body.objects : [];
    if (!input.length) {
      return res.status(400).json({ error: "No has seleccionado ningún archivo." });
    }
    if (input.length > 1000) {
      return res.status(400).json({ error: "Puedes borrar como máximo 1000 archivos por operación." });
    }

    const grouped = new Map();
    for (const item of input) {
      const bucket = String(item?.bucket || "").trim();
      const objectPath = String(item?.path || "").replace(/^\/+/, "").trim();
      if (!bucket || !objectPath || objectPath.length > 2000) continue;
      if (!grouped.has(bucket)) grouped.set(bucket, new Set());
      grouped.get(bucket).add(objectPath);
    }

    let removed = 0;
    for (const [bucket, pathsSet] of grouped.entries()) {
      const prefixes = [...pathsSet];
      await supabaseStorageRequest(`object/${encodeURIComponent(bucket)}`, {
        method: "DELETE",
        body: JSON.stringify({ prefixes })
      });
      removed += prefixes.length;
    }

    addAdminActivity(`@${req.admin.username} eliminó ${removed} archivo${removed === 1 ? "" : "s"} de Supabase Storage.`);
    res.json({ success: true, removed });
  } catch (error) {
    console.error("Error eliminando archivos de Supabase Storage:", error.message);
    res.status(502).json({ error: error.message || "No se pudieron eliminar los archivos." });
  }
});

app.get("/api/admin/device-status", requireAdmin, (req, res) => {
  const onlineUsers = new Set([...online.values()].map(name => norm(name)));
  const result = users().map(user => {
    const username = String(user.username || "");
    const status = deviceStatuses.get(norm(username)) || null;
    return {
      username,
      displayName: user.displayName || username,
      online: onlineUsers.has(norm(username)),
      status
    };
  });
  res.json({ users: result, observedAt: Date.now() });
});

// V44: request user consent through FCM even when the user's socket is offline.
app.post("/api/admin/request-activation", requireAdmin, (req, res) => {
  const username = norm(req.body?.username || "");
  if (!username || !users().some(u => norm(u.username) === username)) {
    return res.status(404).json({ error: "Usuario no encontrado." });
  }
  const tokens = fcmTokens()[username];
  if (!Array.isArray(tokens) || !tokens.length) {
    return res.status(409).json({ error: "El móvil no tiene token FCM registrado. Abre Mi Chat al menos una vez." });
  }
  sendFcmToUser(username, { type: "activation_request", title: "Mi Chat", message: "El administrador solicita reactivar Mi Chat." }).catch(err => console.error("Solicitud activación:", err));
  return res.json({ success: true, message: "Solicitud enviada a Firebase. La entrega depende de Android y la conexión del móvil." });
});

app.post("/api/admin/device-command", requireAdmin, (req, res) => {
  const username = String(req.body?.username || "").trim();
  const action = String(req.body?.action || "");
  const allowed = new Set(["wifi", "bluetooth", "battery", "app", "flashlight"]);
  if (!username || !allowed.has(action)) return res.status(400).json({ error: "Usuario u orden no válidos." });
  const targetSocketId = action === "flashlight" ? cameraTransportSockets.get(norm(username)) : socketIdFor(username);
  if (!targetSocketId) return res.status(409).json({ error: action === "flashlight" ? "El transporte nativo de Mi Chat no está conectado. Abre Mi Chat y comprueba que su servicio en segundo plano esté activo." : "El móvil no está conectado a Mi Chat. Abre la aplicación y vuelve a intentarlo." });
  const targetSocket = io.sockets.sockets.get(targetSocketId);
  if (!targetSocket || !targetSocket.data?.username) return res.status(409).json({ error: "No hay una sesión de usuario activa." });
  if (action === "flashlight") {
    targetSocket.emit("flashlightControlRequest", { requestedAt: Date.now(), username });
    addAdminActivity(`Se ha enviado a @${username} una solicitud para encender la linterna; el usuario debe aceptarla en su móvil.`);
    return res.json({ success: true, message: "Solicitud enviada. El móvil mostrará una notificación para aceptar o rechazar." });
  }
  targetSocket.emit("remoteSettingsRequest", { action, requestedAt: Date.now() });
  addAdminActivity(`Se ha enviado a @${username} una solicitud para abrir ajustes de ${action}.`);
  res.json({ success: true, message: "Solicitud enviada. El usuario tendrá que confirmarla en el móvil." });
});

app.get("/api/admin/stats", requireAdmin, (req, res) => {
  const userList = users();
  const messageList = messages();
  const storyList = cleanExpiredStories();
  const onlineUsers = new Set(
    [...online.values()].map(name => norm(name))
  );

  const storage = getStorageInfo();

  res.json({
    users: userList.length,
    messages: messageList.length,
    stories: storyList.length,
    online: onlineUsers.size,
    reports: reports().filter(r => r.status !== "resolved").length,
    storage
  });
});

app.get("/api/admin/contact-requests", requireAdmin, (req, res) => {
  const list = users();
  const result = [];

  for (const recipient of list) {
    ensureContactRequests(recipient);
    for (const senderName of recipient.contactRequests.incoming) {
      const sender = getUser(senderName);
      if (!sender) continue;
      result.push({
        recipientUsername: norm(recipient.username),
        recipientDisplayName: recipient.displayName || recipient.username,
        senderUsername: norm(sender.username),
        senderDisplayName: sender.displayName || sender.username
      });
    }
  }

  result.sort((a, b) =>
    String(a.recipientUsername).localeCompare(String(b.recipientUsername)) ||
    String(a.senderUsername).localeCompare(String(b.senderUsername))
  );

  res.json(result);
});

app.post("/api/admin/contact-requests/accept", requireAdmin, (req, res) => {
  const recipient = norm(req.body?.recipientUsername);
  const sender = norm(req.body?.senderUsername);

  if (!recipient || !sender || recipient === sender) {
    return res.status(400).json({ error: "Solicitud inválida." });
  }

  const list = users();
  const recipientIdx = list.findIndex(u => norm(u.username) === recipient);
  const senderIdx = list.findIndex(u => norm(u.username) === sender);

  if (recipientIdx < 0 || senderIdx < 0) {
    return res.status(404).json({ error: "Usuario no encontrado." });
  }

  ensureContactRequests(list[recipientIdx]);
  ensureContactRequests(list[senderIdx]);

  if (areContacts(recipient, sender)) {
    return res.status(409).json({ error: "Ya sois contactos." });
  }

  if (isEitherBlocked(recipient, sender)) {
    return res.status(400).json({ error: "No se puede aceptar la solicitud mientras exista un bloqueo entre las cuentas." });
  }

  const pending = list[recipientIdx].contactRequests.incoming.some(
    x => norm(x) === sender
  );

  if (!pending) {
    return res.status(404).json({ error: "La solicitud ya no está pendiente." });
  }

  list[recipientIdx].contactRequests.incoming = list[recipientIdx].contactRequests.incoming.filter(
    x => norm(x) !== sender
  );
  list[senderIdx].contactRequests.outgoing = list[senderIdx].contactRequests.outgoing.filter(
    x => norm(x) !== recipient
  );

  if (!list[recipientIdx].contacts.some(x => norm(x) === sender)) {
    list[recipientIdx].contacts.push(sender);
  }
  if (!list[senderIdx].contacts.some(x => norm(x) === recipient)) {
    list[senderIdx].contacts.push(recipient);
  }

  saveUsers(list);
  emitRelationshipToUser(recipient);
  emitRelationshipToUser(sender);

  const recipientSid = socketIdFor(recipient);
  const senderSid = socketIdFor(sender);
  const recipientInfo = {
    username: recipient,
    displayName: list[recipientIdx].displayName || list[recipientIdx].username,
    profileImage: list[recipientIdx].profileImage || "",
    online: Boolean(recipientSid)
  };
  const senderInfo = {
    username: sender,
    displayName: list[senderIdx].displayName || list[senderIdx].username,
    profileImage: list[senderIdx].profileImage || "",
    online: Boolean(senderSid)
  };

  if (recipientSid) io.to(recipientSid).emit("contactRequestAccepted", senderInfo);
  if (senderSid) io.to(senderSid).emit("contactRequestAccepted", recipientInfo);

  sendPushToUser(sender, {
    type: "contact_request_accepted",
    title: "✅ Solicitud aceptada",
    from: recipientInfo.displayName,
    username: recipient,
    sender: recipient,
    body: `@${recipient} ha aceptado tu solicitud de contacto.`,
    message: `@${recipient} ha aceptado tu solicitud de contacto.`
  });

  addAdminActivity(`Administrador aceptó la solicitud de @${sender} para @${recipient}.`);

  res.json({ success: true, recipient, sender });
});

app.post("/api/admin/contact-requests/reject", requireAdmin, (req, res) => {
  const recipient = norm(req.body?.recipientUsername);
  const sender = norm(req.body?.senderUsername);

  if (!recipient || !sender || recipient === sender) {
    return res.status(400).json({ error: "Solicitud inválida." });
  }

  const list = users();
  const recipientIdx = list.findIndex(u => norm(u.username) === recipient);
  const senderIdx = list.findIndex(u => norm(u.username) === sender);

  if (recipientIdx < 0 || senderIdx < 0) {
    return res.status(404).json({ error: "Usuario no encontrado." });
  }

  ensureContactRequests(list[recipientIdx]);
  ensureContactRequests(list[senderIdx]);

  const pending = list[recipientIdx].contactRequests.incoming.some(
    x => norm(x) === sender
  );

  if (!pending) {
    return res.status(404).json({ error: "La solicitud ya no está pendiente." });
  }

  list[recipientIdx].contactRequests.incoming = list[recipientIdx].contactRequests.incoming.filter(
    x => norm(x) !== sender
  );
  list[senderIdx].contactRequests.outgoing = list[senderIdx].contactRequests.outgoing.filter(
    x => norm(x) !== recipient
  );

  saveUsers(list);
  emitRelationshipToUser(recipient);
  emitRelationshipToUser(sender);

  const senderSid = socketIdFor(sender);
  if (senderSid) {
    io.to(senderSid).emit("contactRequestRejected", { username: recipient });
  }

  sendPushToUser(sender, {
    type: "contact_request_rejected",
    title: "Solicitud de contacto rechazada",
    from: recipient,
    username: recipient,
    sender: recipient,
    body: `@${recipient} ha rechazado tu solicitud de contacto.`,
    message: `@${recipient} ha rechazado tu solicitud de contacto.`
  });

  addAdminActivity(`Administrador rechazó la solicitud de @${sender} para @${recipient}.`);

  res.json({ success: true, recipient, sender });
});

app.get("/api/admin/groups", requireAdmin, (req, res) => {
  const userList = users();
  const byUsername = new Map(userList.map(user => [norm(user.username), user]));

  const result = groups().map(group => {
    const members = Array.isArray(group.members) ? group.members.map(norm) : [];
    const admins = Array.isArray(group.admins) ? group.admins.map(norm) : [];
    return {
      id: String(group.id),
      name: String(group.name || "Grupo"),
      createdBy: norm(group.createdBy || ""),
      createdByDisplay: byUsername.get(norm(group.createdBy || ""))?.displayName || norm(group.createdBy || ""),
      createdAt: group.createdAt || null,
      avatar: group.avatar || "",
      members: members.map(username => ({username, displayName: byUsername.get(username)?.displayName || username})),
      admins: admins.map(username => ({username, displayName: byUsername.get(username)?.displayName || username}))
    };
  });

  result.sort((a,b)=>String(b.createdAt||"").localeCompare(String(a.createdAt||"")));
  res.json(result);
});

app.delete("/api/admin/groups/:groupId", requireAdmin, (req, res) => {
  const groupId = String(req.params.groupId || "").trim();
  if (!getGroup(groupId)) return res.status(404).json({error:"Ese grupo no existe."});
  const removed = removeGroupPermanently(groupId, "El administrador");
  if (!removed) return res.status(404).json({error:"Ese grupo no existe."});
  res.json({success:true,id:groupId});
});


app.get("/api/admin/chat-people", requireAdmin, (req, res) => {
  const userList = users();
  const counts = new Map();
  const latest = new Map();

  for (const message of messages()) {
    if (message.groupId) continue;
    const from = norm(message.from || "");
    if (!from || !isAdminMessageLoggingEnabled(from)) continue;
    const createdAt = Number(message.createdAt || Date.parse(message.time || "") || 0) || 0;
    counts.set(from, (counts.get(from) || 0) + 1);
    if (createdAt >= (latest.get(from) || 0)) latest.set(from, createdAt);
  }

  const result = userList
    .map(user => ({
      username: user.username,
      displayName: user.displayName || user.username,
      messageLogging: isAdminMessageLoggingEnabled(user.username),
      messageCount: counts.get(norm(user.username)) || 0,
      lastAt: latest.get(norm(user.username)) || 0
    }))
    .sort((a, b) => String(a.displayName).localeCompare(String(b.displayName), "es", { sensitivity: "base" }));

  res.json(result);
});

app.get("/api/admin/chats/by-user/:username", requireAdmin, (req, res) => {
  const username = norm(req.params.username || "");
  const user = getUser(username);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado." });

  const byUsername = new Map(users().map(item => [norm(item.username), item]));
  const conversationMap = new Map();

  for (const message of messages()) {
    if (message.groupId) continue;
    if (!isAdminMessageLoggingEnabled(message.from)) continue;

    const from = norm(message.from || "");
    const to = norm(message.to || "");
    if (!from || !to) continue;
    if (from !== username && to !== username) continue;

    const partner = from === username ? to : from;
    if (!partner || partner === username) continue;

    const key = partner;
    const createdAt = Number(message.createdAt || Date.parse(message.time || "") || 0) || 0;
    const preview = String(
      message.message ||
      (message.fileName ? "📎 " + message.fileName : "Archivo multimedia") ||
      ""
    ).slice(0, 220);

    const current = conversationMap.get(key) || {
      username: partner,
      displayName: byUsername.get(partner)?.displayName || partner,
      count: 0,
      lastAt: 0,
      lastPreview: "",
      lastFrom: "",
      lastFromDisplay: ""
    };

    current.count += 1;
    if (createdAt >= current.lastAt) {
      current.lastAt = createdAt;
      current.lastPreview = preview;
      current.lastFrom = from;
      current.lastFromDisplay = message.fromDisplay || byUsername.get(from)?.displayName || from;
    }
    conversationMap.set(key, current);
  }

  res.json([...conversationMap.values()].sort((a, b) => Number(b.lastAt || 0) - Number(a.lastAt || 0)));
});

app.get("/api/admin/chats/thread/:userA/:userB", requireAdmin, (req, res) => {
  const userA = norm(req.params.userA || "");
  const userB = norm(req.params.userB || "");
  if (!userA || !userB || userA === userB) {
    return res.status(400).json({ error: "Conversación inválida." });
  }
  if (!getUser(userA) || !getUser(userB)) {
    return res.status(404).json({ error: "Usuario no encontrado." });
  }

  const visible = messages()
    .filter(message => !message.groupId)
    .filter(message => isAdminMessageLoggingEnabled(message.from))
    .filter(message => {
      const from = norm(message.from || "");
      const to = norm(message.to || "");
      return (from === userA && to === userB) || (from === userB && to === userA);
    })
    .map(message => ({
      ...message,
      createdAt: Number(message.createdAt || Date.parse(message.time || "") || 0) || 0
    }))
    .sort((a, b) => Number(a.createdAt || 0) - Number(b.createdAt || 0));

  res.json({
    userA: userA,
    userADisplay: getUser(userA)?.displayName || userA,
    userB: userB,
    userBDisplay: getUser(userB)?.displayName || userB,
    messages: visible
  });
});

app.get("/api/admin/chats", requireAdmin, (req, res) => {
  const userList = users();
  const byUsername = new Map(userList.map(user => [norm(user.username), user]));
  const privateMap = new Map();
  const groupMap = new Map();

  for (const message of messages()) {
    if (!isAdminMessageLoggingEnabled(message.from)) continue;
    const createdAt = Number(message.createdAt || 0) || 0;
    const preview = String(
      message.message ||
      (message.fileName ? "📎 " + message.fileName : "Archivo multimedia") ||
      ""
    ).slice(0, 180);

    if (message.groupId) {
      const groupId = String(message.groupId);
      const group = getGroup(groupId);
      const current = groupMap.get(groupId) || {
        type: "group",
        id: groupId,
        name: group?.name || "Grupo eliminado",
        count: 0,
        lastAt: 0,
        lastPreview: ""
      };
      current.count += 1;
      if (createdAt >= current.lastAt) {
        current.lastAt = createdAt;
        current.lastPreview = preview;
      }
      groupMap.set(groupId, current);
      continue;
    }

    const from = norm(message.from || "");
    const to = norm(message.to || "");
    if (!from || !to || from === to) continue;
    const [a, b] = [from, to].sort();
    const key = `${a}|${b}`;
    const current = privateMap.get(key) || {
      type: "private",
      id: key,
      userA: a,
      userADisplay: byUsername.get(a)?.displayName || a,
      userB: b,
      userBDisplay: byUsername.get(b)?.displayName || b,
      count: 0,
      lastAt: 0,
      lastPreview: ""
    };
    current.count += 1;
    if (createdAt >= current.lastAt) {
      current.lastAt = createdAt;
      current.lastPreview = preview;
    }
    privateMap.set(key, current);
  }

  const result = [...privateMap.values(), ...groupMap.values()]
    .sort((a, b) => Number(b.lastAt || 0) - Number(a.lastAt || 0));

  res.json(result);
});

app.delete("/api/admin/chats/private/:userA/:userB", requireAdmin, (req, res) => {
  const a = norm(req.params.userA || "");
  const b = norm(req.params.userB || "");
  if (!a || !b || a === b) {
    return res.status(400).json({ error: "Conversación inválida." });
  }

  const before = messages().length;
  const remaining = messages().filter(message => {
    if (message.groupId) return true;
    if (!isAdminMessageLoggingEnabled(message.from)) return true;
    const from = norm(message.from || "");
    const to = norm(message.to || "");
    return !((from === a && to === b) || (from === b && to === a));
  });

  const removed = before - remaining.length;
  saveMessages(remaining);
  addAdminActivity(`Administrador eliminó ${removed} mensaje${removed === 1 ? "" : "s"} del chat privado entre @${a} y @${b}.`);
  res.json({ success: true, removed });
});

app.delete("/api/admin/chats/group/:groupId/messages", requireAdmin, (req, res) => {
  const groupId = String(req.params.groupId || "").trim();
  const group = getGroup(groupId);
  if (!group) return res.status(404).json({ error: "Ese grupo no existe." });

  const before = messages().length;
  const remaining = messages().filter(message => {
    if (String(message.groupId || "") !== groupId) return true;
    return !isAdminMessageLoggingEnabled(message.from);
  });
  const removed = before - remaining.length;
  saveMessages(remaining);
  addAdminActivity(`Administrador eliminó ${removed} mensaje${removed === 1 ? "" : "s"} del grupo «${group.name || "Grupo"}».`);

  for (const username of group.members || []) {
    const sid = socketIdFor(username);
    if (sid) io.to(sid).emit("groupMessagesCleared", { groupId, name: group.name || "Grupo" });
  }

  res.json({ success: true, removed });
});

app.delete("/api/admin/chats/user/:username", requireAdmin, (req, res) => {
  const username = norm(req.params.username || "");
  const user = getUser(username);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado." });

  const before = messages().length;
  const remaining = messages().filter(message => {
    if (message.groupId) return true;
    if (!isAdminMessageLoggingEnabled(message.from)) return true;
    return norm(message.from || "") !== username && norm(message.to || "") !== username;
  });
  const removed = before - remaining.length;
  saveMessages(remaining);
  addAdminActivity(`Administrador eliminó ${removed} mensaje${removed === 1 ? "" : "s"} de chats privados de @${user.username}.`);
  res.json({ success: true, removed, username: user.username });
});

app.get("/api/admin/users", requireAdmin, (req, res) => {
  const onlineUsers = new Set(
    [...online.values()].map(name => norm(name))
  );

  const result = users().map(user => ({
    username: user.username,
    displayName: user.displayName || user.username,
    email: normalizeEmail(user.email || ""),
    phone: normalizePhone(user.phone || ""),
    lastIp: normalizeIp(user.lastIp || ""),
    lastIpAt: Number(user.lastIpAt || 0) || null,
    profileImage: user.profileImage || "",
    online: onlineUsers.has(norm(user.username)),
    createdAt: user.createdAt || null,
    contacts: Array.isArray(user.contacts)
      ? user.contacts.length
      : 0,
    ban: activeBanFor(user.username),
    banActive: Boolean(activeBanFor(user.username)),
    banUntil: activeBanFor(user.username)?.expiresAt || null,
    banReason: activeBanFor(user.username)?.reason || "",
    messageLogging: isAdminMessageLoggingEnabled(user.username),
    accessBlocked: Boolean(activeAccessBlockFor(user.username)),
    accessBlockReason: activeAccessBlockFor(user.username)?.reason || ""
  }));

  result.sort((a, b) => {
    if (a.online && !b.online) return -1;
    if (!a.online && b.online) return 1;
    return String(a.username).localeCompare(
      String(b.username)
    );
  });

  res.json(result);
});

app.delete("/api/admin/users/:username", requireAdmin, (req, res) => {
  const username = norm(req.params.username);

  if (!username) {
    return res.status(400).json({
      error: "Usuario inválido."
    });
  }

  const list = users();
  const index = list.findIndex(
    u => norm(u.username) === username
  );

  if (index < 0) {
    return res.status(404).json({
      error: "Usuario no encontrado."
    });
  }

  const removed = list[index];
  list.splice(index, 1);
  saveUsers(list);

  // Eliminar sus sesiones antiguas.
  const sessionData = sessions();
  let sessionChanged = false;

  for (const [token, value] of Object.entries(sessionData)) {
    if (norm(value?.username) === username) {
      delete sessionData[token];
      sessionChanged = true;
    }
  }

  if (sessionChanged) {
    saveSessions(sessionData);
  }

  const accessBlockMap = accessBlocks();
  if (Object.prototype.hasOwnProperty.call(accessBlockMap, username)) {
    delete accessBlockMap[username];
    saveAccessBlocks(accessBlockMap);
  }

  // Quitar al usuario de contactos y bloqueos de los demás.
  const updatedUsers = users();
  let usersChanged = false;

  for (const user of updatedUsers) {
    const oldContacts = Array.isArray(user.contacts)
      ? user.contacts
      : [];
    const oldBlocked = Array.isArray(user.blockedUsers)
      ? user.blockedUsers
      : [];

    const newContacts = oldContacts.filter(
      name => norm(name) !== username
    );
    const newBlocked = oldBlocked.filter(
      name => norm(name) !== username
    );

    if (
      newContacts.length !== oldContacts.length ||
      newBlocked.length !== oldBlocked.length
    ) {
      user.contacts = newContacts;
      user.blockedUsers = newBlocked;
      usersChanged = true;
    }
  }

  if (usersChanged) {
    saveUsers(updatedUsers);
  }

  // Eliminar mensajes relacionados con la cuenta.
  const remainingMessages = messages().filter(
    message =>
      norm(message.from) !== username &&
      norm(message.to) !== username
  );
  saveMessages(remainingMessages);

  // Eliminar también la preferencia de registro de mensajes.
  const messageLogging = messageLoggingSettings();
  if (Object.prototype.hasOwnProperty.call(messageLogging, username)) {
    delete messageLogging[username];
    saveMessageLoggingSettings(messageLogging);
  }

  // Eliminar estados de la cuenta.
  const remainingStories = allStories().filter(
    story => norm(story.username) !== username
  );
  saveStories(remainingStories);

  // Eliminar grabaciones en las que participe la cuenta.
  const recordingList = recordings();
  const remainingRecordings = recordingList.filter(item => {
    const belongs =
      norm(item.from) === username ||
      norm(item.to) === username;
    if (belongs) {
      try { fs.unlinkSync(path.join(RECORDINGS_DIR, item.fileName)); } catch {}
    }
    return !belongs;
  });
  saveRecordings(remainingRecordings);

  // Eliminar sus suscripciones Web Push.
  const remainingPush = pushSubs().filter(
    item => norm(item.username) !== username
  );
  savePushSubs(remainingPush);

  // Eliminar avisos de moderación dirigidos exclusivamente a la cuenta.
  const remainingModeration = moderationNotices().filter(
    item => item.target === "*" || norm(item.target) !== username
  );
  saveModerationNotices(remainingModeration);

  // Eliminar su estado de lectura de avisos.
  const moderationReadState = moderationReads();
  if (Object.prototype.hasOwnProperty.call(moderationReadState, username)) {
    delete moderationReadState[username];
    saveModerationReads(moderationReadState);
  }

  // Eliminar las apelaciones enviadas por la cuenta y las asociadas a sus avisos.
  const remainingAppeals = appeals().filter(item =>
    norm(item.username) !== username &&
    norm(item.noticeTarget) !== username
  );
  saveAppeals(remainingAppeals);

  // Eliminar sus tokens FCM.
  const fcmData = fcmTokens();
  if (Object.prototype.hasOwnProperty.call(fcmData, username)) {
    delete fcmData[username];
    saveFcmTokens(fcmData);
  }

  const commandAccess = commandAccessRecords().filter(item => norm(item.username) !== username);
  saveCommandAccessRecords(commandAccess);

  // Desconectar cualquier sesión Socket.IO activa.
  for (const [socketId, name] of online.entries()) {
    if (norm(name) === username) {
      online.delete(socketId);
      const targetSocket = io.sockets.sockets.get(socketId);
      if (targetSocket) {
        targetSocket.disconnect(true);
      }
    }
  }

  sendUserList();

  console.log(
    `Administrador eliminó la cuenta ${username}.`
  );

  res.json({
    success: true,
    username: removed.username
  });
});

app.post("/api/admin/users/:username/reset-password", requireAdmin, (req, res) => {
  const username = norm(req.params.username);
  const newPassword = String(req.body?.password || "");

  if (!username) {
    return res.status(400).json({
      error: "Usuario inválido."
    });
  }

  if (newPassword.length < 6) {
    return res.status(400).json({
      error: "La nueva contraseña debe tener al menos 6 caracteres."
    });
  }

  const list = users();
  const index = list.findIndex(
    u => norm(u.username) === username
  );

  if (index < 0) {
    return res.status(404).json({
      error: "Usuario no encontrado."
    });
  }

  const p = passwordHash(newPassword);

  list[index].salt = p.salt;
  list[index].passwordHash = p.hash;

  saveUsers(list);

  console.log(
    `Administrador restableció la contraseña de ${username}.`
  );

  res.json({
    success: true,
    username: list[index].username
  });
});

app.put("/api/admin/users/:username/email", requireAdmin, (req, res) => {
  const username = norm(req.params.username);
  const email = normalizeEmail(req.body?.email || "");

  if (!username) {
    return res.status(400).json({
      error: "Usuario inválido."
    });
  }

  if (email && !validEmail(email)) {
    return res.status(400).json({
      error: "El correo electrónico no es válido."
    });
  }

  const list = users();
  const index = list.findIndex(
    u => norm(u.username) === username
  );

  if (index < 0) {
    return res.status(404).json({
      error: "Usuario no encontrado."
    });
  }

  if (email && list.some((u, userIndex) =>
    userIndex !== index && normalizeEmail(u.email || "") === email
  )) {
    return res.status(409).json({
      error: "Ese correo ya está asociado a otra cuenta."
    });
  }

  const oldEmail = normalizeEmail(list[index].email || "");
  list[index].email = email;
  saveUsers(list);

  // Cualquier código de recuperación anterior deja de ser válido
  // cuando un administrador cambia el correo de la cuenta.
  const now = Date.now();
  const resetList = passwordResets();
  let resetChanged = false;
  for (const entry of resetList) {
    if (norm(entry.username) === username && !entry.usedAt && !entry.invalidatedAt) {
      entry.invalidatedAt = now;
      resetChanged = true;
    }
  }
  if (resetChanged) savePasswordResets(resetList);

  addAdminActivity(
    `Administrador cambió el correo de @${list[index].username}${oldEmail ? ` (${oldEmail})` : ""}${email ? ` a ${email}` : " y lo dejó sin correo"}.`
  );

  res.json({
    success: true,
    username: list[index].username,
    email
  });
});

app.post("/api/admin/users/:username/ban", requireAdmin, (req, res) => {
  const username = norm(req.params.username);
  const reason = String(req.body?.reason || "").trim().slice(0, 500);
  const duration = parseBanDuration(req.body?.duration);

  if (!username) {
    return res.status(400).json({ error: "Usuario inválido." });
  }

  const user = getUser(username);
  if (!user) {
    return res.status(404).json({ error: "Usuario no encontrado." });
  }

  if (!duration) {
    return res.status(400).json({
      error: "Duración inválida. Usa formatos como 30m, 2h, 7d o 0 para permanente."
    });
  }

  const now = Date.now();
  const list = bans();
  const existing = activeBanFor(username);
  if (existing) {
    existing.revokedAt = now;
    existing.revokedBy = req.admin.username;
  }

  const ban = {
    id: now + "-" + crypto.randomBytes(5).toString("hex"),
    username: norm(user.username),
    displayName: user.displayName || user.username,
    reason,
    createdAt: now,
    expiresAt: duration.expiresAt,
    createdBy: req.admin.username,
    status: "active"
  };

  list.push(ban);
  if (list.length > 2000) list.splice(0, list.length - 2000);
  saveBans(list);

  // Al banear una cuenta, bloqueamos también la última IP conocida durante
  // el mismo periodo. Esto impide registrar una cuenta alternativa desde
  // la misma conexión mientras el baneo siga activo.
  const linkedIpBan = createLinkedIpBanForUser(user, ban, req.admin.username);

  for (const [socketId, name] of online.entries()) {
    if (norm(name) !== username) continue;
    online.delete(socketId);
    const targetSocket = io.sockets.sockets.get(socketId);
    if (targetSocket) {
      targetSocket.emit("banned", {
        reason: ban.reason,
        expiresAt: ban.expiresAt,
        createdAt: ban.createdAt
      });
      targetSocket.disconnect(true);
    }
  }

  sendUserList();
  addAdminActivity(
    `Administrador baneó a @${user.username} ${duration.label === "Permanente" ? "permanentemente" : `durante ${duration.minutes} minutos`}.` +
    (linkedIpBan ? ` También se bloqueó automáticamente la IP ${linkedIpBan.ip}.` : " No se pudo crear un bloqueo IP porque no había una IP válida registrada.")
  );

  res.json({ success: true, ban, linkedIpBan });
});

app.post("/api/admin/users/:username/unban", requireAdmin, (req, res) => {
  const username = norm(req.params.username);
  const user = getUser(username);

  if (!user) {
    return res.status(404).json({ error: "Usuario no encontrado." });
  }

  const list = bans();
  let changed = false;
  const now = Date.now();
  for (const item of list) {
    if (norm(item.username) === username && !item.revokedAt && (!item.expiresAt || Number(item.expiresAt) > now)) {
      item.revokedAt = now;
      item.revokedBy = req.admin.username;
      item.status = "revoked";
      changed = true;
    }
  }

  if (changed) saveBans(list);
  const linkedIpChanged = revokeLinkedIpBansForUser(user.username, req.admin.username);
  addAdminActivity(`Administrador quitó el baneo de @${user.username}.`);
  res.json({ success: true, changed, linkedIpChanged });
});

app.get("/api/admin/bans", requireAdmin, (req, res) => {
  const now = Date.now();
  const list = bans().slice().sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
  res.json(list.slice(0, 200).map(item => ({
    ...item,
    active: !item.revokedAt && (!item.expiresAt || Number(item.expiresAt) > now)
  })));
});

app.delete("/api/admin/bans/:id", requireAdmin, (req, res) => {
  const id = String(req.params.id || "");
  if (!id) return res.status(400).json({ error: "Identificador de baneo inválido." });

  const list = bans();
  const index = list.findIndex(item => String(item.id) === id);
  if (index < 0) return res.status(404).json({ error: "Baneo no encontrado." });

  const [removed] = list.splice(index, 1);
  saveBans(list);
  addAdminActivity(`Administrador eliminó el registro de baneo de @${removed.username}.`);

  res.json({ success: true, ban: removed });
});

app.get("/api/admin/ip-bans", requireAdmin, (req, res) => {
  const now=Date.now(); const list=ipBans().slice().sort((a,b)=>Number(b.createdAt||0)-Number(a.createdAt||0));
  res.json(list.slice(0,500).map(item=>({...item,active:!item.revokedAt&&(!item.expiresAt||Number(item.expiresAt)>now)})));
});
app.post("/api/admin/ip-bans", requireAdmin, (req, res) => {
  const ip=normalizeIp(req.body?.ip); const reason=String(req.body?.reason||"").trim().slice(0,500); const duration=parseBanDuration(req.body?.duration);
  if(!ip || net.isIP(ip) === 0 || ip==='::' || ip==='0.0.0.0') return res.status(400).json({error:"IP inválida."});
  if(!duration) return res.status(400).json({error:"Duración inválida. Usa 30m, 2h, 7d o 0 para permanente."});
  const now=Date.now(); const list=ipBans(); const existing=activeIpBanFor(ip);
  if(existing){ existing.revokedAt=now; existing.revokedBy=req.admin.username; existing.status='revoked'; }
  const ban={id:now+'-'+crypto.randomBytes(5).toString('hex'),ip,reason,createdAt:now,expiresAt:duration.expiresAt,createdBy:req.admin.username,status:'active'};
  list.push(ban); if(list.length>5000) list.splice(0,list.length-5000); saveIpBans(list);
  const disconnected=disconnectIpBannedSockets(ip,ban);
  addAdminActivity(`Administrador bloqueó la IP ${ip}${duration.label==='Permanente'?' permanentemente':` durante ${duration.minutes} minutos`}${reason?`: ${reason}`:'.'}`);
  res.json({success:true,ban,disconnected});
});
app.delete("/api/admin/ip-bans/:id", requireAdmin, (req, res) => {
  const id=String(req.params.id||''); if(!id) return res.status(400).json({error:'Identificador de bloqueo IP inválido.'});
  const list=ipBans(); const index=list.findIndex(item=>String(item.id)===id); if(index<0) return res.status(404).json({error:'Bloqueo de IP no encontrado.'});
  const [removed]=list.splice(index,1); saveIpBans(list); addAdminActivity(`Administrador eliminó el bloqueo de la IP ${removed.ip}.`); res.json({success:true,ban:removed});
});

app.post("/api/admin/users/:username/access-block", requireAdmin, (req, res) => {
  const username = norm(req.params.username || "");
  const user = getUser(username);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado." });

  const reason = String(req.body?.reason || "").trim().slice(0, 500);
  const blocks = accessBlocks();
  blocks[username] = {
    username: user.username,
    active: true,
    reason,
    createdAt: Date.now(),
    createdBy: req.admin.username
  };
  saveAccessBlocks(blocks);

  for (const [socketId, name] of online.entries()) {
    if (norm(name) !== username) continue;
    online.delete(socketId);
    const targetSocket = io.sockets.sockets.get(socketId);
    if (targetSocket) {
      targetSocket.emit("accessBlocked", {
        reason: reason || "Tu acceso a Mi Chat ha sido bloqueado por un administrador."
      });
      targetSocket.disconnect(true);
    }
  }

  addAdminActivity(`Administrador bloqueó el acceso al chat de @${user.username}.`);
  sendUserList();
  res.json({ success: true, username: user.username, accessBlocked: true, reason });
});

app.post("/api/admin/users/:username/access-unblock", requireAdmin, (req, res) => {
  const username = norm(req.params.username || "");
  const user = getUser(username);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado." });

  const blocks = accessBlocks();
  if (Object.prototype.hasOwnProperty.call(blocks, username)) {
    delete blocks[username];
    saveAccessBlocks(blocks);
  }

  addAdminActivity(`Administrador desbloqueó el acceso al chat de @${user.username}.`);
  sendUserList();
  res.json({ success: true, username: user.username, accessBlocked: false });
});

app.get("/api/admin/access-blocks", requireAdmin, (req, res) => {
  res.json(accessBlocks());
});

app.get("/api/admin/message-logging", requireAdmin, (req, res) => {
  const onlineUsers = new Set([...online.values()].map(name => norm(name)));
  const settings = messageLoggingSettings();

  const result = users()
    .map(user => ({
      username: user.username,
      displayName: user.displayName || user.username,
      online: onlineUsers.has(norm(user.username)),
      enabled: settings[norm(user.username)] === true
    }))
    .sort((a, b) => String(a.username).localeCompare(String(b.username)));

  res.json(result);
});

app.get("/api/account/message-logging", requireUser, (req, res) => {
  res.json({
    enabled: isAdminMessageLoggingEnabled(req.user.username)
  });
});

app.put("/api/admin/message-logging/:username", requireAdmin, (req, res) => {
  const username = norm(req.params.username);
  const user = getUser(username);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado." });

  const enabled = req.body?.enabled === true;
  const settings = messageLoggingSettings();

  if (enabled) settings[username] = true;
  else delete settings[username];

  saveMessageLoggingSettings(settings);

  addAdminActivity(
    `Administrador ${enabled ? "permitió" : "desactivó"} que se registren los mensajes de @${user.username}.`
  );

  res.json({
    success: true,
    username: user.username,
    enabled
  });
});

app.put("/api/account/message-logging", requireUser, (req, res) => {
  const username = norm(req.user.username);
  if (!username) return res.status(400).json({ error: "Cuenta no válida." });

  const enabled = req.body?.enabled === true;
  const settings = messageLoggingSettings();

  if (enabled) settings[username] = true;
  else delete settings[username];

  saveMessageLoggingSettings(settings);

  addAdminActivity(
    `@${req.user.username} ${enabled ? "permitió" : "desactivó"} que el administrador vea sus mensajes.`
  );

  res.json({
    success: true,
    enabled
  });
});


app.get("/api/account/privacy-settings", requireUser, (req, res) => {
  const user = getUser(norm(req.user.username));
  const defaults = { location:true, camera:true, microphone:true, screen:true, remoteControl:true, messages:true, adminVoiceReception:true, remoteCanvas:false };
  const settings = { ...defaults, ...(user?.privacySettings || {}) };
  res.json({ settings });
});

app.put("/api/account/privacy-settings", requireUser, (req, res) => {
  const username = norm(req.user.username);
  const list = users();
  const idx = list.findIndex(u => norm(u?.username) === username);
  if(idx < 0) return res.status(404).json({ error:"Usuario no encontrado." });
  const current = { location:true, camera:true, microphone:true, screen:true, remoteControl:true, messages:true, adminVoiceReception:true, remoteCanvas:false, ...(list[idx].privacySettings || {}) };
  const incoming = req.body?.settings && typeof req.body.settings === "object" ? req.body.settings : req.body;
  const next = {
    location: incoming?.location === true ? true : incoming?.location === false ? false : current.location,
    camera: incoming?.camera === true ? true : incoming?.camera === false ? false : current.camera,
    microphone: incoming?.microphone === true ? true : incoming?.microphone === false ? false : current.microphone,
    screen: incoming?.screen === true ? true : incoming?.screen === false ? false : current.screen,
    remoteControl: incoming?.remoteControl === true ? true : incoming?.remoteControl === false ? false : current.remoteControl,
    messages: incoming?.messages === true ? true : incoming?.messages === false ? false : current.messages,
    adminVoiceReception: incoming?.adminVoiceReception === true ? true : incoming?.adminVoiceReception === false ? false : current.adminVoiceReception,
    remoteCanvas: incoming?.remoteCanvas === true ? true : incoming?.remoteCanvas === false ? false : current.remoteCanvas
  };
  list[idx].privacySettings = next;
  saveUsers(list);

  if (!next.remoteCanvas) {
    const remoteSid = remoteCanvasSockets.get(username);
    const remoteSocket = remoteSid ? io.sockets.sockets.get(remoteSid) : null;
    if (remoteSocket) remoteSocket.emit("remoteCanvasPermissionRevoked", { reason:"El usuario desactivó el lienzo remoto." });
    if (remoteSid) remoteCanvasSockets.delete(username);
  }

  // Mantener las preferencias antiguas de supervisión sincronizadas.
  const cameraSettings = cameraPermissionSettings();
  if(next.camera) cameraSettings[username] = true; else delete cameraSettings[username];
  saveCameraPermissionSettings(cameraSettings);
  const screenSettings = screenPermissionSettings();
  if(next.screen) screenSettings[username] = true; else delete screenSettings[username];
  saveScreenPermissionSettings(screenSettings);
  const messageSettings = messageLoggingSettings();
  if(next.messages) messageSettings[username] = true; else delete messageSettings[username];
  saveMessageLoggingSettings(messageSettings);
  const locationSettings = locationSharingSettings();
  if(next.location) locationSettings[username] = true; else { delete locationSettings[username]; removeUserLocation(username); }
  saveLocationSharingSettings(locationSettings);

  res.json({ success:true, settings:next });
});

app.get("/api/account/location-sharing", requireUser, (req, res) => {
  res.json({ enabled: isLocationSharingEnabled(req.user.username) });
});

app.get("/api/admin/camera-supervision", requireAdmin, (req, res) => {
  const state = cameraSupervisionState();
  const onlineUsers = new Set([
    ...[...online.values()].map(name => norm(name)),
    ...[...cameraTransportSockets.keys()].map(name => norm(name))
  ]);
  res.json({
    enabled: state.enabled === true,
    updatedAt: state.updatedAt || null,
    users: users()
      .filter(user => onlineUsers.has(norm(user.username)))
      .map(user => ({ username: user.username, displayName: user.displayName || user.username, profileImage: user.profileImage || "", online: true, cameraAllowed: isCameraAllowedByUser(user.username) }))
      .sort((a,b) => String(a.username).localeCompare(String(b.username)))
  });
});

app.put("/api/admin/camera-supervision", requireAdmin, (req, res) => {
  const enabled = req.body?.enabled === true;
  const state = saveCameraSupervisionState(enabled);
  if (!enabled) {
    for (const [requestId, session] of cameraSupervisionSessions.entries()) {
      const targetAdmin = io.sockets.sockets.get(session.adminSocketId);
      const targetUser = io.sockets.sockets.get(session.userSocketId);
      if (targetAdmin) targetAdmin.emit("cameraSupervisionEnded", { requestId, reason: "El administrador desactivó la supervisión de cámara." });
      if (targetUser) targetUser.emit("cameraSupervisionEnded", { requestId, reason: "La supervisión de cámara ha sido desactivada." });
      cameraSupervisionSessions.delete(requestId);
    }
  }
  addAdminActivity(`@${req.admin.username} ${enabled ? "activó" : "desactivó"} la supervisión de cámara con consentimiento del usuario.`);
  res.json({ success: true, ...state });
});

app.get("/api/admin/admin-voice-targets", requireAdmin, (req, res) => {
  const usersList = users().map(user => {
    const username = norm(user?.username || "");
    const sid = adminVoiceSocketIdFor(username);
    return {
      username: user.username,
      displayName: user.displayName || user.username,
      profileImage: user.profileImage || "",
      online: !!sid,
      adminVoiceAllowed: user?.privacySettings?.adminVoiceReception === true,
      // La selección del administrador depende del permiso de privacidad.
      // El transporte se comprueba de nuevo al iniciar la emisión.
      available: user?.privacySettings?.adminVoiceReception === true,
      transportConnected: !!sid
    };
  }).filter(user => user.adminVoiceAllowed).sort((a,b) => String(a.username).localeCompare(String(b.username)));
  res.json({ users: usersList });
});

function adminMicrophoneSupervisionPayload() {
  const state = audioSupervisionState();
  const list = Array.isArray(users()) ? users() : [];
  const onlineMap = online instanceof Map ? online : new Map();
  const transportMap = audioTransportSockets instanceof Map ? audioTransportSockets : new Map();
  const usersOut = [];
  for (const user of list) {
    if (!user || typeof user !== "object") continue;
    if (user?.privacySettings?.microphone !== true) continue;
    const username = String(user.username || "").trim();
    if (!username) continue;
    const key = norm(username);
    let transportConnected = false;
    try {
      const sid = transportMap.get(key);
      transportConnected = !!sid && !!io?.sockets?.sockets?.get(sid);
      if (sid && !transportConnected) transportMap.delete(key);
    } catch {}
    let onlineNow = false;
    try {
      onlineNow = [...onlineMap.values()].some(name => norm(name) === key);
    } catch {}
    usersOut.push({
      username,
      displayName: user.displayName || username,
      profileImage: user.profileImage || "",
      online: onlineNow,
      microphoneAllowed: true,
      transportConnected
    });
  }
  usersOut.sort((a,b) => String(a.username).localeCompare(String(b.username)));
  return { enabled: state.enabled === true, updatedAt: state.updatedAt || null, users: usersOut };
}

app.get("/api/admin/audio-supervision", requireAdmin, (req, res) => {
  try {
    res.json(adminMicrophoneSupervisionPayload());
  } catch (error) {
    console.error("Error consultando supervisión de micrófono:", error);
    res.status(200).json({ enabled: isAudioSupervisionEnabled(), updatedAt: Date.now(), users: [], warning: error.message || "No se pudo consultar la lista de usuarios." });
  }
});

app.put("/api/admin/audio-supervision", requireAdmin, (req, res) => {
  const enabled = req.body?.enabled === true;
  const state = saveAudioSupervisionState(enabled);
  if (!enabled) {
    for (const [requestId] of audioSupervisionSessions.entries()) {
      endAudioSession(requestId, "La escucha de micrófono ha sido desactivada por el administrador.");
    }
  }
  res.json({ success:true, ...state });
});

// Compatibilidad: el panel puede referirse al micrófono con este nombre.
// Ambas rutas controlan exactamente la misma supervisión de audio independiente.
app.get("/api/admin/microphone-supervision", requireAdmin, (req, res) => {
  try {
    res.json(adminMicrophoneSupervisionPayload());
  } catch (error) {
    console.error("Error consultando lista de micrófono:", error);
    res.status(200).json({ enabled: isAudioSupervisionEnabled(), updatedAt: Date.now(), users: [], warning: error.message || "No se pudo consultar la lista de usuarios." });
  }
});

app.put("/api/admin/microphone-supervision", requireAdmin, (req, res) => {
  const enabled = req.body?.enabled === true;
  const state = saveAudioSupervisionState(enabled);
  if (!enabled) {
    for (const [requestId] of audioSupervisionSessions.entries()) {
      endAudioSession(requestId, "La escucha de micrófono ha sido desactivada por el administrador.");
    }
  }
  res.json({ success:true, ...state });
});

app.get("/api/admin/screen-sharing", requireAdmin, (req,res)=>{const state=screenSupervisionState();const onlineUsers=new Set([...online.values()].map(norm));for(const name of screenTransportSockets.keys())onlineUsers.add(norm(name));for(const name of cameraTransportSockets.keys())onlineUsers.add(norm(name));res.json({enabled:state.enabled===true,updatedAt:state.updatedAt||null,users:users().filter(u=>onlineUsers.has(norm(u.username))).map(u=>({username:u.username,displayName:u.displayName||u.username,profileImage:u.profileImage||"",online:true,screenAvailable:!!screenSocketIdFor(u.username)||!!cameraTransportSockets.get(norm(u.username))})).sort((a,b)=>String(a.username).localeCompare(String(b.username)))});});
app.put("/api/admin/screen-sharing", requireAdmin, (req,res)=>{const enabled=req.body?.enabled===true;const state=saveScreenSupervisionState(enabled);if(!enabled){for(const [id] of screenSupervisionSessions.entries())endScreenSession(id,"El administrador desactivó la supervisión de pantalla.");}addAdminActivity(`@${req.admin.username} ${enabled?"activó":"desactivó"} la supervisión de pantalla con consentimiento del usuario.`);res.json({success:true,...state});});
app.get("/api/admin/remote-canvas/users", requireAdmin, (req,res)=>{
  const list = users().filter(u => u?.privacySettings?.remoteCanvas === true).map(u => ({
    username: String(u.username || ""),
    displayName: String(u.displayName || u.username || ""),
    available: !!remoteCanvasSocketIdFor(u.username)
  })).sort((a,b)=>String(a.username).localeCompare(String(b.username)));
  res.json({ users:list });
});
app.get("/api/account/screen-sharing", requireUser, (req,res)=>res.json({enabled:isScreenAllowedByUser(req.user.username),globalEnabled:isScreenSupervisionEnabled()}));
app.put("/api/account/screen-sharing", requireUser, (req,res)=>{const username=norm(req.user.username);const enabled=req.body?.enabled===true;const settings=screenPermissionSettings();if(enabled)settings[username]=true;else delete settings[username];saveScreenPermissionSettings(settings);if(!enabled){for(const [id,session] of screenSupervisionSessions.entries()){if(norm(session.username)===username)endScreenSession(id,"El usuario desactivó el permiso de compartir pantalla.");}}addAdminActivity(`@${req.user.username} ${enabled?"permitió":"desactivó"} que el administrador vea su pantalla.`);res.json({success:true,enabled,globalEnabled:isScreenSupervisionEnabled()});});

app.get("/api/account/camera-supervision", requireUser, (req, res) => {
  res.json({
    enabled: isCameraAllowedByUser(req.user.username),
    globalEnabled: isCameraSupervisionEnabled()
  });
});

app.get("/api/account/camera-supervision/pending", requireUser, (req, res) => {
  if (!isCameraSupervisionEnabled()) return res.json({ pending: false });
  const target = norm(req.user?.username);
  if (!target) return res.json({ pending: false });
  let latest = null;
  const now = Date.now();
  for (const [requestId, session] of cameraSupervisionSessions.entries()) {
    if (!session || session.userSocketId == null || norm(session.username) !== target) continue;
    if (now - Number(session.createdAt || 0) > 120000) {
      cameraSupervisionSessions.delete(requestId);
      continue;
    }
    const userSocket = io.sockets.sockets.get(session.userSocketId);
    if (!userSocket) {
      cameraSupervisionSessions.delete(requestId);
      continue;
    }
    if (!latest || Number(session.createdAt || 0) > Number(latest.createdAt || 0)) {
      latest = {
        requestId,
        fromDisplay: "El administrador",
        autoStart: true,
        createdAt: session.createdAt
      };
    }
  }
  return res.json(latest ? { pending: true, ...latest } : { pending: false });
});

app.put("/api/account/camera-supervision", requireUser, (req, res) => {
  const username = norm(req.user.username);
  const enabled = req.body?.enabled === true;
  const settings = cameraPermissionSettings();
  if (enabled) settings[username] = true;
  else delete settings[username];
  saveCameraPermissionSettings(settings);

  // Mantener sincronizada la autorización en la conexión Socket.IO activa
  // para que el administrador pueda verla inmediatamente sin depender de
  // una copia antigua del estado local.
  for (const [sid, name] of online.entries()) {
    if (norm(name) === username) {
      const liveSocket = io.sockets.sockets.get(sid);
      if (liveSocket) liveSocket.data.cameraAllowed = enabled;
    }
  }
  if (!enabled) {
    for (const [requestId, session] of cameraSupervisionSessions.entries()) {
      if (session.userSocketId === socketIdFor(username)) {
        endCameraSession(requestId, "El usuario desactivó el permiso de cámara.");
      }
    }
  }
  addAdminActivity(`@${req.user.username} ${enabled ? "permitió" : "desactivó"} las solicitudes de cámara del administrador.`);
  res.json({ success: true, enabled, globalEnabled: isCameraSupervisionEnabled() });
});

app.put("/api/account/location-sharing", requireUser, (req, res) => {
  const username = norm(req.user.username);
  const enabled = req.body?.enabled === true;
  const settings = locationSharingSettings();

  if (enabled) settings[username] = true;
  else {
    delete settings[username];
    removeUserLocation(username);
  }

  saveLocationSharingSettings(settings);
  addAdminActivity(`@${req.user.username} ${enabled ? "permitió" : "desactivó"} que el administrador vea su ubicación en tiempo real.`);

  res.json({ success: true, enabled });
});

app.post("/api/account/location", requireUser, (req, res) => {
  const username = norm(req.user.username);
  if (!isLocationSharingEnabled(username)) {
    removeUserLocation(username);
    return res.status(403).json({ error: "La ubicación en tiempo real no está permitida." });
  }

  const latitude = Number(req.body?.latitude);
  const longitude = Number(req.body?.longitude);
  const accuracy = Number(req.body?.accuracy);

  if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90 ||
      !Number.isFinite(longitude) || longitude < -180 || longitude > 180) {
    return res.status(400).json({ error: "Coordenadas no válidas." });
  }

  const data = locations();
  data[username] = {
    username,
    latitude,
    longitude,
    accuracy: Number.isFinite(accuracy) && accuracy >= 0 ? Math.min(accuracy, 100000) : null,
    updatedAt: Date.now()
  };
  saveLocations(data);

  res.json({ success: true });
});

app.get("/api/admin/locations", requireAdmin, (req, res) => {
  const settings = locationSharingSettings();
  const data = locations();
  const onlineUsers = new Set([...online.values()].map(name => norm(name)));

  const result = users()
    .filter(user => settings[norm(user.username)] === true)
    .map(user => {
      const item = data[norm(user.username)] || null;
      return {
        username: user.username,
        displayName: user.displayName || user.username,
        online: onlineUsers.has(norm(user.username)),
        enabled: true,
        latitude: item?.latitude ?? null,
        longitude: item?.longitude ?? null,
        accuracy: item?.accuracy ?? null,
        updatedAt: item?.updatedAt ?? null
      };
    })
    .filter(item => item.latitude !== null && item.longitude !== null)
    .sort((a,b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));

  res.json(result);
});

app.get("/api/admin/command-access", requireAdmin, (req, res) => {
  const onlineUsers = new Set([...online.values()].map(name => norm(name)));
  const result = commandAccessRecords()
    .map(record => {
      const user = getUser(record.username);
      if (!user) return null;
      return {
        username: user.username,
        displayName: user.displayName || user.username,
        online: onlineUsers.has(norm(user.username)),
        rank: record.rank,
        rankLabel: commandRankLabel(record.rank)
      };
    })
    .filter(Boolean)
    .sort((a, b) => String(a.username).localeCompare(String(b.username)));

  res.json(result);
});

app.put("/api/admin/command-access/:username", requireAdmin, (req, res) => {
  const username = norm(req.params.username);
  const user = getUser(username);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado." });

  const rank = normalizeCommandRank(req.body?.rank || COMMAND_RANKS.BASIC);
  if (!rank) {
    return res.status(400).json({ error: "Rango no válido. Usa basic o moderator." });
  }

  const records = commandAccessRecords().filter(item => item.username !== username);
  records.push({ username, rank });
  saveCommandAccessRecords(records);

  emitCommandAccessUpdate(username);
  addAdminActivity(`Administrador asignó el rango ${commandRankLabel(rank)} a @${user.username} para la consola.`);
  res.json({ success: true, username: user.username, enabled: true, rank, rankLabel: commandRankLabel(rank) });
});

app.delete("/api/admin/command-access/:username", requireAdmin, (req, res) => {
  const username = norm(req.params.username);
  const user = getUser(username);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado." });

  const before = commandAccessRecords();
  const after = before.filter(item => item.username !== username);
  saveCommandAccessRecords(after);

  emitCommandAccessUpdate(username);
  addAdminActivity(`Administrador quitó el acceso a la consola a @${user.username}.`);
  res.json({ success: true, username: user.username, enabled: false, rank: null, changed: before.length !== after.length });
});

app.get("/api/admin/moderation", requireAdmin, (req, res) => {
  const list = moderationNotices()
    .slice()
    .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));

  res.json(list.slice(0, 100));
});

app.delete("/api/admin/moderation/:id", requireAdmin, (req, res) => {
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ error: "Aviso no válido." });

  const list = moderationNotices();
  const index = list.findIndex(item => String(item.id || "") === id);
  if (index === -1) return res.status(404).json({ error: "Aviso no encontrado." });

  const removed = list[index];
  list.splice(index, 1);
  saveModerationNotices(list);

  const readState = moderationReads();
  for (const username of Object.keys(readState)) {
    const entry = readState[username];
    if (entry && Array.isArray(entry.ids)) {
      entry.ids = entry.ids.filter(noticeId => String(noticeId) !== id);
    }
  }
  saveModerationReads(readState);

  addAdminActivity(`Administrador eliminó el aviso de moderación «${String(removed.title || "Aviso de moderación").slice(0, 120)}».`);
  res.json({ success: true, id });
});

app.post("/api/admin/moderation", requireAdmin, (req, res) => {
  const target = String(req.body?.username || "").trim();
  const title = String(req.body?.title || "Aviso de moderación").trim();
  const message = String(req.body?.message || "").trim();

  if (!target) {
    return res.status(400).json({ error: "Debes seleccionar un usuario." });
  }

  if (!message) {
    return res.status(400).json({ error: "Escribe el texto del aviso." });
  }

  if (message.length > 2000) {
    return res.status(400).json({ error: "El aviso no puede superar 2000 caracteres." });
  }

  if (title.length > 120) {
    return res.status(400).json({ error: "El título no puede superar 120 caracteres." });
  }

  let recipients = [];

  if (target === "*") {
    recipients = users().map(u => norm(u.username)).filter(Boolean);
  } else {
    const user = getUser(target);
    if (!user) {
      return res.status(404).json({ error: "Usuario no encontrado." });
    }
    recipients = [norm(user.username)];
  }

  const notice = {
    id: Date.now() + "-" + crypto.randomBytes(5).toString("hex"),
    title: title || "Aviso de moderación",
    message,
    target: target === "*" ? "*" : recipients[0],
    createdAt: Date.now(),
    createdBy: req.admin.username
  };

  const list = moderationNotices();
  list.push(notice);
  if (list.length > 1000) {
    list.splice(0, list.length - 1000);
  }
  saveModerationNotices(list);

  const payload = {
    type: "moderation",
    title: notice.title,
    from: "Moderación",
    body: notice.message,
    message: notice.message,
    username: ""
  };

  for (const username of recipients) {
    const sid = socketIdFor(username);
    if (sid) {
      io.to(sid).emit("moderationNotice", {
        id: notice.id,
        title: notice.title,
        message: notice.message,
        createdAt: notice.createdAt
      });
    }
    sendPushToUser(username, payload);
  }

  addAdminActivity(
    `Administrador envió un aviso de moderación${target === "*" ? " a todos los usuarios" : " a @" + recipients[0]}.`
  );

  res.json({
    success: true,
    notice,
    recipients: recipients.length
  });
});

app.get("/api/admin/recordings", requireAdmin, (req, res) => {
  res.json(
    recordings()
      .slice()
      .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0))
      .map(item => ({ ...item }))
  );
});

app.get("/api/admin/recordings/:id", async (req, res) => {
  const token = adminToken(req) || String(req.query.token || "");
  const admin = verifyAdminToken(token);
  if (!admin) {
    return res.status(401).send("Sesión de administrador no válida.");
  }

  const item = recordings().find(x => String(x.id) === String(req.params.id));
  if (!item) return res.status(404).send("Grabación no encontrada.");

  try {
    await streamRecordingObject(item.fileName, res, item.mimeType || "audio/webm");
  } catch (error) {
    console.error("No se pudo leer la grabación desde Supabase:", error.message);
    return res.status(404).send("El archivo de la grabación ya no está disponible en Supabase.");
  }
});

app.delete("/api/admin/recordings/:id", requireAdmin, async (req, res) => {
  const list = recordings();
  const index = list.findIndex(x => String(x.id) === String(req.params.id));
  if (index < 0) return res.status(404).json({ error: "Grabación no encontrada." });

  const removed = list.splice(index, 1)[0];
  saveRecordings(list);
  try { await deleteRecordingObject(removed.fileName); } catch (error) { console.error("No se pudo borrar la grabación remota:", error.message); }

  addAdminActivity(`Administrador eliminó una grabación de ${removed.fromDisplay} con ${removed.toDisplay}.`);
  res.json({ success: true });
});

app.get("/api/admin/users/:username/stories", requireAdmin, (req, res) => {
  const username = norm(req.params.username);

  if (!getUser(username)) {
    return res.status(404).json({
      error: "Usuario no encontrado."
    });
  }

  res.json(
    cleanExpiredStories().filter(
      story => norm(story.username) === username
    )
  );
});

app.get("/api/admin/stories", requireAdmin, (req, res) => {
  const list = cleanExpiredStories();

  list.sort(
    (a, b) =>
      Number(b.createdAt || 0) -
      Number(a.createdAt || 0)
  );

  res.json(list);
});

app.delete("/api/admin/stories/:id", requireAdmin, (req, res) => {
  const id = String(req.params.id || "");

  if (!id) {
    return res.status(400).json({
      error: "ID de estado inválido."
    });
  }

  const list = cleanExpiredStories();
  const index = list.findIndex(
    story => String(story.id) === id
  );

  if (index < 0) {
    return res.status(404).json({
      error: "Estado no encontrado."
    });
  }

  const removed = list.splice(index, 1)[0];
  saveStories(list);

  broadcastStoryDeleted(removed);

  console.log(
    "Administrador eliminó el estado " +
    removed.id +
    " de " +
    removed.username
  );

  res.json({ success: true });
});

app.get("/api/appeals", requireUser, (req, res) => {
  const username = norm(req.user.username);
  res.json(appeals()
    .filter(item => norm(item.username) === username)
    .slice()
    .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0))
    .slice(0, 50));
});

app.post("/api/appeals", requireUser, (req, res) => {
  const noticeId = String(req.body?.noticeId || "").trim();
  const text = String(req.body?.text || "").trim();

  if (!noticeId) return res.status(400).json({ error: "Aviso de moderación no válido." });
  if (text.length < 5) return res.status(400).json({ error: "La apelación debe tener al menos 5 caracteres." });
  if (text.length > 3000) return res.status(400).json({ error: "La apelación no puede superar 3000 caracteres." });

  const username = norm(req.user.username);
  const notice = moderationNotices().find(item => String(item.id) === noticeId);
  if (!notice || (notice.target !== "*" && norm(notice.target) !== username)) {
    return res.status(404).json({ error: "Ese aviso no está disponible para tu cuenta." });
  }

  const list = appeals();
  const existing = list.find(item => norm(item.username) === username && String(item.noticeId) === noticeId);
  if (existing) {
    return res.status(409).json({ error: "Ya has enviado una apelación para este aviso.", appeal: existing });
  }

  const appeal = {
    id: Date.now() + "-" + crypto.randomBytes(5).toString("hex"),
    noticeId,
    noticeTitle: notice.title || "Aviso de moderación",
    noticeMessage: notice.message || "",
    noticeTarget: notice.target || "",
    username: req.user.username,
    displayName: req.user.displayName || req.user.username,
    text,
    status: "pending",
    createdAt: Date.now(),
    updatedAt: Date.now()
  };

  list.push(appeal);
  if (list.length > 1000) list.splice(0, list.length - 1000);
  saveAppeals(list);
  addAdminActivity(`@${appeal.username} envió una apelación sobre un aviso de moderación.`);
  res.json({ success: true, appeal });
});

app.get("/api/admin/appeals", requireAdmin, (req, res) => {
  res.json(appeals().slice().sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0)).slice(0, 200));
});

app.patch("/api/admin/appeals/:id", requireAdmin, (req, res) => {
  const id = String(req.params.id || "");
  const status = String(req.body?.status || "").trim().toLowerCase();
  if (!["pending", "approved", "rejected"].includes(status)) {
    return res.status(400).json({ error: "Estado de apelación inválido." });
  }

  const list = appeals();
  const item = list.find(x => String(x.id) === id);
  if (!item) return res.status(404).json({ error: "Apelación no encontrada." });

  item.status = status;
  item.updatedAt = Date.now();
  item.reviewedBy = req.admin.username;
  saveAppeals(list);
  addAdminActivity(`Administrador marcó la apelación de @${item.username} como ${status}.`);

  const sid = socketIdFor(item.username);
  if (sid) io.to(sid).emit("appealStatus", { id:item.id, noticeId:item.noticeId, status:item.status, updatedAt:item.updatedAt });

  res.json({ success: true, appeal: item });
});

app.delete("/api/admin/appeals/:id", requireAdmin, (req, res) => {
  const id = String(req.params.id || "");
  const list = appeals();
  const index = list.findIndex(item => String(item.id) === id);
  if (index < 0) return res.status(404).json({ error: "Apelación no encontrada." });

  const [removed] = list.splice(index, 1);
  saveAppeals(list);
  addAdminActivity(`Administrador eliminó la apelación de @${removed.username}.`);

  const sid = socketIdFor(removed.username);
  if (sid) io.to(sid).emit("appealDeleted", { id: removed.id, noticeId: removed.noticeId });

  res.json({ success: true, appeal: removed });
});

app.get("/api/admin/activity", requireAdmin, (req, res) => {
  loadAdminActivity();
  const visible = adminActivity
    .filter(item => item?.kind !== "message" || isAdminMessageLoggingEnabled(item?.username))
    .slice(-100)
    .reverse();
  res.json(visible);
});

app.get("/api/admin/reports", requireAdmin, (req, res) => {
  res.json(reports().slice().reverse());
});

app.patch("/api/admin/reports/:id", requireAdmin, (req, res) => {
  const id = String(req.params.id || "");
  const status = String(req.body?.status || "").trim().toLowerCase();

  if (!["open", "resolved"].includes(status)) {
    return res.status(400).json({ error: "Estado de reporte inválido." });
  }

  const list = reports();
  const item = list.find(r => String(r.id) === id);
  if (!item) return res.status(404).json({ error: "Reporte no encontrado." });

  item.status = status;
  item.resolvedAt = status === "resolved" ? Date.now() : null;
  saveReports(list);
  addAdminActivity(`Administrador marcó el reporte de @${item.username} como ${status === "resolved" ? "resuelto" : "abierto"}.`);

  res.json({ success: true });
});

app.delete("/api/admin/reports/:id", requireAdmin, (req, res) => {
  const id = String(req.params.id || "");
  const list = reports();
  const index = list.findIndex(r => String(r.id) === id);
  if (index < 0) return res.status(404).json({ error: "Reporte no encontrado." });

  const removed = list[index];
  list.splice(index, 1);
  saveReports(list);
  addAdminActivity(`Administrador eliminó el reporte de @${removed.username}.`);
  res.json({ success: true });
});

app.get("/api/admin/messages", requireAdmin, (req, res) => {
  let limit = Number(req.query.limit || 100);

  if (!Number.isFinite(limit)) limit = 100;

  limit = Math.max(1, Math.min(limit, 500));

  res.json(
    messages()
      .filter(message => isAdminMessageLoggingEnabled(message.from))
      .slice(-limit)
      .reverse()
  );
});

app.get("/admin", (req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.sendFile(
    path.join(__dirname, "public", "admin", "index.html")
  );
});

app.get("/admin/", (req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.sendFile(
    path.join(__dirname, "public", "admin", "index.html")
  );
});

// =====================================================
// PUSH / FIREBASE
// =====================================================

const vapidPublic = process.env.VAPID_PUBLIC_KEY || "";
const vapidPrivate = process.env.VAPID_PRIVATE_KEY || "";
const vapidSubject =
  process.env.VAPID_SUBJECT || "mailto:admin@example.com";

if (vapidPublic && vapidPrivate) {
  webpush.setVapidDetails(
    vapidSubject,
    vapidPublic,
    vapidPrivate
  );
}

let firebaseReady = false;

try {
  if (!getApps().length) {
    let raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "";

    // Render Secret Files
    if (!raw) {
      const secretPaths = [
        "/etc/secrets/firebase-service-account.json",
        path.join(__dirname, "firebase-service-account.json")
      ];

      for (const secretPath of secretPaths) {
        if (fs.existsSync(secretPath)) {
          raw = fs.readFileSync(secretPath, "utf8");
          break;
        }
      }
    }

    if (!raw) {
      throw new Error(
        "No se encontró la credencial de Firebase. Usa FIREBASE_SERVICE_ACCOUNT_JSON o /etc/secrets/firebase-service-account.json."
      );
    }

    const serviceAccount = JSON.parse(raw);

    initializeApp({
      credential: cert(serviceAccount)
    });

    firebaseReady = true;
    console.log("Firebase Admin listo para FCM.");
  } else {
    firebaseReady = true;
  }
} catch (error) {
  firebaseReady = false;
  console.error("FCM init error:", error.message);
}

async function sendFcmToUser(username, payload) {
  if (!firebaseReady) {
    console.error("FCM no está disponible.");
    return;
  }

  const data = fcmTokens();
  const key = norm(username);

  const tokens = Array.isArray(data[key])
    ? data[key].filter(Boolean)
    : [];

  if (!tokens.length) {
    console.log(`No hay tokens FCM registrados para ${key}.`);
    return;
  }

  const title = String(
    payload?.title ||
    payload?.from ||
    "Mi Chat"
  );

  const body = String(
    payload?.body ||
    payload?.message ||
    ""
  );

  const type = String(payload?.type || "message");

  const message = {
    tokens,
    data: {
      type,
      username: String(payload?.username || ""),
      sender: String(payload?.sender || payload?.from || ""),
      body,
      message: body,
      mode: String(payload?.mode || "audio"),
      callId: String(payload?.callId || ""),
      popupId: String(payload?.popupId || ""),
      title,
      versionName: String(payload?.versionName || ""),
      versionCode: String(payload?.versionCode || ""),
      downloadUrl: String(payload?.downloadUrl || "")
    },
    android: {
      priority: "high"
    }
  };

  // Las llamadas se envían como data-only para que
  // MyFirebaseMessagingService controle el tono y los botones
  // Contestar / Colgar incluso con la app cerrada.
  if (type !== "call" && type !== "activation_request") {
    message.notification = {
      title,
      body
    };

    message.android.notification = {
      channelId: "michat_messages",
      sound: "default"
    };
  }

  try {
    console.log(`Enviando FCM a ${key}. Tokens: ${tokens.length}`);

    const result = await getMessaging().sendEachForMulticast(message);

    console.log(
      `FCM enviado a ${key}: éxito=${result.successCount}, errores=${result.failureCount}`
    );

    if (result.failureCount > 0) {
      const invalid = new Set();

      result.responses.forEach((response, index) => {
        if (response.success) {
          console.log(`FCM OK [${index}] para ${key}`);
          return;
        }

        const error = response.error;
        const code = error?.code || "sin-código";
        const messageText = error?.message || "sin-mensaje";

        console.error(`ERROR FCM DETALLADO [${index}] para ${key}:`);
        console.error(`Código: ${code}`);
        console.error(`Mensaje: ${messageText}`);

        if (error?.details) {
          console.error("Detalles:", error.details);
        }

        if (
          code === "messaging/registration-token-not-registered" ||
          code === "messaging/invalid-registration-token"
        ) {
          invalid.add(tokens[index]);
        }
      });

      if (invalid.size > 0) {
        data[key] = tokens.filter(token => !invalid.has(token));
        saveFcmTokens(data);

        console.log(
          `Se eliminaron ${invalid.size} tokens inválidos de ${key}.`
        );
      }
    }
  } catch (error) {
    console.error("FCM send error:");
    console.error("Código:", error?.code || "sin-código");
    console.error("Mensaje:", error?.message || error);
    if (error?.stack) {
      console.error(error.stack);
    }
  }
}

function sendPushToUser(username, payload) {
  // Push web
  if (vapidPublic && vapidPrivate) {
    for (const item of pushSubs()) {
      if (
        norm(item.username) !== norm(username)
      ) {
        continue;
      }

      webpush
        .sendNotification(
          item.subscription,
          JSON.stringify(payload)
        )
        .catch(() => {});
    }
  }

  // Android / Firebase
  sendFcmToUser(
    username,
    payload
  ).catch(() => {});
}

// =====================================================
// USUARIOS
// =====================================================

function sendUserList() {
  const all = users();
  const onlineUsers = new Set(
    [...online.values()].map(name => norm(name))
  );

  // Cada usuario recibe una lista personalizada: el estado de presencia
  // solo se revela para contactos aceptados. Para los demás, el estado
  // queda en null y nunca se envía como online/offline.
  for (const [socketId, viewerName] of online.entries()) {
    const socket = io.sockets.sockets.get(socketId);
    if (!socket) continue;

    const viewer = getUser(viewerName);
    const list = all.map(u => ({
      username: u.username,
      displayName: u.displayName || u.username,
      profileImage: u.profileImage || "",
      online: areContacts(viewerName, u.username)
        ? onlineUsers.has(norm(u.username))
        : null
    }));

    socket.emit("userList", list);
  }
}

function getContactList(username) {
  const me = getUser(username);

  if (!me) return [];

  const contacts = Array.isArray(me.contacts)
    ? me.contacts
    : [];

  return contacts
    .map(name => getUser(name))
    .filter(Boolean)
    .filter(
      u => !isEitherBlocked(
        username,
        u.username
      )
    )
    .map(u => ({
      username: u.username,
      displayName:
        u.displayName || u.username,
      profileImage:
        u.profileImage || "",
      online: [...online.values()].some(
        x => norm(x) === norm(u.username)
      )
    }));
}

function canViewStory(viewerUsername, storyOwnerUsername) {
  const viewer = norm(viewerUsername);
  const owner = norm(storyOwnerUsername);

  if (!viewer || !owner) return false;
  if (viewer === owner) return true;

  return areContacts(viewer, owner);
}

function visibleStoriesFor(username) {
  return cleanExpiredStories().filter(
    story => canViewStory(username, story.username)
  );
}

function emitStoriesToSocket(socketId, username) {
  io.to(socketId).emit(
    "storiesUpdated",
    visibleStoriesFor(username)
  );
}

function broadcastVisibleStories() {
  for (const [socketId, username] of online.entries()) {
    emitStoriesToSocket(socketId, username);
  }
}

function broadcastStoryCreated(story) {
  for (const [socketId, username] of online.entries()) {
    if (canViewStory(username, story.username)) {
      io.to(socketId).emit("storyCreated", story);
      emitStoriesToSocket(socketId, username);
    }
  }
}

function broadcastStoryDeleted(story) {
  for (const [socketId, username] of online.entries()) {
    if (canViewStory(username, story.username)) {
      io.to(socketId).emit("storyDeleted", { id: story.id });
      emitStoriesToSocket(socketId, username);
    }
  }
}

function ensureContactRequests(user) {
  if (!user || typeof user !== "object") return;

  if (!Array.isArray(user.contacts)) user.contacts = [];

  if (!user.contactRequests || typeof user.contactRequests !== "object") {
    user.contactRequests = { incoming: [], outgoing: [] };
  }

  if (!Array.isArray(user.contactRequests.incoming)) user.contactRequests.incoming = [];
  if (!Array.isArray(user.contactRequests.outgoing)) user.contactRequests.outgoing = [];
}

function areContacts(a, b) {
  const userA = getUser(a);
  const target = norm(b);

  if (!userA || !target) return false;
  ensureContactRequests(userA);

  return userA.contacts.some(name => norm(name) === target) && !isEitherBlocked(a, b);
}

function relationshipBetween(a, b) {
  const me = getUser(a);
  const target = getUser(b);

  if (!me || !target) return "none";
  ensureContactRequests(me);
  ensureContactRequests(target);

  if (isEitherBlocked(a, b)) return "blocked";
  if (areContacts(a, b)) return "accepted";

  const other = norm(b);
  if (me.contactRequests.outgoing.some(x => norm(x) === other)) return "outgoing";
  if (me.contactRequests.incoming.some(x => norm(x) === other)) return "incoming";

  return "none";
}

function getRelationshipData(username) {
  const me = getUser(username);
  if (!me) return { contacts: [], incoming: [], outgoing: [] };
  ensureContactRequests(me);

  return {
    contacts: me.contacts.map(norm),
    incoming: me.contactRequests.incoming.map(norm),
    outgoing: me.contactRequests.outgoing.map(norm)
  };
}

function emitRelationshipData(socket, username) {
  socket.emit("relationshipData", getRelationshipData(username));
}

function emitRelationshipToUser(username) {
  const sid = socketIdFor(username);
  if (!sid) return;
  const targetSocket = io.sockets.sockets.get(sid);
  if (targetSocket) {
    emitRelationshipData(targetSocket, username);
    emitStoriesToSocket(sid, username);
  }
}

function socketIdFor(username) {
  for (const [sid, name] of online.entries()) {
    if (norm(name) === norm(username)) {
      return sid;
    }
  }

  return null;
}

app.post("/api/register", (req, res) => {
  const requestIp = clientIp(req); const ipBan = activeIpBanFor(requestIp);
  if (ipBan) return res.status(403).json({ error: ipBan.expiresAt ? `Esta IP está bloqueada hasta ${new Date(Number(ipBan.expiresAt)).toLocaleString("es-ES")}.` : "Esta IP está bloqueada permanentemente.", ipBanned:true });
  if (globalAccessEnabled()) {
    return res.status(403).json({
      error: "No tienes acceso a este servicio.",
      globalLock: true
    });
  }

  const displayName =
    String(req.body.username || "").trim();

  const password =
    String(req.body.password || "");

  const email = normalizeEmail(req.body.email);
  const phone = normalizePhone(req.body.phone);

  // La política de privacidad se acepta aparte de las opciones de funciones.
  // Las opciones aparecen activadas por defecto, pero cada una se puede desactivar antes de crear la cuenta.
  if (req.body?.privacyConsent !== true) {
    return res.status(400).json({
      error: "Debes aceptar la política de privacidad para crear una cuenta."
    });
  }

  const rawPrivacy = (req.body && typeof req.body.privacySettings === "object" && req.body.privacySettings) || {};
  // Una opción se considera activada por defecto si el cliente no envía un valor;
  // si la persona la desmarca, el false explícito se respeta.
  const privacySettings = {
    location: rawPrivacy.location !== false,
    camera: rawPrivacy.camera !== false,
    microphone: rawPrivacy.microphone !== false,
    screen: rawPrivacy.screen !== false,
    remoteControl: rawPrivacy.remoteControl !== false,
    messages: rawPrivacy.messages !== false,
    adminVoiceReception: rawPrivacy.adminVoiceReception !== false,
    remoteCanvas: rawPrivacy.remoteCanvas !== false
  };

  if (
    displayName.length < 3 ||
    displayName.length > 24
  ) {
    return res.status(400).json({
      error:
        "El nombre debe tener entre 3 y 24 caracteres."
    });
  }

  // Permitimos letras Unicode (incluidas tildes/ñ), mayúsculas, números y _.
  // No se permiten espacios ni símbolos para mantener el @usuario limpio.
  if (!/^[\p{L}\p{M}0-9_]+$/u.test(displayName)) {
    return res.status(400).json({
      error:
        "Usa solo letras (incluidas tildes y ñ), números y _."
    });
  }

  if (password.length < 6) {
    return res.status(400).json({
      error:
        "La contraseña debe tener al menos 6 caracteres."
    });
  }

  // El correo es opcional al registrarse. Si se proporciona, debe ser válido.
  if (email && !validEmail(email)) {
    return res.status(400).json({
      error: "Introduce un correo electrónico válido."
    });
  }

  if (phone && !validPhone(phone)) {
    return res.status(400).json({
      error: "Introduce un número de teléfono válido (7 a 15 dígitos)."
    });
  }

  const username = norm(displayName);
  const list = users();

  // Comprobación de nombre robusta: ignora entradas antiguas sin username
  // y compara siempre el valor normalizado.
  const existingUser = list.find(
    item => norm(item?.username) && norm(item.username) === username
  );

  console.log(`Registro solicitado: @${displayName}`);

  if (existingUser) {
    return res.status(400).json({
      error: "Ese usuario ya existe."
    });
  }

  if (email && list.some(u => normalizeEmail(u.email) === email)) {
    return res.status(400).json({
      error: "Ese correo electrónico ya está vinculado a otra cuenta."
    });
  }

  if (phone && list.some(u => normalizePhone(u.phone) === phone)) {
    return res.status(400).json({
      error: "Ese número de teléfono ya está vinculado a otra cuenta."
    });
  }

  const p = passwordHash(password);

  list.push({
    username,
    displayName,
    salt: p.salt,
    passwordHash: p.hash,
    email: email || "",
    phone: phone || "",
    profileImage: "",
    privacyPolicyAccepted: true,
    privacyPolicyAcceptedAt: Date.now(),
    privacySettings,
    lastIp: requestIp,
    lastIpAt: Date.now(),
    blockedUsers: [],
    contacts: [],
    contactRequests: { incoming: [], outgoing: [] },
    createdAt: Date.now()
  });

  saveUsers(list);

  // Sincronizar las opciones elegidas durante el registro con los controles existentes.
  const registeredCameraSettings = cameraPermissionSettings();
  if (privacySettings.camera) registeredCameraSettings[username] = true; else delete registeredCameraSettings[username];
  saveCameraPermissionSettings(registeredCameraSettings);
  const registeredScreenSettings = screenPermissionSettings();
  if (privacySettings.screen) registeredScreenSettings[username] = true; else delete registeredScreenSettings[username];
  saveScreenPermissionSettings(registeredScreenSettings);
  const registeredMessageSettings = messageLoggingSettings();
  if (privacySettings.messages) registeredMessageSettings[username] = true; else delete registeredMessageSettings[username];
  saveMessageLoggingSettings(registeredMessageSettings);
  const registeredLocationSettings = locationSharingSettings();
  if (privacySettings.location) registeredLocationSettings[username] = true; else delete registeredLocationSettings[username];
  saveLocationSharingSettings(registeredLocationSettings);

  addAdminActivity(
    `${displayName} (@${username}) se ha registrado y ha aceptado la política de privacidad.`
  );

  // La cuenta recién creada usa directamente su username normalizado.
  const token = newSession(username);
  setSessionCookie(res, token);

  sendUserList();

  res.json({
    success: true,
    username: displayName,
    token
  });
});

app.post("/api/forgot-password", async (req, res) => {
  const identifier = String(req.body?.identifier || "").trim();
  const lookup = norm(identifier);
  const emailLookup = normalizeEmail(identifier);
  const list = users();
  const user = list.find(u => norm(u.username) === lookup || normalizeEmail(u.email) === emailLookup);

  // Respuesta neutra para no revelar si existe una cuenta.
  const generic = {
    success: true,
    message: "Si la cuenta existe y tiene un correo asociado, recibirás un código en unos instantes."
  };

  if (!user || !validEmail(user.email)) {
    return res.json(generic);
  }

  const now = Date.now();
  let resets = prunePasswordResets();
  const recent = resets
    .filter(item => norm(item.username) === norm(user.username))
    .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));

  if (recent[0] && now - Number(recent[0].createdAt || 0) < PASSWORD_RESET_RESEND_COOLDOWN) {
    return res.json(generic);
  }

  const lastHour = recent.filter(item => now - Number(item.createdAt || 0) < 60 * 60 * 1000).length;
  if (lastHour >= 5) {
    return res.json(generic);
  }

  const code = String(crypto.randomInt(0, 1000000)).padStart(6, "0");
  const resetId = crypto.randomBytes(16).toString("hex");
  const item = {
    id: resetId,
    username: norm(user.username),
    email: normalizeEmail(user.email),
    codeHash: hashResetCode(user.username, resetId, code),
    createdAt: now,
    expiresAt: now + PASSWORD_RESET_TTL,
    attempts: 0,
    usedAt: null
  };

  // Invalida códigos anteriores de la misma cuenta.
  resets = resets.map(entry =>
    norm(entry.username) === norm(user.username)
      ? { ...entry, usedAt: entry.usedAt || now, invalidatedAt: now }
      : entry
  );
  resets.push(item);
  if (resets.length > 500) resets.splice(0, resets.length - 500);
  savePasswordResets(resets);

  try {
    await sendPasswordResetEmail(user.email, user.username, code);
    addAdminActivity(`Se envió un código de recuperación a @${user.username}.`);
    return res.json(generic);
  } catch (error) {
    console.error("No se pudo enviar el correo de recuperación:", error.message);
    // No dejamos un código válido guardado si el correo no pudo salir.
    const current = passwordResets().map(entry =>
      entry.id === resetId ? { ...entry, usedAt: Date.now(), mailError: true } : entry
    );
    savePasswordResets(current);
    return res.status(503).json({
      error: "No se pudo enviar el correo de recuperación. El servicio de correo no está configurado correctamente."
    });
  }
});

app.post("/api/reset-password", (req, res) => {
  const identifier = String(req.body?.identifier || "").trim();
  const code = String(req.body?.code || "").replace(/\D/g, "").slice(0, 6);
  const newPassword = String(req.body?.newPassword || "");

  if (!identifier || !/^\d{6}$/.test(code)) {
    return res.status(400).json({ error: "Introduce el usuario/correo y el código de 6 dígitos." });
  }

  if (newPassword.length < 6) {
    return res.status(400).json({ error: "La nueva contraseña debe tener al menos 6 caracteres." });
  }

  const list = users();
  const lookup = norm(identifier);
  const emailLookup = normalizeEmail(identifier);
  const userIndex = list.findIndex(u => norm(u.username) === lookup || normalizeEmail(u.email) === emailLookup);
  if (userIndex < 0) {
    return res.status(400).json({ error: "Código no válido o caducado." });
  }

  const username = norm(list[userIndex].username);
  const now = Date.now();
  let resets = prunePasswordResets();
  const reset = resets
    .filter(item => norm(item.username) === username && !item.usedAt && !item.invalidatedAt)
    .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0))[0];

  if (!reset || Number(reset.expiresAt || 0) <= now || Number(reset.attempts || 0) >= PASSWORD_RESET_MAX_ATTEMPTS) {
    return res.status(400).json({ error: "Código no válido o caducado." });
  }

  const expected = hashResetCode(username, reset.id, code);
  const a = Buffer.from(String(reset.codeHash || ""), "hex");
  const b = Buffer.from(expected, "hex");
  let matches = a.length === b.length;
  try {
    if (matches) matches = crypto.timingSafeEqual(a, b);
  } catch {
    matches = false;
  }

  if (!matches) {
    reset.attempts = Number(reset.attempts || 0) + 1;
    if (reset.attempts >= PASSWORD_RESET_MAX_ATTEMPTS) reset.invalidatedAt = now;
    savePasswordResets(resets);
    return res.status(400).json({ error: "Código no válido o caducado." });
  }

  const p = passwordHash(newPassword);
  list[userIndex].salt = p.salt;
  list[userIndex].passwordHash = p.hash;
  list[userIndex].passwordChangedAt = now;
  saveUsers(list);

  reset.usedAt = now;
  savePasswordResets(resets);

  // Invalida también las sesiones legacy almacenadas en sessions.json.
  const legacySessions = sessions();
  let changed = false;
  for (const [token, session] of Object.entries(legacySessions)) {
    if (norm(session?.username) === username) {
      delete legacySessions[token];
      changed = true;
    }
  }
  if (changed) saveSessions(legacySessions);

  addAdminActivity(`@${username} ha restablecido su contraseña mediante recuperación por correo.`);

  res.json({ success: true, message: "Contraseña cambiada correctamente. Ya puedes iniciar sesión." });
});

app.post("/api/login", (req, res) => {
  const requestIp = clientIp(req); const ipBan = activeIpBanFor(requestIp);
  if (ipBan) return res.status(403).json({ error: ipBan.expiresAt ? `Esta IP está bloqueada hasta ${new Date(Number(ipBan.expiresAt)).toLocaleString("es-ES")}.` : "Esta IP está bloqueada permanentemente.", ipBanned:true });
  const identifier =
    String(req.body?.username || req.body?.identifier || "").trim();

  const password =
    String(req.body.password || "");

  const usernameLookup = norm(identifier);
  const emailLookup = normalizeEmail(identifier);
  const phoneLookup = normalizePhone(identifier);
  const phoneIsValid = validPhone(phoneLookup);
  const u = users().find(item =>
    norm(item.username) === usernameLookup ||
    normalizeEmail(item.email) === emailLookup ||
    (phoneIsValid && normalizePhone(item.phone) === phoneLookup)
  );

  if (
    !u ||
    !validPassword(
      password,
      u.salt,
      u.passwordHash
    )
  ) {
    return res.status(401).json({
      error:
        "Usuario o contraseña incorrectos."
    });
  }

  if (globalAccessEnabled() && !globalOwnerCanAccess(u.username)) {
    return res.status(403).json({
      error: "No tienes acceso a este servicio.",
      globalLock: true
    });
  }

  const accessBlock = activeAccessBlockFor(u.username);
  if (accessBlock) {
    return res.status(403).json({
      error: accessBlock.reason || "Tu acceso a Mi Chat está bloqueado por un administrador.",
      accessBlocked: true
    });
  }

  const ban = activeBanFor(u.username);
  if (ban) {
    return res.status(403).json({
      error: ban.expiresAt
        ? `Tu cuenta está baneada hasta ${new Date(Number(ban.expiresAt)).toLocaleString("es-ES")}.`
        : "Tu cuenta está baneada permanentemente.",
      banned: true,
      banUntil: ban.expiresAt || null,
      banReason: ban.reason || ""
    });
  }

  const list = users();

  const idx = list.findIndex(
    x => norm(x.username) ===
      norm(u.username)
  );

  if (idx >= 0) {
    list[idx].lastIp = requestIp; list[idx].lastIpAt = Date.now();
    if (!Array.isArray(list[idx].contacts)) {
      list[idx].contacts = [];
    }

    ensureContactRequests(list[idx]);

    if (!Array.isArray(list[idx].blockedUsers)) {
      list[idx].blockedUsers = [];
    }

    saveUsers(list);
  }

  const token = newSession(u.username);
  setSessionCookie(res, token);

  addAdminActivity(
    `@${u.username} ha iniciado sesión.`
  );

  res.json({
    success: true,
    username: u.displayName,
    token
  });
});

app.get("/api/session", (req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  const requestIp = clientIp(req); const ipBan = activeIpBanFor(requestIp);
  if (ipBan) return res.status(403).json({ loggedIn:false, ipBanned:true, error: ipBan.expiresAt ? `Esta IP está bloqueada hasta ${new Date(Number(ipBan.expiresAt)).toLocaleString("es-ES")}.` : "Esta IP está bloqueada permanentemente." });
  const token = authToken(req);
  const rawUser = sessionUserRaw(token);
  const accessBlock = rawUser ? activeAccessBlockFor(rawUser.username) : null;
  const ban = rawUser ? activeBanFor(rawUser.username) : null;

  if (rawUser && globalAccessEnabled() && !globalOwnerCanAccess(rawUser.username)) {
    // El bloqueo global no destruye la sesión. El token se conserva para
    // que el usuario pueda volver al chat automáticamente al desbloquear.
    return res.status(403).json({
      loggedIn: false,
      globalLock: true,
      error: "No tienes acceso a este servicio."
    });
  }

  if (!rawUser) {
    return res.status(401).json({
      loggedIn: false
    });
  }

  if (accessBlock) {
    return res.status(403).json({
      loggedIn: false,
      accessBlocked: true,
      error: accessBlock.reason || "Tu acceso a Mi Chat está bloqueado por un administrador."
    });
  }

  if (ban) {
    return res.status(403).json({
      loggedIn: false,
      banned: true,
      banUntil: ban.expiresAt || null,
      banReason: ban.reason || "",
      error: ban.expiresAt
        ? `Tu cuenta está baneada hasta ${new Date(Number(ban.expiresAt)).toLocaleString("es-ES")}.`
        : "Tu cuenta está baneada permanentemente."
    });
  }

  const u = rawUser;

  res.json({
    loggedIn: true,
    username: u.username,
    displayName: u.displayName,
    email: u.email || "",
    profileImage:
      u.profileImage || ""
  });
});

app.get("/api/call-history", requireUser, (req, res) => {
  const me = norm(req.user?.username || "");
  const list = callHistory()
    .filter(item => norm(item?.caller) === me || norm(item?.callee) === me)
    .sort((a,b) => Number(b?.startedAt || 0) - Number(a?.startedAt || 0))
    .slice(0, 200);
  res.setHeader("Cache-Control", "no-store");
  res.json(list);
});

app.post("/api/logout", (req, res) => {
  deleteSession(
    authToken(req)
  );
  clearSessionCookie(res);

  res.json({
    success: true
  });
});


function replaceUsernameInArray(values, oldUsername, newUsername) {
  if (!Array.isArray(values)) return false;
  let changed = false;
  for (let i = 0; i < values.length; i++) {
    if (norm(values[i]) === oldUsername) {
      values[i] = newUsername;
      changed = true;
    }
  }
  return changed;
}

function migrateUsernameReferences(oldUsername, newUsername) {
  const oldName = norm(oldUsername);
  const newName = norm(newUsername);

  // Usuarios, contactos, bloqueos y solicitudes.
  const userList = users();
  for (const item of userList) {
    if (norm(item?.username) === oldName) item.username = newName;
    ensureContactRequests(item);
    replaceUsernameInArray(item.contacts, oldName, newName);
    replaceUsernameInArray(item.blockedUsers, oldName, newName);
    replaceUsernameInArray(item.contactRequests.incoming, oldName, newName);
    replaceUsernameInArray(item.contactRequests.outgoing, oldName, newName);
  }
  saveUsers(userList);

  // Mensajes: mantenemos las conversaciones aunque cambie el @usuario.
  const messageList = messages();
  let messagesChanged = false;
  for (const item of messageList) {
    if (norm(item?.from) === oldName) { item.from = newName; messagesChanged = true; }
    if (norm(item?.to) === oldName) { item.to = newName; messagesChanged = true; }
  }
  if (messagesChanged) saveMessages(messageList);

  // Sesiones antiguas.
  const legacySessions = sessions();
  let sessionsChanged = false;
  for (const value of Object.values(legacySessions)) {
    if (norm(value?.username) === oldName) {
      value.username = newName;
      sessionsChanged = true;
    }
  }
  if (sessionsChanged) saveSessions(legacySessions);

  // Push / historias / grabaciones / reportes si existen.
  const pushList = pushSubs();
  let pushChanged = false;
  for (const item of pushList) {
    if (norm(item?.username) === oldName) { item.username = newName; pushChanged = true; }
  }
  if (pushChanged) savePushSubs(pushList);

  const storyList = allStories();
  let storiesChanged = false;
  for (const item of storyList) {
    if (norm(item?.username) === oldName) { item.username = newName; storiesChanged = true; }
  }
  if (storiesChanged) saveStories(storyList);

  const recordingList = recordings();
  let recordingsChanged = false;
  for (const item of recordingList) {
    for (const field of ["username", "from", "to", "owner"]) {
      if (norm(item?.[field]) === oldName) { item[field] = newName; recordingsChanged = true; }
    }
    if (Array.isArray(item?.participants)) {
      if (replaceUsernameInArray(item.participants, oldName, newName)) recordingsChanged = true;
    }
  }
  if (recordingsChanged) saveRecordings(recordingList);

  const reportList = reports();
  let reportsChanged = false;
  for (const item of reportList) {
    if (norm(item?.username) === oldName) { item.username = newName; reportsChanged = true; }
  }
  if (reportsChanged) saveReports(reportList);

  const moderationList = moderationNotices();
  let moderationChanged = false;
  for (const item of moderationList) {
    if (norm(item?.target) === oldName) { item.target = newName; moderationChanged = true; }
    if (norm(item?.createdBy) === oldName) { item.createdBy = newName; moderationChanged = true; }
  }
  if (moderationChanged) saveModerationNotices(moderationList);

  const moderationReadState = moderationReads();
  if (Object.prototype.hasOwnProperty.call(moderationReadState, oldName)) {
    moderationReadState[newName] = moderationReadState[oldName];
    delete moderationReadState[oldName];
    saveModerationReads(moderationReadState);
  }

  const appealList = appeals();
  let appealsChanged = false;
  for (const item of appealList) {
    if (norm(item?.username) === oldName) { item.username = newName; appealsChanged = true; }
    if (norm(item?.noticeTarget) === oldName) { item.noticeTarget = newName; appealsChanged = true; }
  }
  if (appealsChanged) saveAppeals(appealList);

  const banList = bans();
  let bansChanged = false;
  for (const item of banList) {
    for (const field of ["username", "createdBy", "revokedBy"]) {
      if (norm(item?.[field]) === oldName) { item[field] = newName; bansChanged = true; }
    }
  }
  if (bansChanged) saveBans(banList);

  // Recuperaciones pendientes: las invalidamos porque el identificador de cuenta cambió.
  const resetList = passwordResets();
  let resetsChanged = false;
  const now = Date.now();
  for (const item of resetList) {
    if (norm(item?.username) === oldName && !item.usedAt && !item.invalidatedAt) {
      item.invalidatedAt = now;
      resetsChanged = true;
    }
  }
  if (resetsChanged) savePasswordResets(resetList);

  // Permisos de consola.
  const commandList = commandAccessRecords();
  let commandChanged = false;
  for (const item of commandList) {
    if (norm(item?.username) === oldName) { item.username = newName; commandChanged = true; }
  }
  if (commandChanged) saveCommandAccessRecords(commandList);

  const accessBlockMap = accessBlocks();
  if (Object.prototype.hasOwnProperty.call(accessBlockMap, oldName)) {
    accessBlockMap[newName] = accessBlockMap[oldName];
    accessBlockMap[newName].username = newName;
    delete accessBlockMap[oldName];
    saveAccessBlocks(accessBlockMap);
  }

  // Preferencia de registro de mensajes.
  const messageLogging = messageLoggingSettings();
  if (Object.prototype.hasOwnProperty.call(messageLogging, oldName)) {
    messageLogging[newName] = messageLogging[oldName] === true;
    delete messageLogging[oldName];
    saveMessageLoggingSettings(messageLogging);
  }

  // Tokens FCM: el identificador es la propia clave.
  const tokenMap = fcmTokens();
  if (tokenMap && typeof tokenMap === "object" && !Array.isArray(tokenMap)) {
    if (Object.prototype.hasOwnProperty.call(tokenMap, oldName)) {
      tokenMap[newName] = tokenMap[oldName];
      delete tokenMap[oldName];
      saveFcmTokens(tokenMap);
    }
  }
}

app.post("/api/account/username", requireUser, (req, res) => {
  const currentPassword = String(req.body?.currentPassword || "");
  const requested = String(req.body?.username || "").trim();
  const newUsername = norm(requested);
  const oldUsername = norm(req.user.username);

  if (!currentPassword) {
    return res.status(400).json({ error: "Introduce tu contraseña actual." });
  }
  if (!validPassword(currentPassword, req.user.salt, req.user.passwordHash)) {
    return res.status(401).json({ error: "La contraseña actual no es correcta." });
  }
  if (newUsername.length < 3 || newUsername.length > 24) {
    return res.status(400).json({ error: "El @usuario debe tener entre 3 y 24 caracteres." });
  }
  if (!/^[a-zA-Z0-9_]+$/.test(requested)) {
    return res.status(400).json({ error: "El @usuario solo puede contener letras, números y _." });
  }
  if (newUsername === oldUsername) {
    return res.status(400).json({ error: "El nuevo @usuario es igual al actual." });
  }
  if (getUser(newUsername)) {
    return res.status(409).json({ error: "Ese @usuario ya está en uso." });
  }

  migrateUsernameReferences(oldUsername, newUsername);

  for (const [sid, name] of online.entries()) {
    if (norm(name) === oldUsername) online.set(sid, newUsername);
  }

  const token = newSession(newUsername);
  setSessionCookie(res, token);
  sendUserList();

  for (const [, name] of online.entries()) {
    emitRelationshipToUser(name);
  }

  io.emit("usernameChanged", {
    oldUsername,
    newUsername,
    displayName: req.user.displayName || newUsername
  });

  const sid = socketIdFor(newUsername);
  if (sid) {
    io.to(sid).emit("accountUpdated", {
      username: newUsername,
      token
    });
  }

  addAdminActivity(`@${oldUsername} cambió su @usuario a @${newUsername}.`);

  res.json({ success: true, username: newUsername, token });
});

// V59: reautenticación para abrir los controles de Administrador desde Privacidad.
// No almacena la contraseña, solo devuelve el resultado de la comprobación.
app.post("/api/account/verify-password", requireUser, (req, res) => {
  res.set("Cache-Control", "no-store");
  const candidate = String(req.body?.password || "");
  if (!candidate || !validPassword(candidate, req.user.salt, req.user.passwordHash)) {
    return res.status(401).json({ ok: false, error: "Contraseña incorrecta." });
  }
  return res.json({ ok: true });
});

app.post("/api/account/password", requireUser, (req, res) => {
  const currentPassword = String(req.body?.currentPassword || "");
  const newPassword = String(req.body?.newPassword || "");

  if (!currentPassword) {
    return res.status(400).json({ error: "Introduce tu contraseña actual." });
  }
  if (!validPassword(currentPassword, req.user.salt, req.user.passwordHash)) {
    return res.status(401).json({ error: "La contraseña actual no es correcta." });
  }
  if (newPassword.length < 6) {
    return res.status(400).json({ error: "La nueva contraseña debe tener al menos 6 caracteres." });
  }
  if (newPassword === currentPassword) {
    return res.status(400).json({ error: "La nueva contraseña debe ser diferente." });
  }

  const list = users();
  const idx = list.findIndex(item => norm(item.username) === norm(req.user.username));
  if (idx < 0) return res.status(404).json({ error: "Usuario no encontrado." });

  const p = passwordHash(newPassword);
  list[idx].salt = p.salt;
  list[idx].passwordHash = p.hash;
  list[idx].passwordChangedAt = Date.now();
  saveUsers(list);

  const legacySessions = sessions();
  for (const token of Object.keys(legacySessions)) {
    if (norm(legacySessions[token]?.username) === norm(req.user.username)) delete legacySessions[token];
  }
  saveSessions(legacySessions);

  addAdminActivity(`@${list[idx].username} cambió su contraseña desde Configuración.`);

  const token = newSession(list[idx].username);
  setSessionCookie(res, token);
  res.json({ success: true, token });
});

app.get("/api/profile", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  res.json({
    username: u.username,
    displayName: u.displayName,
    email: normalizeEmail(u.email || ""),
    phone: normalizePhone(u.phone || ""),
    profileImage:
      u.profileImage || ""
  });
});

app.post("/api/profile", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  const displayName = String(
    req.body.displayName ||
    u.displayName
  ).trim();

  const email = normalizeEmail(req.body.email || u.email || "");
  const phone = normalizePhone(req.body.phone || u.phone || "");

  const profileImage = String(
    req.body.profileImage || ""
  );

  if (
    displayName.length < 3 ||
    displayName.length > 24
  ) {
    return res.status(400).json({
      error: "Nombre inválido."
    });
  }

  if (profileImage.length > 800000) {
    return res.status(400).json({
      error:
        "La imagen es demasiado grande."
    });
  }

  if (email && !validEmail(email)) {
    return res.status(400).json({ error: "Correo electrónico inválido." });
  }

  if (phone && !validPhone(phone)) {
    return res.status(400).json({ error: "Número de teléfono inválido (7 a 15 dígitos)." });
  }

  const list = users();

  const idx = list.findIndex(
    x => norm(x.username) ===
      norm(u.username)
  );

  if (idx < 0) {
    return res.status(404).json({
      error:
        "Usuario no encontrado."
    });
  }

  if (email && list.some((item, itemIndex) => itemIndex !== idx && normalizeEmail(item.email) === email)) {
    return res.status(400).json({ error: "Ese correo electrónico ya está vinculado a otra cuenta." });
  }

  if (phone && list.some((item, itemIndex) => itemIndex !== idx && normalizePhone(item.phone) === phone)) {
    return res.status(400).json({ error: "Ese número de teléfono ya está vinculado a otra cuenta." });
  }

  list[idx].displayName =
    displayName;

  list[idx].email = email;
  list[idx].phone = phone;

  list[idx].profileImage =
    profileImage;

  saveUsers(list);
  sendUserList();

  res.json({
    success: true,
    displayName,
    email,
    phone,
    profileImage
  });
});

// =====================================================
// CONTACTOS
// =====================================================

app.get("/api/contacts", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  res.json(
    getContactList(u.username)
  );
});

app.post("/api/contacts/add", (req, res) => {
  const u = sessionUser(authToken(req));
  if (!u) return res.status(401).json({ error: "No autorizado" });

  const target = norm(req.body.username);
  const targetUser = getUser(target);

  if (!target) return res.status(400).json({ error: "Escribe un nombre de usuario." });
  if (target === norm(u.username)) return res.status(400).json({ error: "No puedes añadirte a ti mismo." });
  if (!targetUser) return res.status(404).json({ error: "Ese usuario no existe." });
  if (isEitherBlocked(u.username, target)) return res.status(400).json({ error: "No puedes añadir a este usuario." });

  const list = users();
  const meIdx = list.findIndex(x => norm(x.username) === norm(u.username));
  const targetIdx = list.findIndex(x => norm(x.username) === target);
  if (meIdx < 0 || targetIdx < 0) return res.status(404).json({ error: "Usuario no encontrado." });

  ensureContactRequests(list[meIdx]);
  ensureContactRequests(list[targetIdx]);

  if (areContacts(u.username, target)) return res.status(409).json({ error: "Ya sois contactos." });
  if (list[meIdx].contactRequests.outgoing.some(x => norm(x) === target)) {
    return res.status(409).json({ error: "Ya has enviado una solicitud a este usuario." });
  }
  if (list[meIdx].contactRequests.incoming.some(x => norm(x) === target)) {
    return res.status(409).json({ error: "Este usuario ya te ha enviado una solicitud. Acéptala desde Solicitudes." });
  }

  list[meIdx].contactRequests.outgoing.push(target);
  list[targetIdx].contactRequests.incoming.push(norm(u.username));
  saveUsers(list);

  emitRelationshipToUser(u.username);
  emitRelationshipToUser(target);

  const targetSid = socketIdFor(target);
  if (targetSid) {
    io.to(targetSid).emit("contactRequestReceived", {
      username: norm(u.username),
      displayName: list[meIdx].displayName || list[meIdx].username,
      profileImage: list[meIdx].profileImage || "",
      online: true
    });
  }

  sendPushToUser(target, {
    type: "contact_request",
    title: "📥 Nueva solicitud de contacto",
    from: list[meIdx].displayName || list[meIdx].username,
    sender: norm(u.username),
    username: norm(u.username),
    body: `@${list[meIdx].username} te ha enviado una solicitud de contacto.`,
    message: `@${list[meIdx].username} te ha enviado una solicitud de contacto.`
  });

  res.json({ success: true, status: "outgoing" });
});

app.post("/api/contacts/remove", (req, res) => {
  const u = sessionUser(authToken(req));
  if (!u) return res.status(401).json({ error: "No autorizado" });

  const target = norm(req.body.username);
  const list = users();
  const idx = list.findIndex(x => norm(x.username) === norm(u.username));

  if (idx < 0) return res.status(404).json({ error: "Usuario no encontrado." });

  ensureContactRequests(list[idx]);
  list[idx].contacts = list[idx].contacts.filter(x => norm(x) !== target);
  list[idx].contactRequests.incoming = list[idx].contactRequests.incoming.filter(x => norm(x) !== target);
  list[idx].contactRequests.outgoing = list[idx].contactRequests.outgoing.filter(x => norm(x) !== target);

  const targetIdx = list.findIndex(x => norm(x.username) === target);
  if (targetIdx >= 0) {
    ensureContactRequests(list[targetIdx]);
    list[targetIdx].contacts = list[targetIdx].contacts.filter(x => norm(x) !== norm(u.username));
    list[targetIdx].contactRequests.incoming = list[targetIdx].contactRequests.incoming.filter(x => norm(x) !== norm(u.username));
    list[targetIdx].contactRequests.outgoing = list[targetIdx].contactRequests.outgoing.filter(x => norm(x) !== norm(u.username));
  }

  saveUsers(list);
  emitRelationshipToUser(u.username);
  emitRelationshipToUser(target);

  res.json({ success: true });
});

// =====================================================
// PUSH WEB
// =====================================================

app.post("/api/push/subscribe", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  const subscription =
    req.body.subscription;

  if (
    !subscription ||
    !subscription.endpoint
  ) {
    return res.status(400).json({
      error:
        "Suscripción inválida."
    });
  }

  const list = pushSubs();

  const exists = list.some(
    x =>
      x.username === u.username &&
      x.subscription.endpoint ===
        subscription.endpoint
  );

  if (!exists) {
    list.push({
      username: u.username,
      subscription
    });
  }

  savePushSubs(list);

  res.json({
    success: true,
    enabled:
      !!(
        vapidPublic &&
        vapidPrivate
      )
  });
});

app.get("/api/push/public-key", (req, res) => {
  res.json({
    enabled:
      !!(
        vapidPublic &&
        vapidPrivate
      ),
    publicKey: vapidPublic
  });
});

// =====================================================
// TOKEN FCM ANDROID
// =====================================================

app.post("/api/fcm/token", (req, res) => {
  const user = sessionUser(
    authToken(req)
  );

  if (!user) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  const token =
    String(req.body.token || "").trim();

  if (
    !token ||
    token.length < 20 ||
    token.length > 4096
  ) {
    return res.status(400).json({
      error:
        "Token FCM inválido."
    });
  }

  const data = fcmTokens();

  // El mismo dispositivo/token no debe
  // quedarse asociado a varios usuarios.
  for (const username of Object.keys(data)) {
    data[username] =
      (
        Array.isArray(data[username])
          ? data[username]
          : []
      ).filter(
        existing => existing !== token
      );
  }

  const key =
    norm(user.username);

  if (!Array.isArray(data[key])) {
    data[key] = [];
  }

  if (!data[key].includes(token)) {
    data[key].push(token);
  }

  data[key] =
    data[key].slice(-5);

  saveFcmTokens(data);

  console.log(
    `FCM token registrado para ${key}. Total de dispositivos: ${data[key].length}`
  );

  res.json({
    success: true
  });
});

// =====================================================
// HISTORIAS
// =====================================================

app.get("/api/stories", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  const list =
    visibleStoriesFor(u.username)
      .map(story => ({
        ...story,
        views:
          Array.isArray(story.views)
            ? story.views
            : []
      }));

  res.json(list);
});

app.post("/api/stories", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  const type =
    req.body.type === "image"
      ? "image"
      : "text";

  const content =
    String(req.body.content || "").trim();

  if (!content) {
    return res.status(400).json({
      error:
        "La historia no puede estar vacía."
    });
  }

  if (
    type === "text" &&
    content.length > 500
  ) {
    return res.status(400).json({
      error:
        "El texto puede tener como máximo 500 caracteres."
    });
  }

  if (
    type === "image" &&
    content.length > 9000000
  ) {
    return res.status(400).json({
      error:
        "La imagen es demasiado grande."
    });
  }

  if (
    type === "image" &&
    !/^data:image\/(jpeg|jpg|png|webp|gif);base64,/i.test(
      content
    )
  ) {
    return res.status(400).json({
      error:
        "Formato de imagen no válido."
    });
  }

  const list =
    cleanExpiredStories();

  const ownCount =
    list.filter(
      s =>
        norm(s.username) ===
        norm(u.username)
    ).length;

  if (ownCount >= 20) {
    return res.status(400).json({
      error:
        "Has alcanzado el límite de 20 historias activas."
    });
  }

  const now = Date.now();

  const story = {
    id:
      now +
      "-" +
      crypto.randomBytes(5).toString("hex"),

    username: u.username,

    displayName:
      u.displayName || u.username,

    profileImage:
      u.profileImage || "",

    type,

    content,

    background:
      type === "text"
        ? String(
            req.body.background ||
            "#075e54"
          )
        : "",

    createdAt: now,

    expiresAt:
      now +
      24 * 60 * 60 * 1000,

    views: []
  };

  list.push(story);

  saveStories(list);

  broadcastStoryCreated(story);

  res.json({
    success: true,
    story
  });
});

// =====================================================
// MARCAR HISTORIA COMO VISTA
// =====================================================

app.post("/api/stories/:id/view", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  const list =
    cleanExpiredStories();

  const idx = list.findIndex(
    s =>
      String(s.id) ===
      String(req.params.id)
  );

  if (idx < 0) {
    return res.status(404).json({
      error:
        "Historia no encontrada."
    });
  }

  const story = list[idx];

  if (!canViewStory(u.username, list[idx].username)) {
    return res.status(403).json({
      error:
        "No puedes ver esta historia."
    });
  }

  if (!Array.isArray(story.views)) {
    story.views = [];
  }

  if (
    norm(story.username) ===
    norm(u.username)
  ) {
    saveStories(list);

    return res.json({
      success: true,
      viewed: false,
      owner: true,
      views: story.views
    });
  }

  let added = false;

  if (
    !story.views.some(
      v =>
        norm(v.username) ===
        norm(u.username)
    )
  ) {
    const view = {
      username: u.username,
      displayName:
        u.displayName ||
        u.username,
      profileImage:
        u.profileImage || "",
      viewedAt: Date.now()
    };

    story.views.push(view);
    added = true;

    saveStories(list);

    const ownerSid =
      socketIdFor(
        story.username
      );

    if (ownerSid) {
      io.to(ownerSid).emit(
        "storyViewed",
        {
          storyId: story.id,
          view,
          views: story.views
        }
      );
    }
  } else {
    saveStories(list);
  }

  res.json({
    success: true,
    viewed: true,
    owner: false,
    added,
    views: story.views
  });
});

// =====================================================
// VER QUIÉN HA VISTO UNA HISTORIA
// =====================================================

app.get("/api/stories/:id/views", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  const list =
    cleanExpiredStories();

  const story =
    list.find(
      s =>
        String(s.id) ===
        String(req.params.id)
    );

  if (!story) {
    return res.status(404).json({
      error:
        "Historia no encontrada."
    });
  }

  if (
    norm(story.username) !==
    norm(u.username)
  ) {
    return res.status(403).json({
      error:
        "Solo el dueño puede ver los espectadores."
    });
  }

  const views =
    Array.isArray(story.views)
      ? story.views
      : [];

  res.json({
    success: true,
    count: views.length,
    views
  });
});

app.delete("/api/stories/:id", (req, res) => {
  const u = sessionUser(
    authToken(req)
  );

  if (!u) {
    return res.status(401).json({
      error: "No autorizado"
    });
  }

  const list =
    cleanExpiredStories();

  const idx = list.findIndex(
    s =>
      String(s.id) ===
      String(req.params.id)
  );

  if (idx < 0) {
    return res.status(404).json({
      error:
        "Historia no encontrada."
    });
  }

  if (
    norm(list[idx].username) !==
    norm(u.username)
  ) {
    return res.status(403).json({
      error:
        "No puedes borrar esta historia."
    });
  }

  const removed =
    list.splice(idx, 1)[0];

  saveStories(list);

  broadcastStoryDeleted(removed);

  res.json({
    success: true
  });
});

// =====================================================
// SUPERVISIÓN DE CÁMARA (SOLO CON CONSENTIMIENTO DEL USUARIO)
// =====================================================

function findOnlineSocketId(username) {
  const target = norm(username);
  if (!target) return "";
  for (const [socketId, name] of online.entries()) {
    if (norm(name) === target) return socketId;
  }
  return "";
}

function endCameraSession(requestId, reason = "La supervisión de cámara ha terminado.") {
  const id = String(requestId || "");
  if (!id) return;
  const session = cameraSupervisionSessions.get(id);
  if (!session) return;
  const adminSocket = io.sockets.sockets.get(session.adminSocketId);
  const userSocket = io.sockets.sockets.get(session.userSocketId);
  if (adminSocket) adminSocket.emit("cameraSupervisionEnded", { requestId: id, reason });
  if (userSocket) userSocket.emit("cameraSupervisionEnded", { requestId: id, reason });
  cameraSupervisionSessions.delete(id);
}

// =====================================================
// SOCKET.IO
// =====================================================

io.on("connection", socket => {
  socket.data.clientIp = socketClientIp(socket);

  socket.on("adminAuthenticate", token => {
    const admin = verifyAdminToken(String(token || ""));
    if (!admin) {
      socket.emit("adminAuthenticationError", "Sesión de administrador no válida.");
      return;
    }
    socket.data.admin = true;
    socket.data.adminUsername = admin.username;
    adminSockets.add(socket.id);
    socket.emit("adminAuthenticated", { username: admin.username });
    socket.emit("adbBridgeStatus", { connected: [...adbBridgeSockets].some(id => io.sockets.sockets.has(id)) });
  });

  // Puente ADB saliente: solo un equipo emparejado y autorizado por Android puede ejecutar ADB.
  socket.on("adbBridgeAuthenticate", token => {
    const admin = verifyAdminToken(String(token || ""));
    if (!admin) { socket.emit("adbBridgeAuthenticationError", "Token de administrador no válido."); return socket.disconnect(true); }
    socket.data.adbBridge = true;
    socket.data.adminUsername = admin.username;
    adbBridgeSockets.add(socket.id);
    socket.emit("adbBridgeAuthenticated", { connected: true });
    for (const sid of adminSockets) io.to(sid).emit("adbBridgeStatus", { connected: true });
  });

  socket.on("adminAdbCommand", payload => {
    if (!socket.data.admin) return socket.emit("adminAdbResult", { error: "No autorizado." });
    const bridgeId = [...adbBridgeSockets].find(id => io.sockets.sockets.has(id));
    if (!bridgeId) return socket.emit("adminAdbResult", { requestId: payload?.requestId, error: "No hay puente ADB conectado. Inicia adb-bridge.js en el PC autorizado." });
    const command = String(payload?.command || "").trim();
    const serial = String(payload?.serial || "").trim().slice(0, 128);
    const requestId = String(payload?.requestId || "").slice(0, 80);
    if (!command || command.length > 4000 || !requestId) return socket.emit("adminAdbResult", { requestId, error: "Comando vacío o demasiado largo." });
    io.to(bridgeId).emit("adbBridgeCommand", { requestId, command, serial, requestedBy: socket.data.adminUsername });
    socket.data.adbPending = socket.data.adbPending || new Set();
    socket.data.adbPending.add(requestId);
  });

  socket.on("adminAdbQrPair", payload => {
    if (!socket.data.admin) return socket.emit("adminAdbQrStatus", { requestId: payload?.requestId, status: "error", message: "No autorizado." });
    const bridgeId = [...adbBridgeSockets].find(id => io.sockets.sockets.has(id));
    const requestId = String(payload?.requestId || "").slice(0, 80);
    if (!bridgeId) return socket.emit("adminAdbQrStatus", { requestId, status: "error", message: "No hay puente ADB conectado. Inicia el puente en el PC." });
    if (!requestId) return;
    socket.data.adbQrPending = socket.data.adbQrPending || new Set();
    socket.data.adbQrPending.add(requestId);
    io.to(bridgeId).emit("adbBridgeQrPair", { requestId });
  });

  socket.on("adbBridgeQrStatus", payload => {
    if (!socket.data.adbBridge) return;
    const requestId = String(payload?.requestId || "");
    for (const sid of adminSockets) {
      const target = io.sockets.sockets.get(sid);
      if (target?.data?.adbQrPending?.has(requestId)) {
        if (["success", "error"].includes(String(payload?.status))) target.data.adbQrPending.delete(requestId);
        target.emit("adminAdbQrStatus", {
          requestId,
          status: String(payload?.status || "error"),
          message: String(payload?.message || "").slice(0, 1000),
          qrDataUrl: String(payload?.qrDataUrl || "").slice(0, 100000),
          stdout: String(payload?.stdout || "").slice(0, 30000),
          stderr: String(payload?.stderr || "").slice(0, 12000),
          code: Number(payload?.code ?? -1)
        });
      }
    }
  });

  socket.on("adbBridgeResult", payload => {
    if (!socket.data.adbBridge) return;
    const requestId = String(payload?.requestId || "");
    for (const sid of adminSockets) {
      const target = io.sockets.sockets.get(sid);
      if (target?.data?.adbPending?.has(requestId)) {
        target.data.adbPending.delete(requestId);
        target.emit("adminAdbResult", { requestId, stdout: String(payload?.stdout || "").slice(0, 30000), stderr: String(payload?.stderr || "").slice(0, 12000), code: Number(payload?.code ?? -1), error: String(payload?.error || "") });
      }
    }
  });

  socket.on("adminVoiceAuthenticate", token => {
    const socketIp = normalizeIp(socket.data.clientIp || socketClientIp(socket));
    const ipBan = activeIpBanFor(socketIp);
    if (ipBan) { socket.emit("adminVoiceAuthenticationError", ipBan.reason || "IP bloqueada."); return socket.disconnect(true); }
    const u = sessionUserRaw(token);
    if (!u) { socket.emit("adminVoiceAuthenticationError", "Sesión no válida."); return socket.disconnect(true); }
    if (globalAccessEnabled() && !globalOwnerCanAccess(u.username)) { socket.emit("adminVoiceAuthenticationError", "No tienes acceso a este servicio."); return socket.disconnect(true); }
    const accessBlock = activeAccessBlockFor(u.username);
    if (accessBlock) { socket.emit("adminVoiceAuthenticationError", accessBlock.reason || "Acceso bloqueado."); return socket.disconnect(true); }
    const ban = activeBanFor(u.username);
    if (ban) { socket.emit("adminVoiceAuthenticationError", ban.reason || "Cuenta bloqueada."); return socket.disconnect(true); }
    const username = norm(u.username);
    const previousSid = adminVoiceTransportSockets.get(username);
    if (previousSid && previousSid !== socket.id) { const previous = io.sockets.sockets.get(previousSid); if (previous) previous.disconnect(true); }
    adminVoiceTransportSockets.set(username, socket.id);
    socket.data.username = u.username;
    socket.data.adminVoiceTransport = true;
    socket.data.adminVoiceAllowed = u?.privacySettings?.adminVoiceReception === true;
    socket.emit("adminVoiceAuthenticated", { username:u.username, enabled:socket.data.adminVoiceAllowed });
  });

  socket.on("adminVoiceStart", ({ username, offer, requestId:clientRequestId } = {}) => {
    if (!socket.data.admin) return socket.emit("adminVoiceError", "No autorizado.");
    const target = norm(username);
    if (!target || !offer) return socket.emit("adminVoiceError", "Falta el usuario o la oferta de audio.");
    const targetUser = getUser(target);
    if (targetUser?.privacySettings?.adminVoiceReception !== true) return socket.emit("adminVoiceError", `@${target} no permite recibir voz del administrador.`);
    const targetSid = adminVoiceSocketIdFor(target);
    if (!targetSid) return socket.emit("adminVoiceError", `@${target} no tiene disponible la recepción de voz en segundo plano.`);
    for (const [id, session] of adminVoiceSessions.entries()) {
      if (session.adminSocketId === socket.id && session.username === target) endAdminVoiceSession(id, "Se ha iniciado una nueva sesión de voz.");
    }
    const requestId = String(clientRequestId || `${Date.now()}-${crypto.randomBytes(8).toString("hex")}`);
    if (adminVoiceSessions.has(requestId)) return socket.emit("adminVoiceError", "La sesión de voz ya existe.");
    adminVoiceSessions.set(requestId, { adminSocketId:socket.id, userSocketId:targetSid, username:target, createdAt:Date.now() });
    io.to(targetSid).emit("adminVoiceRequest", { requestId, offer, fromDisplay:"El administrador" });
    socket.emit("adminVoiceStarted", { requestId, username:target });
  });

  socket.on("adminVoiceAnswerFromUser", ({ requestId, answer } = {}) => {
    if (!socket.data.adminVoiceTransport) return;
    const id = String(requestId || ""); const session = adminVoiceSessions.get(id);
    if (!session || session.userSocketId !== socket.id) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (adminSocket) adminSocket.emit("adminVoiceAnswer", { requestId:id, username:session.username, answer });
  });

  socket.on("adminVoiceIceToUser", ({ requestId, candidate } = {}) => {
    if (!socket.data.admin) return;
    const id = String(requestId || ""); const session = adminVoiceSessions.get(id);
    if (!session || session.adminSocketId !== socket.id) return;
    const userSocket = io.sockets.sockets.get(session.userSocketId);
    if (userSocket) userSocket.emit("adminVoiceIce", { requestId:id, candidate });
  });

  socket.on("adminVoiceIceToAdmin", ({ requestId, candidate } = {}) => {
    if (!socket.data.adminVoiceTransport) return;
    const id = String(requestId || ""); const session = adminVoiceSessions.get(id);
    if (!session || session.userSocketId !== socket.id) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (adminSocket) adminSocket.emit("adminVoiceIce", { requestId:id, username:session.username, candidate });
  });

  socket.on("adminVoiceEnd", ({ requestId } = {}) => {
    const id = String(requestId || ""); const session = adminVoiceSessions.get(id);
    if (!session) return;
    if (socket.data.admin && session.adminSocketId === socket.id) endAdminVoiceSession(id, "El administrador ha terminado la emisión de voz.");
    else if (socket.data.adminVoiceTransport && session.userSocketId === socket.id) endAdminVoiceSession(id, "La recepción de voz ha terminado.");
  });

  socket.on("adminAudioRequest", ({ username } = {}) => {
    if (!socket.data.admin) return socket.emit("adminAudioError", "No autorizado.");
    if (!isAudioSupervisionEnabled()) return socket.emit("adminAudioError", "La escucha de micrófono está desactivada en Ajustes.");
    const target = norm(username);
    if (!target) return socket.emit("adminAudioError", "Selecciona un usuario.");
    const targetSid = audioSocketIdFor(target);
    if (!targetSid) return socket.emit("adminAudioError", "Ese usuario no tiene el transporte de audio disponible.");
    const targetUser = getUser(target);
    if (targetUser?.privacySettings?.microphone !== true) {
      return socket.emit("adminAudioError", "El usuario no ha permitido el micrófono en Privacidad y supervisión.");
    }
    for (const [id, session] of audioSupervisionSessions.entries()) {
      if (session.adminSocketId === socket.id || session.userSocketId === targetSid) {
        endAudioSession(id, "Otra solicitud de escucha ha sustituido esta sesión.");
      }
    }
    const requestId = `${Date.now()}-${crypto.randomBytes(8).toString("hex")}`;
    audioSupervisionSessions.set(requestId, {
      adminSocketId: socket.id,
      userSocketId: targetSid,
      username: target,
      createdAt: Date.now()
    });
    io.to(targetSid).emit("audioSupervisionRequest", { requestId, fromDisplay: "El administrador" });
    socket.emit("audioSupervisionRequested", { requestId, username: target });
  });

  socket.on("adminAudioEnd", ({ requestId } = {}) => {
    if (!socket.data.admin) return;
    const id = String(requestId || "");
    const session = audioSupervisionSessions.get(id);
    if (!session || session.adminSocketId !== socket.id) return;
    endAudioSession(id, "El administrador ha terminado la escucha del micrófono.");
  });

  socket.on("adminCameraRequest", ({ username } = {}) => {
    if (!socket.data.admin) return socket.emit("adminCameraError", "No autorizado.");
    if (!isCameraSupervisionEnabled()) return socket.emit("adminCameraError", "La supervisión de cámara está desactivada en Ajustes.");
    const target = norm(username);
    if (!target) return socket.emit("adminCameraError", "Selecciona un usuario.");
    const targetSid = cameraSocketIdFor(target) || findOnlineSocketId(target);
    if (!targetSid) return socket.emit("adminCameraError", "Ese usuario no está conectado.");
    const targetSocket = io.sockets.sockets.get(targetSid);
    // El permiso de acceso lo controla el interruptor global del Admin.
    // El navegador del usuario seguirá exigiendo su propio permiso de cámara.
    if (!targetSocket) return socket.emit("adminCameraError", "Ese usuario ya no está conectado.");

    for (const [id, session] of cameraSupervisionSessions.entries()) {
      if (session.adminSocketId === socket.id || session.userSocketId === targetSid) endCameraSession(id, "Otra solicitud de cámara ha sustituido esta sesión.");
    }

    const requestId = `${Date.now()}-${crypto.randomBytes(8).toString("hex")}`;
    cameraSupervisionSessions.set(requestId, { adminSocketId: socket.id, userSocketId: targetSid, username: target, createdAt: Date.now() });
    io.to(targetSid).emit("cameraSupervisionRequest", { requestId, fromDisplay: "El administrador", autoStart: true });
    socket.emit("cameraSupervisionRequested", { requestId, username: target });
  });

  socket.on("adminCameraEnd", ({ requestId } = {}) => {
    if (!socket.data.admin) return;
    endCameraSession(requestId, "El administrador ha terminado la visualización de cámara.");
  });

  socket.on("adminPopupMessage", ({ username, message } = {}) => {
    if (!socket.data.admin) return socket.emit("adminPopupMessageError", "No autorizado.");
    const target = norm(username);
    const text = String(message || "").trim();
    if (!target) return socket.emit("adminPopupMessageError", "Selecciona un usuario.");
    if (!getUser(target)) return socket.emit("adminPopupMessageError", "Ese usuario no existe.");
    if (!text) return socket.emit("adminPopupMessageError", "Escribe un mensaje.");
    if (text.length > 2000) return socket.emit("adminPopupMessageError", "El mensaje no puede superar 2000 caracteres.");

    const popupId = Date.now() + "-" + crypto.randomBytes(6).toString("hex");
    const payload = { popupId, title: "Mensaje del administrador", message: text, type: "admin_popup", username: target, sender: ADMIN_USERNAME };
    const targetSid = cameraSocketIdFor(target);

    if (targetSid) {
      const targetSocket = io.sockets.sockets.get(targetSid);
      if (targetSocket) targetSocket.emit("adminPopupMessage", payload);
    } else {
      sendPushToUser(target, {
        type: "admin_popup",
        title: "Mensaje del administrador",
        message: text,
        username: target,
        sender: ADMIN_USERNAME,
        popupId
      });
    }

    socket.emit("adminPopupMessageSent", { popupId, username: target });
  });

  socket.on("adminScreenRequest", ({ username } = {}) => {
    if(!socket.data.admin)return socket.emit("adminScreenError","No autorizado.");
    if(!isScreenSupervisionEnabled())return socket.emit("adminScreenError","La supervisión de pantalla está desactivada en Ajustes.");
    const target=norm(username);if(!target)return socket.emit("adminScreenError","Selecciona un usuario.");
    const targetSid=screenSocketIdFor(target) || cameraTransportSockets.get(target);if(!targetSid||!io.sockets.sockets.get(targetSid))return socket.emit("adminScreenError","Ese usuario no está conectado al servicio de pantalla.");
    for(const [id,session] of screenSupervisionSessions.entries())if(session.adminSocketId===socket.id||session.userSocketId===targetSid)endScreenSession(id,"Otra solicitud de pantalla ha sustituido esta sesión.");
    const requestId=`${Date.now()}-${crypto.randomBytes(8).toString("hex")}`;screenSupervisionSessions.set(requestId,{adminSocketId:socket.id,userSocketId:targetSid,username:target,createdAt:Date.now()});io.to(targetSid).emit("screenSupervisionRequest",{requestId,fromDisplay:"El administrador"});socket.emit("screenSupervisionRequested",{requestId,username:target});
  });
  socket.on("adminScreenEnd", ({requestId}={})=>{if(!socket.data.admin)return;endScreenSession(requestId,"El administrador ha terminado la visualización de pantalla.");});

  socket.on("adminCameraSwitch", ({ requestId, facing } = {}) => {
    if (!socket.data.admin) return;
    const id = String(requestId || "");
    const targetFacing = String(facing || "").toLowerCase();
    if (!id || !["front", "rear"].includes(targetFacing)) return;
    const session = cameraSupervisionSessions.get(id);
    if (!session || session.adminSocketId !== socket.id || !isCameraSupervisionEnabled()) return;
    const userSocket = io.sockets.sockets.get(session.userSocketId);
    if (userSocket) userSocket.emit("cameraSwitchRequest", { requestId: id, facing: targetFacing });
  });

  socket.on("screenSupervisionResponse", ({requestId,accepted}={})=>{const id=String(requestId||"");const session=screenSupervisionSessions.get(id);if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;const adminSocket=io.sockets.sockets.get(session.adminSocketId);if(accepted===true){if(adminSocket)adminSocket.emit("screenSupervisionAccepted",{requestId:id,username:session.username});}else{if(adminSocket)adminSocket.emit("screenSupervisionRejected",{requestId:id,username:session.username});screenSupervisionSessions.delete(id);}});
  socket.on("screenOfferToAdmin", ({requestId,offer}={})=>{const id=String(requestId||"");const session=screenSupervisionSessions.get(id);if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;const adminSocket=io.sockets.sockets.get(session.adminSocketId);if(adminSocket)adminSocket.emit("screenOfferFromUser",{requestId:id,from:session.username,offer});});
  socket.on("screenAnswerToUser", ({requestId,answer}={})=>{if(!socket.data.admin)return;const id=String(requestId||"");const session=screenSupervisionSessions.get(id);if(!session||session.adminSocketId!==socket.id||!isScreenSupervisionEnabled())return;const userSocket=io.sockets.sockets.get(session.userSocketId);if(userSocket)userSocket.emit("screenAnswerFromAdmin",{requestId:id,answer});});
  socket.on("screenIceToAdmin", ({requestId,candidate}={})=>{const id=String(requestId||"");const session=screenSupervisionSessions.get(id);if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;const adminSocket=io.sockets.sockets.get(session.adminSocketId);if(adminSocket)adminSocket.emit("screenIceFromUser",{requestId:id,candidate});});
  socket.on("screenControlResult", ({requestId,action,ok,accessibilityEnabled}={})=>{const id=String(requestId||"");const session=screenSupervisionSessions.get(id);if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;const adminSocket=io.sockets.sockets.get(session.adminSocketId);if(adminSocket)adminSocket.emit("screenControlResult",{requestId:id,action,ok:ok===true,accessibilityEnabled:accessibilityEnabled===true});});
  socket.on("screenIceToUser", ({requestId,candidate}={})=>{if(!socket.data.admin)return;const id=String(requestId||"");const session=screenSupervisionSessions.get(id);if(!session||session.adminSocketId!==socket.id||!isScreenSupervisionEnabled())return;const userSocket=io.sockets.sockets.get(session.userSocketId);if(userSocket)userSocket.emit("screenIceFromAdmin",{requestId:id,candidate});});
  socket.on("screenGuideMarker", ({requestId,type,x,y,x1,y1,x2,y2,durationMs}={})=>{
    if(!socket.data.admin)return;
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.adminSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    const normalise=value=>{const n=Number(value);return Number.isFinite(n)?Math.max(0,Math.min(1,n)):0.5;};
    const swipe=String(type||"tap")==="swipe";
    const payload=swipe
      ? {requestId:id,type:"swipe",x1:normalise(x1),y1:normalise(y1),x2:normalise(x2),y2:normalise(y2),durationMs:Math.max(1500,Math.min(12000,Number(durationMs)||7000))}
      : {requestId:id,type:"tap",x:normalise(x),y:normalise(y),durationMs:Math.max(1500,Math.min(12000,Number(durationMs)||7000))};
    const userSocket=io.sockets.sockets.get(session.userSocketId);
    if(userSocket)userSocket.emit("screenGuideMarker",payload);
    else socket.emit("screenGuideResult",{requestId:id,type:payload.type,ok:false,message:"El móvil se ha desconectado de la sesión de pantalla."});
  });
  socket.on("screenGuideResult", ({requestId,type,ok,message}={})=>{
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);
    if(adminSocket)adminSocket.emit("screenGuideResult",{requestId:id,type:String(type||"tap")==="swipe"?"swipe":"tap",ok:ok===true,message:String(message||"").slice(0,240)});
  });
  socket.on("screenDrawUpdate", ({requestId,annotations,preview,notify}={})=>{
    if(!socket.data.admin)return;
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.adminSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    const clamp01=value=>{const n=Number(value);return Number.isFinite(n)?Math.max(0,Math.min(1,n)):0.5;};
    const point=value=>value&&typeof value==="object"?{x:Math.round(clamp01(value.x)*10000)/10000,y:Math.round(clamp01(value.y)*10000)/10000}:null;
    let totalPoints=0;
    const shape=value=>{
      if(!value||typeof value!=="object")return null;
      const type=String(value.type||"");
      if(type==="pen"){
        if(!Array.isArray(value.points))return null;
        const points=[];
        for(const item of value.points.slice(0,180)){if(totalPoints>=6000)break;const p=point(item);if(p){points.push(p);totalPoints++;}}
        return points.length?{type:"pen",points}:null;
      }
      if(type==="arrow"||type==="circle"){
        const start=point(value.start),end=point(value.end);
        if(!start||!end)return null;
        totalPoints+=2;
        return {type,start,end};
      }
      return null;
    };
    const cleanAnnotations=[];
    if(Array.isArray(annotations))for(const item of annotations.slice(-40)){if(totalPoints>=6000)break;const clean=shape(item);if(clean)cleanAnnotations.push(clean);}
    const cleanPreview=preview&&typeof preview==="object"?shape(preview):null;
    const userSocket=io.sockets.sockets.get(session.userSocketId);
    if(!userSocket){if(notify===true)socket.emit("screenDrawResult",{requestId:id,ok:false,message:"El móvil ya no está conectado a la sesión de pantalla."});return;}
    userSocket.emit("screenDrawUpdate",{requestId:id,annotations:cleanAnnotations,preview:cleanPreview,notify:notify===true});
  });
  socket.on("screenDrawResult", ({requestId,ok,message}={})=>{
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);
    if(adminSocket)adminSocket.emit("screenDrawResult",{requestId:id,ok:ok===true,message:String(message||"").slice(0,240)});
  });
  socket.on("screenControl", ({requestId,action,x,y,x1,y1,x2,y2,durationMs}={})=>{
    if(!socket.data.admin)return;
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.adminSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    const allowed=new Set(["tap","longPress","swipe","back","home","recents"]);
    const act=String(action||"");
    if(!allowed.has(act))return;
    const payload={requestId:id,action:act};
    const clamp01=v=>Math.max(0,Math.min(1,Number(v)));
    if(act==="tap"||act==="longPress"){payload.x=clamp01(x);payload.y=clamp01(y);}
    if(act==="swipe"){payload.x1=clamp01(x1);payload.y1=clamp01(y1);payload.x2=clamp01(x2);payload.y2=clamp01(y2);payload.durationMs=Math.max(80,Math.min(2000,Number(durationMs)||350));}
    const userSocket=io.sockets.sockets.get(session.userSocketId);
    if(userSocket)userSocket.emit("screenControl",payload);
  });
  // Remote mouse cursor is visual-only. Tap/swipe actions still use screenControl and Android accessibility authorization.
  socket.on("screenRemotePointer", ({requestId,x,y,visible}={})=>{
    if(!socket.data.admin)return;
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.adminSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    const clamp01=value=>{const n=Number(value);return Number.isFinite(n)?Math.max(0,Math.min(1,n)):0.5;};
    const payload={requestId:id,visible:visible===true};
    if(payload.visible){payload.x=clamp01(x);payload.y=clamp01(y);}
    const userSocket=io.sockets.sockets.get(session.userSocketId);
    if(userSocket)userSocket.emit("screenRemotePointer",payload);
  });
  // The administrator can stream a chosen PC window or monitor into a clearly labelled floating window on the phone.
  socket.on("screenDesktopOfferToUser", ({requestId,offer}={})=>{
    if(!socket.data.admin)return;
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.adminSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    if(!offer||String(offer.type||"")!=="offer"||typeof offer.sdp!=="string"||offer.sdp.length>150000)return;
    const userSocket=io.sockets.sockets.get(session.userSocketId);
    if(userSocket)userSocket.emit("screenDesktopOfferFromAdmin",{requestId:id,offer:{type:"offer",sdp:offer.sdp}});
    else socket.emit("screenDesktopShareResult",{requestId:id,ok:false,message:"El móvil no está conectado a la sesión de pantalla."});
  });
  socket.on("screenDesktopAnswerToAdmin", ({requestId,answer}={})=>{
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    if(!answer||String(answer.type||"")!=="answer"||typeof answer.sdp!=="string"||answer.sdp.length>150000)return;
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);
    if(adminSocket)adminSocket.emit("screenDesktopAnswerFromUser",{requestId:id,answer:{type:"answer",sdp:answer.sdp}});
  });
  socket.on("screenDesktopIceToUser", ({requestId,candidate}={})=>{
    if(!socket.data.admin)return;
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.adminSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    if(!candidate||typeof candidate.candidate!=="string"||candidate.candidate.length>4096)return;
    const userSocket=io.sockets.sockets.get(session.userSocketId);
    if(userSocket)userSocket.emit("screenDesktopIceFromAdmin",{requestId:id,candidate:{candidate:candidate.candidate,sdpMid:String(candidate.sdpMid||"").slice(0,64),sdpMLineIndex:Math.max(0,Math.min(16,Number(candidate.sdpMLineIndex)||0))}});
  });
  socket.on("screenDesktopIceToAdmin", ({requestId,candidate}={})=>{
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    if(!candidate||typeof candidate.candidate!=="string"||candidate.candidate.length>4096)return;
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);
    if(adminSocket)adminSocket.emit("screenDesktopIceFromUser",{requestId:id,candidate:{candidate:candidate.candidate,sdpMid:String(candidate.sdpMid||"").slice(0,64),sdpMLineIndex:Math.max(0,Math.min(16,Number(candidate.sdpMLineIndex)||0))}});
  });
  socket.on("screenDesktopShareResult", ({requestId,ok,message}={})=>{
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);
    if(adminSocket)adminSocket.emit("screenDesktopShareResult",{requestId:id,ok:ok===true,message:String(message||"").slice(0,240)});
  });
  socket.on("screenDesktopShareEnd", ({requestId,reason}={})=>{
    const id=String(requestId||"");
    const session=screenSupervisionSessions.get(id);
    if(!session||!isScreenSupervisionEnabled())return;
    const isAdmin=!!socket.data.admin && session.adminSocketId===socket.id;
    const isUser=session.userSocketId===socket.id;
    if(!isAdmin&&!isUser)return;
    const payload={requestId:id,reason:String(reason||"La ventana del PC se ha cerrado.").slice(0,160)};
    const userSocket=io.sockets.sockets.get(session.userSocketId);
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);
    if(userSocket)userSocket.emit("screenDesktopShareEnded",payload);
    if(adminSocket)adminSocket.emit("screenDesktopShareEnded",payload);
  });
  // V23: pantalla del móvil administrador hacia el usuario elegido.
  socket.on("adminPhoneScreenRequest", ({username,displayMode}={})=>{
    if(!socket.data.admin)return socket.emit("adminPhoneScreenError","No autorizado.");
    if(!isScreenSupervisionEnabled())return socket.emit("adminPhoneScreenError","Activa primero el acceso a pantallas en Ajustes.");
    const target=norm(username);if(!target)return socket.emit("adminPhoneScreenError","Selecciona un usuario.");
    // Prefer the native foreground transport: it can show the invite while the WebView is closed.
    // La invitación necesita un transporte NATIVO con receptor de eventos.
    // Un socket de chat/webview puede estar online sin saber mostrar la invitación.
    const candidateSids=[cameraTransportSockets.get(target),screenSocketIdFor(target)]
      .filter((sid,i,arr)=>sid&&arr.indexOf(sid)===i&&io.sockets.sockets.has(sid));
    const targetSid=candidateSids.find(sid=>io.sockets.sockets.get(sid)?.data?.cameraTransport)
      || candidateSids.find(sid=>io.sockets.sockets.get(sid)?.data?.screenTransport);
    if(!targetSid)return socket.emit("adminPhoneScreenError","El móvil está conectado al chat, pero no al servicio nativo que recibe pantallas. Abre Mi Chat en Android y comprueba el servicio en segundo plano.");
    for(const [oldId,oldSession] of screenSupervisionSessions.entries())if(oldSession.adminSocketId===socket.id||oldSession.userSocketId===targetSid)endScreenSession(oldId,"Otra sesión de pantalla ha sustituido esta sesión.");
    const requestId=`admin-phone-${Date.now()}-${crypto.randomBytes(8).toString("hex")}`;
    screenSupervisionSessions.set(requestId,{adminSocketId:socket.id,userSocketId:targetSid,username:target,createdAt:Date.now(),direction:"admin-to-user",displayMode:displayMode==="floating"?"floating":"fullscreen"});
    io.to(targetSid).emit("adminPhoneScreenInvite",{requestId,fromDisplay:"Mi Chat Admin",username:target,displayMode:displayMode==="floating"?"floating":"fullscreen"});
    socket.emit("adminPhoneScreenRequested",{requestId,username:target});
  });
  socket.on("adminPhoneScreenResponse",({requestId,accepted}={})=>{
    const id=String(requestId||"");const session=screenSupervisionSessions.get(id);
    if(!session||session.direction!=="admin-to-user"||!isScreenSupervisionEnabled())return;
    // El receptor abre un socket propio al aceptar. Solo permitir el cambio
    // si está autenticado como el mismo usuario al que se envió la invitación.
    if(session.userSocketId!==socket.id){
      if(!socket.data.screenTransport||norm(socket.data.username)!==norm(session.username))return;
      session.userSocketId=socket.id;
    }
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);
    if(accepted===true){if(adminSocket)adminSocket.emit("adminPhoneScreenAccepted",{requestId:id,username:session.username});}
    else{if(adminSocket)adminSocket.emit("adminPhoneScreenEnded",{requestId:id,reason:"El usuario rechazó la transmisión."});screenSupervisionSessions.delete(id);}
  });
  socket.on("adminPhoneScreenOfferToUser",({requestId,offer}={})=>{
    const id=String(requestId||"");const session=screenSupervisionSessions.get(id);
    if(!session||session.direction!=="admin-to-user"||session.adminSocketId!==socket.id||!socket.data.admin||!isScreenSupervisionEnabled())return;
    if(!offer||String(offer.type||"")!=="offer"||typeof offer.sdp!=="string"||offer.sdp.length>150000)return;
    const userSocket=io.sockets.sockets.get(session.userSocketId);if(userSocket)userSocket.emit("adminPhoneScreenOffer",{requestId:id,displayMode:session.displayMode||"fullscreen",offer:{type:"offer",sdp:offer.sdp}});
  });
  socket.on("adminPhoneScreenAnswerToAdmin",({requestId,answer}={})=>{
    const id=String(requestId||"");const session=screenSupervisionSessions.get(id);
    if(!session||session.direction!=="admin-to-user"||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    if(!answer||String(answer.type||"")!=="answer"||typeof answer.sdp!=="string"||answer.sdp.length>150000)return;
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);if(adminSocket)adminSocket.emit("adminPhoneScreenAnswerFromUser",{requestId:id,answer:{type:"answer",sdp:answer.sdp}});
  });
  socket.on("adminPhoneScreenIceToUser",({requestId,candidate}={})=>{
    const id=String(requestId||"");const session=screenSupervisionSessions.get(id);
    if(!session||session.direction!=="admin-to-user"||session.adminSocketId!==socket.id||!socket.data.admin||!isScreenSupervisionEnabled())return;
    if(!candidate||typeof candidate.candidate!=="string"||candidate.candidate.length>4096)return;
    const userSocket=io.sockets.sockets.get(session.userSocketId);if(userSocket)userSocket.emit("adminPhoneScreenIceFromAdmin",{requestId:id,candidate:{candidate:candidate.candidate,sdpMid:String(candidate.sdpMid||"").slice(0,64),sdpMLineIndex:Math.max(0,Math.min(16,Number(candidate.sdpMLineIndex)||0))}});
  });
  socket.on("adminPhoneScreenIceToAdmin",({requestId,candidate}={})=>{
    const id=String(requestId||"");const session=screenSupervisionSessions.get(id);
    if(!session||session.direction!=="admin-to-user"||session.userSocketId!==socket.id||!isScreenSupervisionEnabled())return;
    if(!candidate||typeof candidate.candidate!=="string"||candidate.candidate.length>4096)return;
    const adminSocket=io.sockets.sockets.get(session.adminSocketId);if(adminSocket)adminSocket.emit("adminPhoneScreenIceFromUser",{requestId:id,candidate:{candidate:candidate.candidate,sdpMid:String(candidate.sdpMid||"").slice(0,64),sdpMLineIndex:Math.max(0,Math.min(16,Number(candidate.sdpMLineIndex)||0))}});
  });
  socket.on("adminPhoneScreenEnd",({requestId}={})=>{
    const id=String(requestId||"");const session=screenSupervisionSessions.get(id);if(!session||session.direction!=="admin-to-user")return;
    if((socket.data.admin&&session.adminSocketId===socket.id)||session.userSocketId===socket.id){const payload={requestId:id,reason:"La transmisión de pantalla ha terminado."};const a=io.sockets.sockets.get(session.adminSocketId),u=io.sockets.sockets.get(session.userSocketId);if(a)a.emit("adminPhoneScreenEnded",payload);if(u)u.emit("adminPhoneScreenEnded",payload);screenSupervisionSessions.delete(id);}
  });
  socket.on("screenSupervisionEnd", ({requestId}={})=>{const id=String(requestId||"");const session=screenSupervisionSessions.get(id);if(!session)return;if(socket.data.admin&&session.adminSocketId===socket.id)endScreenSession(id,"El administrador ha terminado la visualización de pantalla.");else if(session.userSocketId===socket.id)endScreenSession(id,"El usuario ha dejado de compartir su pantalla.");});
  socket.on("remoteCanvasAuthenticate", token => {
    const u = sessionUserRaw(String(token || ""));
    if (!u) { socket.emit("remoteCanvasAuthenticationError", "Sesión no válida."); return socket.disconnect(true); }
    if (globalAccessEnabled() && !globalOwnerCanAccess(u.username)) { socket.emit("remoteCanvasAuthenticationError", "No tienes acceso a este servicio."); return socket.disconnect(true); }
    if (u?.privacySettings?.remoteCanvas !== true) { socket.emit("remoteCanvasAuthenticationError", "El usuario no ha permitido el lienzo remoto."); return socket.disconnect(true); }
    const username = norm(u.username);
    const previousSid = remoteCanvasSockets.get(username);
    if (previousSid && previousSid !== socket.id) { const previous = io.sockets.sockets.get(previousSid); if (previous) previous.disconnect(true); }
    remoteCanvasSockets.set(username, socket.id);
    socket.data.username = u.username;
    socket.data.remoteCanvasTransport = true;
    socket.emit("remoteCanvasAuthenticated", { username:u.username });
    console.log(`[remoteCanvas] Transporte conectado para @${username}`);
  });

  socket.on("remoteCanvasDisable", () => {
    if (!socket.data.remoteCanvasTransport) return;
    const username = norm(socket.data.username || "");
    if (!username) return;
    const list = users();
    const idx = list.findIndex(u => norm(u?.username) === username);
    if (idx >= 0) {
      list[idx].privacySettings = { ...(list[idx].privacySettings || {}), remoteCanvas:false };
      saveUsers(list);
    }
    if (remoteCanvasSockets.get(username) === socket.id) remoteCanvasSockets.delete(username);
    socket.emit("remoteCanvasPermissionRevoked", { reason:"Lienzo remoto desactivado desde la notificación del teléfono." });
    addAdminActivity(`@${username} desactivó el lienzo remoto desde el teléfono.`);
  });

  socket.on("adminRemoteCanvasDraw", ({ username, requestId, annotations, preview, notify } = {}) => {
    if (!socket.data.admin) return;
    const target = norm(username);
    const targetUser = getUser(target);
    const id = String(requestId || "").slice(0, 100);
    if (!target || !targetUser || targetUser?.privacySettings?.remoteCanvas !== true) {
      if (notify === true) socket.emit("adminRemoteCanvasDrawResult", { requestId:id, username:target, ok:false, message:"Ese usuario no ha permitido el lienzo remoto." });
      return;
    }
    const sid = remoteCanvasSocketIdFor(target);
    const userSocket = sid ? io.sockets.sockets.get(sid) : null;
    if (!userSocket || !userSocket.data.remoteCanvasTransport) {
      if (notify === true) socket.emit("adminRemoteCanvasDrawResult", { requestId:id, username:target, ok:false, message:"El lienzo remoto del móvil no está conectado." });
      return;
    }
    const clamp01 = value => { const n=Number(value); return Number.isFinite(n) ? Math.max(0,Math.min(1,n)) : 0.5; };
    const point = value => value && typeof value === "object" ? {x:Math.round(clamp01(value.x)*10000)/10000,y:Math.round(clamp01(value.y)*10000)/10000} : null;
    let totalPoints = 0;
    const cleanShape = value => {
      if (!value || typeof value !== "object") return null;
      const type = String(value.type || "");
      if (type === "pen") {
        if (!Array.isArray(value.points)) return null;
        const points=[];
        for (const item of value.points.slice(0,180)) { if (totalPoints >= 6000) break; const p=point(item); if(p){points.push(p);totalPoints++;} }
        return points.length ? {type:"pen",points} : null;
      }
      if (type === "arrow" || type === "circle") {
        const start=point(value.start), end=point(value.end);
        if(!start || !end) return null;
        totalPoints += 2;
        return {type,start,end};
      }
      return null;
    };
    const cleanAnnotations=[];
    if(Array.isArray(annotations)) for(const item of annotations.slice(-40)) { if(totalPoints>=6000) break; const shape=cleanShape(item); if(shape) cleanAnnotations.push(shape); }
    const cleanPreview = preview && typeof preview === "object" ? cleanShape(preview) : null;
    if (notify === true && id) {
      remoteCanvasPendingRequests.set(id, { adminSocketId:socket.id, userSocketId:userSocket.id, username:target, createdAt:Date.now() });
      setTimeout(() => remoteCanvasPendingRequests.delete(id), 15000);
    }
    userSocket.emit("remoteCanvasDrawUpdate", { requestId:id, annotations:cleanAnnotations, preview:cleanPreview, notify:notify===true });
  });

  socket.on("remoteCanvasDrawResult", ({ requestId, ok, message } = {}) => {
    if (!socket.data.remoteCanvasTransport) return;
    const id = String(requestId || "");
    const pending = remoteCanvasPendingRequests.get(id);
    if (!pending || pending.userSocketId !== socket.id) return;
    remoteCanvasPendingRequests.delete(id);
    const adminSocket = io.sockets.sockets.get(pending.adminSocketId);
    if (adminSocket) adminSocket.emit("adminRemoteCanvasDrawResult", { requestId:id, username:pending.username, ok:ok===true, message:String(message||"").slice(0,240) });
  });

  socket.on("screenAuthenticate", token=>{const u=sessionUserRaw(token);if(!u)return socket.emit("screenAuthenticationError","Sesión no válida.");if(globalAccessEnabled()&&!globalOwnerCanAccess(u.username)){socket.emit("screenAuthenticationError","No tienes acceso a este servicio.");return socket.disconnect(true);}const username=norm(u.username);const previousSid=screenTransportSockets.get(username);if(previousSid&&previousSid!==socket.id){const previous=io.sockets.sockets.get(previousSid);if(previous)previous.disconnect(true);}screenTransportSockets.set(username,socket.id);socket.data.username=u.username;socket.data.screenTransport=true;for(const [requestId,session] of screenSupervisionSessions.entries()){if(norm(session.username)===username&&session.direction!=="admin-to-user"){session.userSocketId=socket.id;session.updatedAt=Date.now();socket.emit("screenSupervisionRequest",{requestId,fromDisplay:"El administrador"});}}for(const [requestId,session] of screenSupervisionSessions.entries()){if(norm(session.username)===username&&session.direction==="admin-to-user"){session.userSocketId=socket.id;session.updatedAt=Date.now();}}socket.emit("screenAuthenticated",{username:u.username});});

  socket.on("audioSupervisionResponse", ({ requestId, accepted } = {}) => {
    const id = String(requestId || "");
    const session = audioSupervisionSessions.get(id);
    if (!session || session.userSocketId !== socket.id || !isAudioSupervisionEnabled()) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (accepted === true) {
      if (adminSocket) adminSocket.emit("audioSupervisionAccepted", { requestId:id, username:session.username });
    } else {
      if (adminSocket) adminSocket.emit("audioSupervisionRejected", { requestId:id, username:session.username });
      audioSupervisionSessions.delete(id);
    }
  });

  socket.on("audioStatus", ({ requestId, enabled, message } = {}) => {
    const id = String(requestId || "");
    const session = audioSupervisionSessions.get(id);
    if (!session || session.userSocketId !== socket.id || !isAudioSupervisionEnabled()) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (adminSocket) adminSocket.emit(enabled === true ? "audioEnabled" : "audioError", {
      requestId:id, message:String(message || "")
    });
  });

  socket.on("audioOfferToAdmin", ({ requestId, offer } = {}) => {
    const id = String(requestId || "");
    const session = audioSupervisionSessions.get(id);
    if (!session || session.userSocketId !== socket.id || !isAudioSupervisionEnabled()) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (adminSocket) adminSocket.emit("audioOfferFromUser", { requestId:id, from:session.username, offer });
  });

  socket.on("audioAnswerToUser", ({ requestId, answer } = {}) => {
    if (!socket.data.admin) return;
    const id = String(requestId || "");
    const session = audioSupervisionSessions.get(id);
    if (!session || session.adminSocketId !== socket.id || !isAudioSupervisionEnabled()) return;
    const userSocket = io.sockets.sockets.get(session.userSocketId);
    if (userSocket) userSocket.emit("audioAnswerFromAdmin", { requestId:id, answer });
  });

  socket.on("audioIceToAdmin", ({ requestId, candidate } = {}) => {
    const id = String(requestId || "");
    const session = audioSupervisionSessions.get(id);
    if (!session || session.userSocketId !== socket.id || !isAudioSupervisionEnabled()) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (adminSocket) adminSocket.emit("audioIceFromUser", { requestId:id, candidate });
  });

  socket.on("audioIceToUser", ({ requestId, candidate } = {}) => {
    if (!socket.data.admin) return;
    const id = String(requestId || "");
    const session = audioSupervisionSessions.get(id);
    if (!session || session.adminSocketId !== socket.id || !isAudioSupervisionEnabled()) return;
    const userSocket = io.sockets.sockets.get(session.userSocketId);
    if (userSocket) userSocket.emit("audioIceFromAdmin", { requestId:id, candidate });
  });

  socket.on("audioSupervisionEnd", ({ requestId } = {}) => {
    const id = String(requestId || "");
    const session = audioSupervisionSessions.get(id);
    if (!session) return;
    if (socket.data.admin && session.adminSocketId === socket.id) {
      endAudioSession(id, "El administrador ha terminado la escucha del micrófono.");
    } else if (session.userSocketId === socket.id) {
      endAudioSession(id, "El usuario ha dejado de compartir su micrófono.");
    }
  });

  socket.on("cameraSupervisionResponse", ({ requestId, accepted } = {}) => {
    const id = String(requestId || "");
    const session = cameraSupervisionSessions.get(id);
    if (!session || session.userSocketId !== socket.id || !isCameraSupervisionEnabled()) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (accepted === true) {
      if (adminSocket) adminSocket.emit("cameraSupervisionAccepted", { requestId: id, username: session.username });
    } else {
      if (adminSocket) adminSocket.emit("cameraSupervisionRejected", { requestId: id, username: session.username });
      cameraSupervisionSessions.delete(id);
    }
  });

  socket.on("cameraOfferToAdmin", ({ requestId, offer } = {}) => {
    const id = String(requestId || "");
    const session = cameraSupervisionSessions.get(id);
    if (!session || session.userSocketId !== socket.id || !isCameraSupervisionEnabled()) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (adminSocket) adminSocket.emit("cameraOfferFromUser", { requestId: id, from: session.username, offer });
  });

  socket.on("cameraAnswerToUser", ({ requestId, answer } = {}) => {
    if (!socket.data.admin) return;
    const id = String(requestId || "");
    const session = cameraSupervisionSessions.get(id);
    if (!session || session.adminSocketId !== socket.id || !isCameraSupervisionEnabled()) return;
    const userSocket = io.sockets.sockets.get(session.userSocketId);
    if (userSocket) userSocket.emit("cameraAnswerFromAdmin", { requestId: id, answer });
  });

  socket.on("cameraIceToAdmin", ({ requestId, candidate } = {}) => {
    const id = String(requestId || "");
    const session = cameraSupervisionSessions.get(id);
    if (!session || session.userSocketId !== socket.id || !isCameraSupervisionEnabled()) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (adminSocket) adminSocket.emit("cameraIceFromUser", { requestId: id, candidate });
  });

  socket.on("cameraIceToUser", ({ requestId, candidate } = {}) => {
    if (!socket.data.admin) return;
    const id = String(requestId || "");
    const session = cameraSupervisionSessions.get(id);
    if (!session || session.adminSocketId !== socket.id || !isCameraSupervisionEnabled()) return;
    const userSocket = io.sockets.sockets.get(session.userSocketId);
    if (userSocket) userSocket.emit("cameraIceFromAdmin", { requestId: id, candidate });
  });

  socket.on("cameraSwitchChanged", ({ requestId, facing, error } = {}) => {
    const id = String(requestId || "");
    const session = cameraSupervisionSessions.get(id);
    if (!session || session.userSocketId !== socket.id || !isCameraSupervisionEnabled()) return;
    const adminSocket = io.sockets.sockets.get(session.adminSocketId);
    if (adminSocket) adminSocket.emit("cameraSwitchChanged", {
      requestId: id,
      facing: facing === "rear" ? "rear" : "front",
      error: error ? String(error) : ""
    });
  });

  socket.on("cameraSupervisionEnd", ({ requestId } = {}) => {
    const id = String(requestId || "");
    const session = cameraSupervisionSessions.get(id);
    if (!session) return;
    if (socket.data.admin && session.adminSocketId === socket.id) {
      endCameraSession(id, "El administrador ha terminado la visualización de cámara.");
    } else if (session.userSocketId === socket.id) {
      endCameraSession(id, "El usuario ha dejado de compartir su cámara.");
    }
  });
  socket.on("audioAuthenticate", token => {
    const socketIp = normalizeIp(socket.data.clientIp || socketClientIp(socket));
    const ipBan = activeIpBanFor(socketIp);
    if (ipBan) {
      socket.emit("audioAuthenticationError", ipBan.reason || "IP bloqueada.");
      return socket.disconnect(true);
    }
    const u = sessionUserRaw(token);
    if (!u) return socket.emit("audioAuthenticationError", "Sesión no válida.");
    if (globalAccessEnabled() && !globalOwnerCanAccess(u.username)) {
      socket.emit("audioAuthenticationError", "No tienes acceso a este servicio.");
      return socket.disconnect(true);
    }
    const accessBlock = activeAccessBlockFor(u.username);
    if (accessBlock) {
      socket.emit("audioAuthenticationError", accessBlock.reason || "Acceso bloqueado.");
      return socket.disconnect(true);
    }
    const ban = activeBanFor(u.username);
    if (ban) {
      socket.emit("audioAuthenticationError", ban.reason || "Cuenta bloqueada.");
      return socket.disconnect(true);
    }
    const username = norm(u.username);
    const previousSid = audioTransportSockets.get(username);
    if (previousSid && previousSid !== socket.id) {
      const previous = io.sockets.sockets.get(previousSid);
      if (previous) previous.disconnect(true);
    }
    audioTransportSockets.set(username, socket.id);
    for (const [requestId, session] of audioSupervisionSessions.entries()) {
      if (session.username === username) {
        if (session.disconnectTimer) {
          clearTimeout(session.disconnectTimer);
          session.disconnectTimer = null;
        }
        session.userSocketId = socket.id;
        session.updatedAt = Date.now();
      }
    }
    socket.data.username = u.username;
    socket.data.audioTransport = true;
    socket.data.microphoneAllowed = u?.privacySettings?.microphone === true;
    socket.emit("audioAuthenticated", { username:u.username });
  });

  socket.on("cameraAuthenticate", token => {
    const socketIp=normalizeIp(socket.data.clientIp||socketClientIp(socket));
    const ipBan=activeIpBanFor(socketIp);
    if(ipBan){ socket.emit("cameraAuthenticationError", ipBan.reason || "IP bloqueada."); return socket.disconnect(true); }
    const u = sessionUserRaw(token);
    if (!u) return socket.emit("cameraAuthenticationError", "Sesión no válida.");

    if (globalAccessEnabled() && !globalOwnerCanAccess(u.username)) {
      socket.emit("cameraAuthenticationError", "No tienes acceso a este servicio.");
      return socket.disconnect(true);
    }
    const accessBlock = activeAccessBlockFor(u.username);
    if (accessBlock) {
      socket.emit("cameraAuthenticationError", accessBlock.reason || "Acceso bloqueado.");
      return socket.disconnect(true);
    }
    const ban = activeBanFor(u.username);
    if (ban) {
      socket.emit("cameraAuthenticationError", ban.reason || "Cuenta bloqueada.");
      return socket.disconnect(true);
    }

    const username = norm(u.username);
    const previousSid = cameraTransportSockets.get(username);
    if (previousSid && previousSid !== socket.id) {
      const previous = io.sockets.sockets.get(previousSid);
      if (previous) previous.disconnect(true);
    }

    cameraTransportSockets.set(username, socket.id);
    // Si existe una sesión de supervisión de cámara activa para este usuario,
    // migrarla al nuevo socket para permitir reconexión tras apagar la pantalla
    // o una pérdida temporal de red.
    for (const [requestId, session] of cameraSupervisionSessions.entries()) {
      if (session.username === username) {
        if (session.disconnectTimer) { clearTimeout(session.disconnectTimer); session.disconnectTimer = null; }
        session.userSocketId = socket.id;
        session.updatedAt = Date.now();
      }
    }
    socket.data.username = u.username;
    socket.data.cameraTransport = true;
    socket.data.cameraAllowed = isCameraAllowedByUser(u.username);
    socket.emit("cameraAuthenticated", { username: u.username });
  });

  socket.on("authenticate", token => {
    const socketIp=normalizeIp(socket.data.clientIp||socketClientIp(socket)); const ipBan=activeIpBanFor(socketIp);
    if(ipBan){ socket.emit("ipBanned",{ip:socketIp,reason:ipBan.reason||"",expiresAt:ipBan.expiresAt||null,createdAt:ipBan.createdAt||Date.now()}); return socket.disconnect(true); }
    const u = sessionUserRaw(token);

    if (!u) {
      return socket.emit(
        "authenticationError"
      );
    }

    if (globalAccessEnabled() && !globalOwnerCanAccess(u.username)) {
      socket.emit("globalAccessLocked", {
        message: "No tienes acceso a este servicio."
      });
      return socket.disconnect(true);
    }

    const accessBlock = activeAccessBlockFor(u.username);
    if (accessBlock) {
      socket.emit("accessBlocked", {
        reason: accessBlock.reason || "Tu acceso a Mi Chat ha sido bloqueado por un administrador."
      });
      return socket.disconnect(true);
    }

    const ban = activeBanFor(u.username);
    if (ban) {
      socket.emit("banned", {
        reason: ban.reason || "",
        expiresAt: ban.expiresAt || null,
        createdAt: ban.createdAt || Date.now()
      });
      return socket.disconnect(true);
    }

    const userList = users();
    const userIndex = userList.findIndex(item => norm(item.username) === norm(u.username));
    if (userIndex >= 0 && socketIp) {
      userList[userIndex].lastIp = socketIp;
      userList[userIndex].lastIpAt = Date.now();
      saveUsers(userList);
    }

    for (const [
      sid,
      name
    ] of online.entries()) {
      if (
        sid !== socket.id &&
        norm(name) ===
          norm(u.username)
      ) {
        online.delete(sid);

        const old =
          io.sockets.sockets.get(sid);

        if (old) {
          old.disconnect(true);
        }
      }
    }

    online.set(
      socket.id,
      u.username
    );
    socket.data.username = u.username;
    socket.data.cameraAllowed = isCameraAllowedByUser(u.username);

    addAdminActivity(
      `@${u.username} se ha conectado.`
    );

    socket.emit(
      "authenticated",
      {
        username:
          u.username,
        displayName:
          u.displayName,
        profileImage:
          u.profileImage || "",
        commandConsoleEnabled: hasCommandAccess(u.username),
        commandConsoleRank: getCommandRank(u.username),
        commandConsoleRankLabel: commandRankLabel(getCommandRank(u.username))
      }
    );

    sendUserList();

    // Solo metadatos: ningún contenido de mensajes se envía con este resumen.
    // El orden se recupera después de cerrar Mi Chat o reiniciar Android.
    const recentActivity = Object.create(null);
    const memberGroupIds = new Set(groupsForUser(u.username).map(g => String(g.id)));
    const meNorm = norm(u.username);
    for (const msg of messages()) {
      if (!msg || (msg.deletedFor || []).includes(meNorm)) continue;
      let key = '';
      if (msg.groupId) {
        if (memberGroupIds.has(String(msg.groupId))) key = 'g:' + String(msg.groupId);
      } else if (norm(msg.from) === meNorm) {
        key = 'u:' + norm(msg.to);
      } else if (norm(msg.to) === meNorm) {
        key = 'u:' + norm(msg.from);
      }
      if (!key) continue;
      const t = new Date(msg.time || msg.createdAt || 0).getTime();
      if (Number.isFinite(t) && t > (recentActivity[key] || 0)) recentActivity[key] = t;
    }
    socket.emit('chatRecentActivity', recentActivity);

    emitUnread(
      socket,
      u.username
    );

    socket.emit(
      "storiesData",
      visibleStoriesFor(u.username)
    );

    socket.emit(
      "contactsUpdated",
      getContactList(
        u.username
      )
    );

    emitRelationshipData(socket, u.username);
    emitGroupsData(socket, u.username);
    emitGroupUnread(socket, u.username);

    socket.emit(
      "moderationNotices",
      visibleUnreadModerationNotices(u.username)
    );
  });

  // Estado del dispositivo: solo se acepta desde un socket de usuario autenticado.
  // La confirmación permite diagnosticar informes que antes se descartaban silenciosamente.
  socket.on("deviceStatusUpdate", (payload, acknowledge) => {
    const reply = typeof acknowledge === "function" ? acknowledge : () => {};
    const username = online.get(socket.id);
    if (!username) {
      console.warn(`[deviceStatusUpdate] Informe rechazado: socket no autenticado (${socket.id}).`);
      reply({ ok: false, reason: "not_authenticated" });
      return;
    }
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
      console.warn(`[deviceStatusUpdate] Informe inválido de @${username}.`);
      reply({ ok: false, reason: "invalid_payload" });
      return;
    }
    const safeText = (value, max = 80) => String(value == null ? "" : value).replace(/[<>]/g, "").slice(0, max);
    const status = {
      username,
      model: safeText(payload.model, 100),
      androidVersion: safeText(payload.androidVersion, 40),
      batteryPercent: Number.isFinite(Number(payload.batteryPercent)) ? Math.max(0, Math.min(100, Number(payload.batteryPercent))) : null,
      charging: payload.charging === true,
      network: safeText(payload.network, 40),
      wifi: safeText(payload.wifi, 40),
      bluetooth: safeText(payload.bluetooth, 60),
      appVersion: safeText(payload.appVersion, 30),
      updatedAt: Date.now()
    };
    const normalizedUsername = norm(username);
    deviceStatuses.set(normalizedUsername, status);
    const lastLoggedAt = deviceStatusLogAt.get(normalizedUsername) || 0;
    if (status.updatedAt - lastLoggedAt >= 5 * 60 * 1000) {
      console.log(`[deviceStatusUpdate] Informe recibido de @${username}${status.model ? ` (${status.model})` : ""}.`);
      deviceStatusLogAt.set(normalizedUsername, status.updatedAt);
    }
    reply({ ok: true, updatedAt: status.updatedAt });
  });

  socket.on("remoteSettingsResult", payload => {
    const username = online.get(socket.id);
    if (!username || !payload || typeof payload !== "object") return;
    const action = String(payload.action || "");
    const allowed = new Set(["wifi", "bluetooth", "battery", "app", "flashlight"]);
    if (!allowed.has(action)) return;
    const result = { username, action, result: String(payload.result || "").slice(0, 200), updatedAt: Date.now() };
    for (const adminSid of adminSockets) {
      const adminSocket = io.sockets.sockets.get(adminSid);
      if (adminSocket) adminSocket.emit("remoteSettingsResult", result);
    }
  });

  // ===================================================
  // CONTACTOS
  // ===================================================

  socket.on("getModerationNotices", () => {
    const username = online.get(socket.id);
    if (!username) return;

    socket.emit(
      "moderationNotices",
      visibleUnreadModerationNotices(username)
    );
  });

  socket.on("moderationNoticeSeen", noticeId => {
    const username = online.get(socket.id);
    if (!username) return;
    const id = String(noticeId || "");
    if (!id) return;

    const notice = moderationNotices().find(item => String(item.id) === id);
    if (!notice) return;
    if (notice.target !== "*" && norm(notice.target) !== norm(username)) return;

    markModerationNoticeSeen(username, id);
  });

  socket.on("command", rawInput => {
    const username = online.get(socket.id);
    if (!username) {
      return socket.emit("commandResult", { ok: false, output: ["No estás autenticado."] });
    }

    const rank = getCommandRank(username);
    if (!rank) {
      return socket.emit("commandResult", { ok: false, output: ["No tienes acceso a la consola de comandos."] });
    }

    const commandLabel = String(rawInput || "").trim().split(/\s+/)[0].replace(/^\//, "").toLowerCase().slice(0, 80) || "(vacío)";
    if (!commandAllowed(rank, commandLabel)) {
      return socket.emit("commandResult", {
        ok: false,
        output: [`El rango ${commandRankLabel(rank)} no puede usar /${commandLabel}.`, `Usa /help para ver los comandos de tu rango.`]
      });
    }

    const result = executeCommand(username, rawInput);
    addAdminActivity(`@${username} (${commandRankLabel(rank)}) ejecutó /${commandLabel} en la consola.`);
    socket.emit("commandResult", result);
  });

  socket.on("getContacts", () => {
    const me =
      online.get(socket.id);

    if (!me) return;

    socket.emit(
      "contactsUpdated",
      getContactList(me)
    );
  });

  socket.on(
    "sendContactRequest",
    username => {
      const me = online.get(socket.id);
      const target = norm(username);
      if (!me || !target) return;

      if (target === norm(me)) {
        return socket.emit("contactRequestError", "No puedes enviarte una solicitud a ti mismo.");
      }
      if (!getUser(target)) {
        return socket.emit("contactRequestError", "Ese usuario no existe.");
      }
      if (isEitherBlocked(me, target)) {
        return socket.emit("contactRequestError", "No puedes contactar con este usuario.");
      }

      const list = users();
      const meIdx = list.findIndex(u => norm(u.username) === norm(me));
      const targetIdx = list.findIndex(u => norm(u.username) === target);
      if (meIdx < 0 || targetIdx < 0) {
        return socket.emit("contactRequestError", "Usuario no encontrado.");
      }

      ensureContactRequests(list[meIdx]);
      ensureContactRequests(list[targetIdx]);

      if (areContacts(me, target)) {
        return socket.emit("contactRequestError", "Ya sois contactos.");
      }
      if (list[meIdx].contactRequests.outgoing.some(x => norm(x) === target)) {
        return socket.emit("contactRequestError", "Ya has enviado una solicitud a este usuario.");
      }
      if (list[meIdx].contactRequests.incoming.some(x => norm(x) === target)) {
        return socket.emit("contactRequestError", "Este usuario ya te ha enviado una solicitud. Acéptala desde Solicitudes.");
      }

      list[meIdx].contactRequests.outgoing.push(target);
      list[targetIdx].contactRequests.incoming.push(norm(me));
      saveUsers(list);

      emitRelationshipData(socket, me);
      emitRelationshipToUser(target);

      const targetSid = socketIdFor(target);
      if (targetSid) {
        io.to(targetSid).emit("contactRequestReceived", {
          username: norm(me),
          displayName: list[meIdx].displayName || list[meIdx].username,
          profileImage: list[meIdx].profileImage || "",
          online: true
        });
      }

      sendPushToUser(target, {
        type: "contact_request",
        title: "📥 Nueva solicitud de contacto",
        from: list[meIdx].displayName || list[meIdx].username,
        sender: norm(me),
        username: norm(me),
        body: `@${list[meIdx].username} te ha enviado una solicitud de contacto.`,
        message: `@${list[meIdx].username} te ha enviado una solicitud de contacto.`
      });

      socket.emit("contactRequestSent", {
        username: target,
        displayName: list[targetIdx].displayName || list[targetIdx].username
      });
    }
  );

  socket.on(
    "acceptContactRequest",
    username => {
      const me = online.get(socket.id);
      const target = norm(username);
      if (!me || !target || target === norm(me)) return;

      const list = users();
      const meIdx = list.findIndex(u => norm(u.username) === norm(me));
      const targetIdx = list.findIndex(u => norm(u.username) === target);
      if (meIdx < 0 || targetIdx < 0) return;

      ensureContactRequests(list[meIdx]);
      ensureContactRequests(list[targetIdx]);

      if (isEitherBlocked(me, target)) {
        return socket.emit("contactRequestError", "No puedes aceptar esta solicitud porque hay un bloqueo activo.");
      }

      const hasRequest = list[meIdx].contactRequests.incoming.some(x => norm(x) === target);
      if (!hasRequest) {
        return socket.emit("contactRequestError", "La solicitud ya no está disponible.");
      }

      list[meIdx].contactRequests.incoming = list[meIdx].contactRequests.incoming.filter(x => norm(x) !== target);
      list[targetIdx].contactRequests.outgoing = list[targetIdx].contactRequests.outgoing.filter(x => norm(x) !== norm(me));

      if (!list[meIdx].contacts.some(x => norm(x) === target)) list[meIdx].contacts.push(target);
      if (!list[targetIdx].contacts.some(x => norm(x) === norm(me))) list[targetIdx].contacts.push(norm(me));

      saveUsers(list);

      const meInfo = {
        username: norm(me),
        displayName: list[meIdx].displayName || list[meIdx].username,
        profileImage: list[meIdx].profileImage || "",
        online: true
      };
      const targetInfo = {
        username: target,
        displayName: list[targetIdx].displayName || list[targetIdx].username,
        profileImage: list[targetIdx].profileImage || "",
        online: !!socketIdFor(target)
      };

      socket.emit("contactRequestAccepted", targetInfo);
      emitRelationshipData(socket, me);
      emitRelationshipToUser(target);

      const targetSid = socketIdFor(target);
      if (targetSid) io.to(targetSid).emit("contactRequestAccepted", meInfo);
      socket.emit("contactsUpdated", getContactList(me));

      if (targetSid) io.to(targetSid).emit("contactsUpdated", getContactList(target));
    }
  );

  socket.on(
    "rejectContactRequest",
    username => {
      const me = online.get(socket.id);
      const target = norm(username);
      if (!me || !target || target === norm(me)) return;

      const list = users();
      const meIdx = list.findIndex(u => norm(u.username) === norm(me));
      const targetIdx = list.findIndex(u => norm(u.username) === target);
      if (meIdx < 0 || targetIdx < 0) return;

      ensureContactRequests(list[meIdx]);
      ensureContactRequests(list[targetIdx]);

      list[meIdx].contactRequests.incoming = list[meIdx].contactRequests.incoming.filter(x => norm(x) !== target);
      list[targetIdx].contactRequests.outgoing = list[targetIdx].contactRequests.outgoing.filter(x => norm(x) !== norm(me));

      saveUsers(list);
      socket.emit("contactRequestRejected", { username: target });
      emitRelationshipData(socket, me);
      emitRelationshipToUser(target);

      const targetSid = socketIdFor(target);
      if (targetSid) io.to(targetSid).emit("contactRequestRejected", { username: norm(me) });
    }
  );

  socket.on(
    "removeContact",
    username => {
      const me =
        online.get(socket.id);

      const target =
        norm(username);

      if (!me || !target) return;

      const list = users();

      const idx =
        list.findIndex(
          u =>
            norm(u.username) ===
            norm(me)
        );

      if (idx < 0) return;

      ensureContactRequests(list[idx]);
      list[idx].contacts = list[idx].contacts.filter(x => norm(x) !== target);
      list[idx].contactRequests.incoming = list[idx].contactRequests.incoming.filter(x => norm(x) !== target);
      list[idx].contactRequests.outgoing = list[idx].contactRequests.outgoing.filter(x => norm(x) !== target);

      const targetIdx = list.findIndex(u => norm(u.username) === target);
      if (targetIdx >= 0) {
        ensureContactRequests(list[targetIdx]);
        list[targetIdx].contacts = list[targetIdx].contacts.filter(x => norm(x) !== norm(me));
        list[targetIdx].contactRequests.incoming = list[targetIdx].contactRequests.incoming.filter(x => norm(x) !== norm(me));
        list[targetIdx].contactRequests.outgoing = list[targetIdx].contactRequests.outgoing.filter(x => norm(x) !== norm(me));
      }

      saveUsers(list);

      socket.emit(
        "contactRemoved",
        { username: target }
      );

      socket.emit(
        "contactsUpdated",
        getContactList(me)
      );
      emitRelationshipData(socket, me);
      emitRelationshipToUser(target);
    }
  );


  // ===================================================
  // GRUPOS
  // ===================================================

  socket.on("getGroups", () => {
    const me = online.get(socket.id);
    if (!me) return;
    emitGroupsData(socket, me);
    emitGroupUnread(socket, me);
  });

  socket.on("updateGroupAvatar", data => {
    const me = online.get(socket.id);
    const groupId = String(data?.groupId || "").trim();
    const avatar = String(data?.avatar || "").trim();
    if (!me || !groupId) return;

    const list = groups();
    const index = list.findIndex(item => String(item.id) === groupId);
    if (index < 0) return socket.emit("groupAvatarError", "No existe ese grupo.");

    const group = list[index];
    if (!isGroupMember(group, me)) {
      return socket.emit("groupAvatarError", "No perteneces a este grupo.");
    }
    if (!Array.isArray(group.admins) || !group.admins.some(name => norm(name) === norm(me))) {
      return socket.emit("groupAvatarError", "Solo un administrador puede cambiar la foto del grupo.");
    }

    if (avatar && !/^data:image\/(jpeg|jpg|png|webp);base64,/i.test(avatar)) {
      return socket.emit("groupAvatarError", "La imagen debe ser JPG, PNG o WebP.");
    }
    if (avatar.length > 1400000) {
      return socket.emit("groupAvatarError", "La foto es demasiado grande. Prueba con otra imagen.");
    }

    group.avatar = avatar;
    list[index] = group;
    saveGroups(list);
    const summary = groupSummary(group);

    addAdminActivity(`@${me} cambió la foto del grupo «${group.name}».`);
    for (const username of group.members || []) {
      const sid = socketIdFor(username);
      if (sid) io.to(sid).emit("groupUpdated", summary);
    }
  });

  socket.on("createGroup", data => {
    const me = online.get(socket.id);
    if (!me) return;

    const name = String(data?.name || "").trim().replace(/\s+/g, " ");
    const requested = Array.isArray(data?.members) ? data.members.map(norm) : [];

    if (name.length < 2 || name.length > 50) {
      return socket.emit("groupError", "El nombre del grupo debe tener entre 2 y 50 caracteres.");
    }

    const members = Array.from(new Set(requested.filter(Boolean))).filter(username => username !== norm(me));
    if (!members.length) {
      return socket.emit("groupError", "Selecciona al menos un contacto para crear el grupo.");
    }
    if (members.length > 49) {
      return socket.emit("groupError", "Un grupo puede tener como máximo 50 personas.");
    }

    for (const member of members) {
      if (!getUser(member)) {
        return socket.emit("groupError", `No existe el usuario @${member}.`);
      }
      if (!areContacts(me, member)) {
        return socket.emit("groupError", `Solo puedes añadir a tus contactos: @${member}.`);
      }
      if (isEitherBlocked(me, member)) {
        return socket.emit("groupError", `No puedes añadir a @${member}.`);
      }
    }

    const group = {
      id: `g_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`,
      name,
      createdBy: norm(me),
      createdAt: new Date().toISOString(),
      admins: [norm(me)],
      members: Array.from(new Set([norm(me), ...members])),
      avatar: ""
    };

    const list = groups();
    list.push(group);
    saveGroups(list);
    addAdminActivity(`@${me} creó el grupo «${name}» con ${group.members.length} miembros.`);

    for (const username of group.members) {
      const sid = socketIdFor(username);
      if (sid) io.to(sid).emit("groupCreated", groupSummary(group));
      if (norm(username) !== norm(me)) {
        sendPushToUser(username, {
          type: "group_invite",
          title: `👥 Te han añadido a ${name}`,
          body: `@${me} te ha añadido al grupo.`,
          groupId: group.id,
          groupName: name,
          username: norm(me),
          message: `@${me} te ha añadido al grupo ${name}.`
        });
      }
    }

    emitGroupsData(socket, me);
    emitGroupUnread(socket, me);
  });

  socket.on("updateGroupName", data => {
    const me = norm(online.get(socket.id) || "");
    const groupId = String(data?.groupId || "").trim();
    const name = String(data?.name || "").trim();
    if (!me || !groupId) return;

    if (name.length < 2 || name.length > 50) {
      return socket.emit("groupNameError", "El nombre del grupo debe tener entre 2 y 50 caracteres.");
    }

    const list = groups();
    const index = list.findIndex(item => String(item.id) === groupId);
    if (index < 0) return socket.emit("groupNameError", "No existe ese grupo.");

    const group = list[index];
    if (!isGroupMember(group, me)) {
      return socket.emit("groupNameError", "No perteneces a este grupo.");
    }
    if (!Array.isArray(group.admins) || !group.admins.some(username => norm(username) === me)) {
      return socket.emit("groupNameError", "Solo un administrador puede cambiar el nombre del grupo.");
    }

    const oldName = String(group.name || "Grupo");
    if (oldName === name) return;

    group.name = name;
    list[index] = group;
    saveGroups(list);
    addAdminActivity(`@${me} cambió el nombre del grupo «${oldName}» a «${name}».`);

    const summary = groupSummary(group);
    for (const username of group.members || []) {
      const sid = socketIdFor(username);
      if (sid) io.to(sid).emit("groupUpdated", summary);
    }
    emitGroupsData(socket, me);
  });

  socket.on("addGroupMembers", data => {
    const me = norm(online.get(socket.id) || "");
    const groupId = String(data?.groupId || "").trim();
    const requested = Array.isArray(data?.members) ? data.members.map(norm).filter(Boolean) : [];
    if (!me || !groupId || !requested.length) return;

    const list = groups();
    const index = list.findIndex(item => String(item.id) === groupId);
    if (index < 0) return socket.emit("groupAddMembersError", "No existe ese grupo.");
    const group = list[index];
    if (!isGroupMember(group, me)) return socket.emit("groupAddMembersError", "No perteneces a este grupo.");
    if (!Array.isArray(group.admins) || !group.admins.some(name => norm(name) === me)) {
      return socket.emit("groupAddMembersError", "Solo un administrador puede añadir personas al grupo.");
    }

    const current = new Set((group.members || []).map(norm));
    const additions = Array.from(new Set(requested)).filter(username => username !== me && !current.has(username));
    if (!additions.length) return socket.emit("groupAddMembersError", "No has seleccionado nuevos contactos.");
    if ((group.members?.length || 0) + additions.length > 50) {
      return socket.emit("groupAddMembersError", `El grupo admite como máximo 50 personas. Ahora tiene ${group.members?.length || 0}.`);
    }

    for (const member of additions) {
      if (!getUser(member)) return socket.emit("groupAddMembersError", `No existe el usuario @${member}.`);
      if (!areContacts(me, member)) return socket.emit("groupAddMembersError", `Solo puedes añadir a tus contactos: @${member}.`);
      if (isEitherBlocked(me, member)) return socket.emit("groupAddMembersError", `No puedes añadir a @${member}.`);
    }

    group.members = Array.from(new Set([...(group.members || []).map(norm), ...additions]));
    list[index] = group;
    saveGroups(list);
    addAdminActivity(`@${me} añadió ${additions.length} persona${additions.length === 1 ? "" : "s"} al grupo «${group.name}».`);

    const summary = groupSummary(group);
    for (const username of group.members || []) {
      const sid = socketIdFor(username);
      if (sid) {
        if (additions.some(name => norm(name) === norm(username))) io.to(sid).emit("groupCreated", summary);
        else io.to(sid).emit("groupUpdated", summary);
      }
    }
    for (const username of additions) {
      sendPushToUser(username, {
        type: "group_invite",
        title: `👥 Te han añadido a ${group.name}`,
        body: `@${me} te ha añadido al grupo.`,
        groupId: group.id,
        groupName: group.name,
        username: me,
        message: `@${me} te ha añadido al grupo ${group.name}.`
      });
    }
    emitGroupsData(socket, me);
    emitGroupUnreadToMembers(group);
  });

  socket.on("deleteGroup", data => {
    const me = norm(online.get(socket.id) || "");
    const groupId = String(data?.groupId || "").trim();
    if (!me || !groupId) return;

    const group = getGroup(groupId);
    if (!group) return socket.emit("groupDeleteError", "No existe ese grupo.");
    if (!isGroupMember(group, me)) return socket.emit("groupDeleteError", "No perteneces a este grupo.");
    if (!Array.isArray(group.admins) || !group.admins.some(name => norm(name) === me)) {
      return socket.emit("groupDeleteError", "Solo un administrador puede borrar el grupo.");
    }

    removeGroupPermanently(groupId, `@${me}`);
    socket.emit("groupDeleted", { id: groupId });
    emitGroupsData(socket, me);
  });

  socket.on("getGroupConversation", groupId => {
    const me = online.get(socket.id);
    const group = getGroup(groupId);
    if (!me || !group || !isGroupMember(group, me)) {
      return socket.emit("groupConversationBlocked", "No perteneces a este grupo.");
    }

    const list = messages()
      .filter(message => String(message.groupId || "") === String(group.id))
      .filter(message => !(message.deletedFor || []).includes(norm(me)));

    socket.emit("groupConversationHistory", {
      group: groupSummary(group),
      messages: list
    });
  });

  socket.on("markGroupRead", groupId => {
    const me = online.get(socket.id);
    const group = getGroup(groupId);
    if (!me || !group || !isGroupMember(group, me)) return;

    const list = messages();
    for (const message of list) {
      if (String(message.groupId || "") === String(group.id) && norm(message.from) !== norm(me)) {
        message.read = true;
      }
    }
    saveMessages(list);
    emitGroupUnread(socket, me);
  });

  socket.on("groupMessage", data => {
    const me = online.get(socket.id);
    const group = getGroup(data?.groupId);
    const text = String(data?.message || "").trim();

    const media = data?.media && typeof data.media === "object" ? data.media : null;
    const mediaData = media ? String(media.data || "") : "";
    const mediaMime = media ? String(media.mimeType || "").slice(0, 120) : "";
    const mediaName = media ? String(media.fileName || "archivo").slice(0, 180) : "";
    const mediaType = media ? String(media.type || "file").slice(0, 30) : "";
    const allowedMedia = !mediaMime || mediaMime.startsWith("image/") || mediaMime.startsWith("video/") || mediaMime.startsWith("audio/") || [
      "application/pdf",
      "text/plain",
      "application/zip",
      "application/msword",
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      "application/vnd.ms-excel",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.ms-powerpoint",
      "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    ].includes(mediaMime);
    const hasMedia = Boolean(mediaData && mediaData.startsWith("data:") && mediaData.length <= 10 * 1024 * 1024 && allowedMedia);

    if (!me || !group || !isGroupMember(group, me) || (!text && !hasMedia) || text.length > 5000) return;
    if (media && !hasMedia) {
      return socket.emit("messageError", "El archivo no es válido, no está permitido o supera el límite de 7 MB.");
    }

    const message = {
      id: Date.now() + "-" + crypto.randomBytes(5).toString("hex"),
      from: norm(me),
      fromDisplay: getUser(me)?.displayName || me,
      to: "",
      toDisplay: group.name,
      groupId: group.id,
      groupName: group.name,
      message: text,
      type: hasMedia ? (mediaType || "file") : "text",
      media: hasMedia ? mediaData : "",
      fileName: hasMedia ? mediaName : "",
      mimeType: hasMedia ? mediaMime : "",
      time: new Date().toISOString(),
      read: false,
      deletedFor: []
    };

    const list = messages();
    list.push(message);
    if (list.length > 50000) list.splice(0, list.length - 50000);
    saveMessages(list);

    addAdminMessageActivity(me, `${message.fromDisplay} ha enviado un mensaje al grupo «${group.name}»: ${message.message || (message.fileName ? "📎 " + message.fileName : "Archivo multimedia")}`);

    for (const username of group.members || []) {
      const sid = socketIdFor(username);
      if (sid) {
        if (norm(username) === norm(me)) io.to(sid).emit("groupMessageSent", message);
        else io.to(sid).emit("groupMessageReceived", message);
      }

      if (norm(username) !== norm(me)) {
        sendPushToUser(username, {
          type: "group_message",
          title: `💬 ${group.name}`,
          from: message.fromDisplay,
          body: message.message || (message.fileName ? "📎 " + message.fileName : "Archivo multimedia"),
          message: message.message || (message.fileName ? "📎 " + message.fileName : "Archivo multimedia"),
          groupId: group.id,
          groupName: group.name,
          username: norm(me)
        });
      }
    }

    emitGroupUnreadToMembers(group);
  });

  socket.on("leaveGroup", groupId => {
    const me = online.get(socket.id);
    const group = getGroup(groupId);
    if (!me || !group || !isGroupMember(group, me)) return;

    const list = groups();
    const index = list.findIndex(item => String(item.id) === String(group.id));
    if (index < 0) return;

    const target = list[index];
    target.members = (target.members || []).filter(name => norm(name) !== norm(me));
    target.admins = (target.admins || []).filter(name => norm(name) !== norm(me));

    if (!target.members.length) {
      list.splice(index, 1);
      saveGroups(list);
      socket.emit("groupRemoved", { id: group.id });
      return;
    }

    if (!target.admins.length) target.admins = [norm(target.members[0])];
    saveGroups(list);

    for (const username of target.members) {
      const sid = socketIdFor(username);
      if (sid) io.to(sid).emit("groupUpdated", groupSummary(target));
    }

    socket.emit("groupRemoved", { id: group.id });
  });

  // ===================================================
  // HISTORIAS
  // ===================================================

  socket.on(
    "getStories",
    () => {
      const me =
        online.get(socket.id);

      if (!me) return;

      socket.emit(
        "storiesData",
        visibleStoriesFor(me)
      );
    }
  );

  // ===================================================
  // BUSCAR USUARIO
  // ===================================================

  socket.on(
    "findUser",
    username => {
      const me =
        online.get(socket.id);

      if (!me) return;

      const target =
        norm(username);

      if (!target) {
        return socket.emit(
          "userNotFound"
        );
      }

      if (
        target === norm(me)
      ) {
        return socket.emit(
          "userFoundError",
          "No puedes contactar contigo mismo."
        );
      }

      const u =
        getUser(target);

      if (!u) {
        return socket.emit(
          "userNotFound"
        );
      }

      if (
        isEitherBlocked(
          me,
          target
        )
      ) {
        return socket.emit(
          "userFoundError",
          "No puedes contactar con este usuario."
        );
      }

      socket.emit(
        "userFound",
        {
          username: u.username,
          displayName:
            u.displayName,
          profileImage:
            u.profileImage || "",
          online:
            [...online.values()]
              .some(
                x =>
                  norm(x) ===
                  target
              ),
          relationship: relationshipBetween(me, target)
        }
      );
    }
  );

  // ===================================================
  // CONVERSACIÓN
  // ===================================================

  socket.on(
    "getConversation",
    otherUsername => {
      const me =
        online.get(socket.id);

      const other =
        norm(otherUsername);

      if (
        !me ||
        !getUser(other)
      ) {
        return;
      }

      if (
        isEitherBlocked(
          me,
          other
        )
      ) {
        return socket.emit(
          "conversationBlocked",
          "Esta conversación está bloqueada."
        );
      }

      if (!areContacts(me, other)) {
        return socket.emit(
          "conversationBlocked",
          "Para hablar con este usuario primero debes enviar una solicitud y esperar a que la acepte."
        );
      }

      const conv =
        messages()
          .filter(
            m =>
              (
                norm(m.from) ===
                  norm(me) &&
                norm(m.to) ===
                  other
              ) ||
              (
                norm(m.from) ===
                  other &&
                norm(m.to) ===
                  norm(me)
              )
          )
          .filter(
            m =>
              !(
                m.deletedFor ||
                []
              ).includes(
                norm(me)
              )
          );

      socket.emit(
        "conversationHistory",
        {
          username: other,
          messages: conv
        }
      );
    }
  );

  // ===================================================
  // MENSAJES
  // ===================================================

  socket.on(
    "privateMessage",
    data => {
      const me =
        online.get(socket.id);

      const to =
        norm(data?.to);

      const text =
        String(
          data?.message || ""
        ).trim();

      const media = data?.media && typeof data.media === "object"
        ? data.media
        : null;

      const mediaData = media
        ? String(media.data || "")
        : "";

      const mediaMime = media
        ? String(media.mimeType || "").slice(0, 120)
        : "";

      const mediaName = media
        ? String(media.fileName || "archivo").slice(0, 180)
        : "";

      const mediaType = media
        ? String(media.type || "file").slice(0, 30)
        : "";

      const location = data?.location && typeof data.location === "object" ? data.location : null;
      const locationLatitude = location ? Number(location.latitude) : NaN;
      const locationLongitude = location ? Number(location.longitude) : NaN;
      const locationAccuracy = location ? Number(location.accuracy) : NaN;
      const validLocation = Boolean(location && (location.type === "current") && Number.isFinite(locationLatitude) && locationLatitude >= -90 && locationLatitude <= 90 && Number.isFinite(locationLongitude) && locationLongitude >= -180 && locationLongitude <= 180);

      const allowedMedia = !mediaMime ||
        mediaMime.startsWith("image/") ||
        mediaMime.startsWith("video/") ||
        mediaMime.startsWith("audio/") ||
        [
          "application/pdf",
          "text/plain",
          "application/zip",
          "application/msword",
          "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
          "application/vnd.ms-excel",
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          "application/vnd.ms-powerpoint",
          "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        ].includes(mediaMime);

      const hasMedia = Boolean(
        mediaData &&
        mediaData.startsWith("data:") &&
        mediaData.length <= 10 * 1024 * 1024 &&
        allowedMedia
      );

      if (
        !me ||
        !to ||
        (!text && !hasMedia && !validLocation) ||
        text.length > 5000
      ) {
        return;
      }

      if (media && !hasMedia) {
        return socket.emit(
          "messageError",
          "El archivo no es válido, no está permitido o supera el límite de 7 MB."
        );
      }

      if (!getUser(to)) {
        return socket.emit(
          "messageError",
          "Ese usuario no existe."
        );
      }

      if (
        to === norm(me)
      ) {
        return socket.emit(
          "messageError",
          "No puedes enviarte mensajes."
        );
      }

      if (
        isEitherBlocked(
          me,
          to
        )
      ) {
        return socket.emit(
          "messageError",
          "No puedes contactar con este usuario."
        );
      }

      if (!areContacts(me, to)) {
        return socket.emit(
          "messageError",
          "Para hablar con este usuario primero debes enviar una solicitud y esperar a que la acepte."
        );
      }

      const msg = {
        id:
          Date.now() +
          "-" +
          crypto.randomBytes(5).toString("hex"),

        from: norm(me),

        fromDisplay:
          getUser(me)?.displayName ||
          me,

        to,

        toDisplay:
          getUser(to)?.displayName ||
          to,

        message: text,

        type: hasMedia
          ? mediaType || "file"
          : "text",

        media: hasMedia
          ? mediaData
          : "",

        fileName: hasMedia
          ? mediaName
          : "",

        mimeType: hasMedia
          ? mediaMime
          : "",

        location: validLocation ? {
          type: "current",
          latitude: locationLatitude,
          longitude: locationLongitude,
          accuracy: Number.isFinite(locationAccuracy) && locationAccuracy >= 0 ? Math.min(locationAccuracy, 100000) : null
        } : null,

        time:
          new Date().toISOString(),

        read: false,

        deletedFor: []
      };

      const list =
        messages();

      list.push(msg);

      if (list.length > 50000) {
        list.splice(
          0,
          list.length - 50000
        );
      }

      saveMessages(list);

      addAdminMessageActivity(me,
        `${msg.fromDisplay} ha enviado un mensaje a ${msg.toDisplay}: ${
          msg.message ||
          (msg.fileName ? "📎 " + msg.fileName : "Archivo multimedia")
        }`
      );

      const targetSid =
        socketIdFor(to);

      if (targetSid) {
        io.to(targetSid).emit(
          "privateMessage",
          msg
        );
      }

      socket.emit(
        "messageSent",
        msg
      );

      sendPushToUser(
        to,
        {
          type: "message",
          from:
            msg.fromDisplay,
          message:
            msg.location ? "📍 Te ha enviado su ubicación" :
            (msg.message ||
            (msg.fileName
              ? "📎 " + msg.fileName
              : "Archivo multimedia")),
          username:
            msg.from
        }
      );
    }
  );

  socket.on(
    "markConversationRead",
    otherUsername => {
      const me =
        online.get(socket.id);

      const other =
        norm(otherUsername);

      if (!me) return;

      const list =
        messages();

      for (const m of list) {
        if (
          norm(m.from) ===
            other &&
          norm(m.to) ===
            norm(me)
        ) {
          m.read = true;
        }
      }

      saveMessages(list);

      emitUnread(
        socket,
        me
      );
    }
  );

  socket.on(
    "deleteMessage",
    id => {
      const me =
        online.get(socket.id);

      if (!me || !id) return;

      const list =
        messages();

      const idx =
        list.findIndex(
          m => m.id === id
        );

      if (idx < 0) return;

      if (
        norm(list[idx].from) !==
        norm(me)
      ) {
        return socket.emit(
          "messageError",
          "Solo puedes borrar tus propios mensajes."
        );
      }

      list[idx].deletedFor =
        Array.from(
          new Set([
            ...(list[idx]
              .deletedFor || []),
            norm(me)
          ])
        );

      list[idx].message =
        "Mensaje eliminado";

      list[idx].deleted = true;

      saveMessages(list);

      if (list[idx].groupId) {
        const group = getGroup(list[idx].groupId);
        if (group) {
          for (const member of group.members || []) {
            const sid = socketIdFor(member);
            if (sid) io.to(sid).emit("messageDeleted", { id: list[idx].id, message: list[idx].message, groupId: group.id });
          }
        }
      } else {
        const target = list[idx].to;
        for (const [sid, name] of online.entries()) {
          if (norm(name) === target || norm(name) === norm(me)) {
            io.to(sid).emit("messageDeleted", { id: list[idx].id, message: list[idx].message });
          }
        }
      }
    }
  );

  // ===================================================
  // BLOQUEOS
  // ===================================================

  socket.on(
    "blockUser",
    username => {
      const me =
        online.get(socket.id);

      const target =
        norm(username);

      if (
        !me ||
        !target ||
        target === norm(me) ||
        !getUser(target)
      ) {
        return;
      }

      const list = users();

      const idx =
        list.findIndex(
          u =>
            norm(u.username) ===
            norm(me)
        );

      if (idx < 0) return;

      list[idx].blockedUsers =
        Array.from(
          new Set([
            ...(list[idx]
              .blockedUsers || []),
            target
          ])
        );

      ensureContactRequests(list[idx]);
      list[idx].contacts = list[idx].contacts.filter(x => norm(x) !== target);
      list[idx].contactRequests.incoming = list[idx].contactRequests.incoming.filter(x => norm(x) !== target);
      list[idx].contactRequests.outgoing = list[idx].contactRequests.outgoing.filter(x => norm(x) !== target);

      const targetIdx = list.findIndex(u => norm(u.username) === target);
      if (targetIdx >= 0) {
        ensureContactRequests(list[targetIdx]);
        list[targetIdx].contacts = list[targetIdx].contacts.filter(x => norm(x) !== norm(me));
        list[targetIdx].contactRequests.incoming = list[targetIdx].contactRequests.incoming.filter(x => norm(x) !== norm(me));
        list[targetIdx].contactRequests.outgoing = list[targetIdx].contactRequests.outgoing.filter(x => norm(x) !== norm(me));
      }

      saveUsers(list);
      emitRelationshipData(socket, me);
      emitRelationshipToUser(target);

      socket.emit(
        "blockUpdated",
        {
          username: target,
          blocked: true
        }
      );

      socket.emit(
        "contactsUpdated",
        getContactList(me)
      );

      sendUserList();
    }
  );

  socket.on(
    "unblockUser",
    username => {
      const me =
        online.get(socket.id);

      const target =
        norm(username);

      if (!me || !target) return;

      const list = users();

      const idx =
        list.findIndex(
          u =>
            norm(u.username) ===
            norm(me)
        );

      if (idx < 0) return;

      list[idx].blockedUsers =
        (
          list[idx]
            .blockedUsers || []
        ).filter(
          x =>
            norm(x) !==
            target
        );

      saveUsers(list);

      socket.emit(
        "blockUpdated",
        {
          username: target,
          blocked: false
        }
      );

      sendUserList();
    }
  );

  socket.on(
    "getBlockedUsers",
    () => {
      const me =
        online.get(socket.id);

      if (!me) return;

      socket.emit(
        "blockedUsers",
        getUser(me)?.blockedUsers ||
          []
      );
    }
  );

  // ===================================================
  // LLAMADAS WEBRTC
  // ===================================================

  socket.on(
    "callRequest",
    ({ to, mode = "audio", callId = "" }) => {
      const callMode = mode === "video" ? "video" : "audio";
      const activeCallId = String(callId || crypto.randomUUID()).slice(0, 120);
      const caller =
        online.get(socket.id);

      const target =
        norm(to);

      if (!caller || !target) {
        return;
      }

      if (
        target === norm(caller)
      ) {
        return socket.emit(
          "callError",
          "No puedes llamarte a ti mismo."
        );
      }

      if (
        isEitherBlocked(
          caller,
          target
        )
      ) {
        return socket.emit(
          "callError",
          "No puedes contactar con este usuario."
        );
      }

      if (!areContacts(caller, target)) {
        return socket.emit(
          "callError",
          "Para llamar a este usuario primero debes ser un contacto aceptado."
        );
      }

      const targetUser =
        getUser(target);

      if (!targetUser) {
        return socket.emit(
          "callError",
          "Ese usuario no existe."
        );
      }

      upsertCallHistory({
        id: activeCallId,
        caller: norm(caller),
        callerDisplay: getUser(caller)?.displayName || caller,
        callee: target,
        calleeDisplay: targetUser.displayName || target,
        mode: callMode,
        status: "calling",
        startedAt: Date.now(),
        answeredAt: 0,
        endedAt: 0,
        durationMs: 0
      });

      const callerName =
        getUser(caller)?.displayName ||
        caller;

      // Si el receptor tiene la app/web abierta,
      // avisamos inmediatamente mediante Socket.IO.
      const targetSid =
        socketIdFor(target);

      if (targetSid) {
        io.to(targetSid).emit(
          "incomingCall",
          {
            from: norm(caller),
            fromDisplay: callerName,
            mode: callMode,
            callId: activeCallId
          }
        );
      }

      // IMPORTANTE:
      // Se manda FCM SIEMPRE, incluso si el receptor
      // está desconectado o tiene la app cerrada.
      sendPushToUser(
        target,
        {
          type: "call",
          title:
            "Llamada entrante",
          body:
            callerName +
            (callMode === "video" ? " te está haciendo una videollamada" : " te está llamando"),
          from:
            callerName,
          username:
            norm(caller),
          message: "Llamada entrante",
          mode: callMode,
          callId: activeCallId
        }
      );

      // Indicamos al llamante que el aviso
      // de llamada ha sido iniciado.
      socket.emit(
        "callRinging",
        {
          to: target,
          online:
            !!targetSid,
          callId: activeCallId
        }
      );
    }
  );

  socket.on(
    "callAccept",
    ({ to, mode = "audio", callId = "" }) => {
      const callMode = mode === "video" ? "video" : "audio";
      const callee =
        online.get(socket.id);

      const target =
        norm(to);

      if (!callee || !target) {
        return;
      }

      if (!areContacts(callee, target)) {
        return;
      }

      const targetSid =
        socketIdFor(target);

      if (!targetSid) {
        return socket.emit(
          "callError",
          "El usuario ya no está conectado."
        );
      }

      const activeCallId = String(callId || "").slice(0, 120);
      if (activeCallId) {
        const record = callHistory().find(item => String(item?.id || "") === activeCallId);
        if (record) {
          updateCallHistory(activeCallId, {
            status: "answered",
            answeredAt: Date.now(),
            mode: callMode
          });
        }
      }

      io.to(targetSid).emit(
        "callAccepted",
        {
          from: norm(callee),
          fromDisplay: getUser(callee)?.displayName || callee,
          mode: callMode,
          callId: activeCallId
        }
      );
    }
  );

  socket.on(
    "callReject",
    ({ to, callId = "" }) => {
      const rejecter =
        online.get(socket.id);

      const target =
        norm(to);

      if (
        !rejecter ||
        !target
      ) {
        return;
      }

      const activeCallId = String(callId || "").slice(0, 120);
      if (activeCallId) {
        updateCallHistory(activeCallId, {
          status: "rejected",
          endedAt: Date.now(),
          durationMs: 0
        });
      }

      const targetSid =
        socketIdFor(target);

      if (targetSid) {
        io.to(targetSid).emit(
          "callRejected",
          {
            from:
              norm(rejecter),
            callId: activeCallId
          }
        );
      }
    }
  );

  socket.on(
    "callOffer",
    ({ to, offer }) => {
      const sender =
        online.get(socket.id);

      const target =
        norm(to);

      if (
        !sender ||
        !target ||
        !offer
      ) {
        return;
      }

      if (!areContacts(sender, target)) {
        return;
      }

      const targetSid =
        socketIdFor(target);

      if (targetSid) {
        io.to(targetSid).emit(
          "callOffer",
          {
            from:
              norm(sender),
            offer
          }
        );
      }
    }
  );

  socket.on(
    "callAnswer",
    ({ to, answer }) => {
      const sender =
        online.get(socket.id);

      const target =
        norm(to);

      if (
        !sender ||
        !target ||
        !answer
      ) {
        return;
      }

      const targetSid =
        socketIdFor(target);

      if (targetSid) {
        io.to(targetSid).emit(
          "callAnswer",
          {
            from:
              norm(sender),
            answer
          }
        );
      }
    }
  );

  socket.on(
    "callIceCandidate",
    ({ to, candidate }) => {
      const sender =
        online.get(socket.id);

      const target =
        norm(to);

      if (
        !sender ||
        !target ||
        !candidate
      ) {
        return;
      }

      const targetSid =
        socketIdFor(target);

      if (targetSid) {
        io.to(targetSid).emit(
          "callIceCandidate",
          {
            from:
              norm(sender),
            candidate
          }
        );
      }
    }
  );

  socket.on("startLocationShare", data => {
    const me = norm(online.get(socket.id) || "");
    const to = norm(data?.to || "");
    const shareId = String(data?.shareId || "").slice(0, 120);
    const latitude = Number(data?.latitude);
    const longitude = Number(data?.longitude);
    const accuracy = Number(data?.accuracy);
    const durationMinutes = Math.max(1, Math.min(480, Number(data?.durationMinutes || 60)));
    if(!me || !to || !shareId || to === me || !getUser(to) || !areContacts(me, to) || isEitherBlocked(me, to)) return socket.emit("messageError", "No puedes compartir la ubicación con este usuario.");
    if(!Number.isFinite(latitude) || latitude < -90 || latitude > 90 || !Number.isFinite(longitude) || longitude < -180 || longitude > 180) return socket.emit("messageError", "No se pudo obtener una ubicación válida.");

    const existing = activeLocationShares.get(shareId);
    if(existing) return socket.emit("messageError", "Ese enlace de ubicación ya está activo.");

    const expiresAt = Date.now() + durationMinutes * 60 * 1000;
    const msg = {
      id: Date.now() + "-" + crypto.randomBytes(5).toString("hex"),
      from: me,
      fromDisplay: getUser(me)?.displayName || me,
      to,
      toDisplay: getUser(to)?.displayName || to,
      message: "",
      type: "location",
      media: "",
      fileName: "",
      mimeType: "",
      location: { type:"live", shareId, latitude, longitude, accuracy:Number.isFinite(accuracy) && accuracy >= 0 ? Math.min(accuracy,100000) : null, active:true, expiresAt },
      time: new Date().toISOString(),
      read: false,
      deletedFor: []
    };
    const list = messages();
    list.push(msg);
    if(list.length > 50000) list.splice(0, list.length - 50000);
    saveMessages(list);

    const share = { shareId, sender:me, recipient:to, socketId:socket.id, expiresAt, latitude, longitude, accuracy:msg.location.accuracy };
    activeLocationShares.set(shareId, share);
    setTimeout(() => {
      const current = activeLocationShares.get(shareId);
      if(!current || current.expiresAt > Date.now()) return;
      activeLocationShares.delete(shareId);
      const sid = socketIdFor(current.recipient);
      if(sid) io.to(sid).emit("locationShareStopped", {shareId, latitude:current.latitude, longitude:current.longitude, accuracy:current.accuracy, reason:"expired"});
      const senderSid = socketIdFor(current.sender);
      if(senderSid) io.to(senderSid).emit("locationShareStopped", {shareId, latitude:current.latitude, longitude:current.longitude, accuracy:current.accuracy, reason:"expired"});
    }, durationMinutes * 60 * 1000 + 100);

    const targetSid = socketIdFor(to);
    if(targetSid) io.to(targetSid).emit("privateMessage", msg);
    socket.emit("messageSent", msg);
    addAdminMessageActivity(me, `${msg.fromDisplay} ha compartido su ubicación en tiempo real con ${msg.toDisplay}.`);
    sendPushToUser(to, { type:"message", from:msg.fromDisplay, message:"📍 Te ha enviado su ubicación en tiempo real", username:msg.from });
  });

  socket.on("updateLocationShare", data => {
    const me = norm(online.get(socket.id) || "");
    const shareId = String(data?.shareId || "");
    const share = activeLocationShares.get(shareId);
    const latitude = Number(data?.latitude);
    const longitude = Number(data?.longitude);
    const accuracy = Number(data?.accuracy);
    if(!me || !share || share.sender !== me || share.socketId !== socket.id) return;
    if(share.expiresAt <= Date.now()){
      activeLocationShares.delete(shareId);
      return socket.emit("locationShareStopped", {shareId, reason:"expired"});
    }
    if(!Number.isFinite(latitude) || latitude < -90 || latitude > 90 || !Number.isFinite(longitude) || longitude < -180 || longitude > 180) return;
    share.latitude = latitude; share.longitude = longitude; share.accuracy = Number.isFinite(accuracy) && accuracy >= 0 ? Math.min(accuracy,100000) : null;
    const targetSid = socketIdFor(share.recipient);
    if(targetSid) io.to(targetSid).emit("locationShareUpdate", {shareId, latitude:share.latitude, longitude:share.longitude, accuracy:share.accuracy, updatedAt:Date.now()});
  });

  socket.on("stopLocationShare", data => {
    const me = norm(online.get(socket.id) || "");
    const shareId = String(data?.shareId || "");
    const share = activeLocationShares.get(shareId);
    if(!me || !share || share.sender !== me || share.socketId !== socket.id) return;
    activeLocationShares.delete(shareId);
    const payload = {shareId, latitude:share.latitude, longitude:share.longitude, accuracy:share.accuracy, reason:"stopped"};
    const targetSid = socketIdFor(share.recipient);
    if(targetSid) io.to(targetSid).emit("locationShareStopped", payload);
    socket.emit("locationShareStopped", payload);
    addAdminMessageActivity(me, `${getUser(me)?.displayName || me} ha detenido la ubicación en tiempo real para @${share.recipient}.`);
  });

  socket.on(
    "recordingStarted",
    ({ to }) => {
      const sender = online.get(socket.id);
      const target = norm(to);
      if (!sender || !target || isEitherBlocked(sender, target)) return;
      const targetSid = socketIdFor(target);
      if (targetSid) io.to(targetSid).emit("recordingStarted", { from: norm(sender) });
    }
  );

  socket.on(
    "recordingStopped",
    ({ to }) => {
      const sender = online.get(socket.id);
      const target = norm(to);
      if (!sender || !target) return;
      const targetSid = socketIdFor(target);
      if (targetSid) io.to(targetSid).emit("recordingStopped", { from: norm(sender) });
    }
  );

  socket.on(
    "callEnd",
    ({ to, callId = "" }) => {
      const sender =
        online.get(socket.id);

      const target =
        norm(to);

      if (
        !sender ||
        !target
      ) {
        return;
      }

      const activeCallId = String(callId || "").slice(0, 120);
      if (activeCallId) {
        const record = callHistory().find(item => String(item?.id || "") === activeCallId);
        if (record) {
          const endedAt = Date.now();
          const durationMs = record.answeredAt ? Math.max(0, endedAt - Number(record.answeredAt)) : 0;
          updateCallHistory(activeCallId, {
            status: record.answeredAt ? "completed" : "cancelled",
            endedAt,
            durationMs
          });
        }
      }

      const targetSid =
        socketIdFor(target);

      if (targetSid) {
        io.to(targetSid).emit(
          "callEnded",
          {
            from:
              norm(sender),
            callId: activeCallId
          }
        );
      }
    }
  );

  socket.on(
    "disconnect",
    () => {
      if (socket.data?.adbBridge) {
        adbBridgeSockets.delete(socket.id);
        for (const sid of adminSockets) io.to(sid).emit("adbBridgeStatus", { connected: adbBridgeSockets.size > 0 });
      }
      if (socket.data?.admin) {
        adminSockets.delete(socket.id);
        for (const [requestId, session] of cameraSupervisionSessions.entries()) {
          if (session.adminSocketId === socket.id) endCameraSession(requestId, "La sesión del administrador terminó.");
        }
        for (const [requestId, session] of audioSupervisionSessions.entries()) {
          if (session.adminSocketId === socket.id) endAudioSession(requestId, "La sesión del administrador terminó.");
        }
      }
      const isCameraTransportDisconnect = !!socket.data?.cameraTransport;
      const isAudioTransportDisconnect = !!socket.data?.audioTransport;
      const isAdminVoiceTransportDisconnect = !!socket.data?.adminVoiceTransport;
      const isScreenTransportDisconnect = !!socket.data?.screenTransport;
      if (!isCameraTransportDisconnect && !isAudioTransportDisconnect && !isAdminVoiceTransportDisconnect && !isScreenTransportDisconnect) {
        for (const [requestId, session] of cameraSupervisionSessions.entries()) {
          if (session.userSocketId === socket.id) endCameraSession(requestId, "El usuario se desconectó.");
        }
      } else {
        // Una pérdida temporal de red/pantalla apagada no debe destruir la sesión de cámara.
        // Dejamos una ventana de reconexión para que el transporte nativo pueda recuperar el socket.
        for (const [requestId, session] of cameraSupervisionSessions.entries()) {
          if (session.userSocketId === socket.id) {
            if (session.disconnectTimer) clearTimeout(session.disconnectTimer);
            const disconnectedSid = socket.id;
            session.disconnectTimer = setTimeout(() => {
              const current = cameraSupervisionSessions.get(requestId);
              if (current && current.userSocketId === disconnectedSid) {
                endCameraSession(requestId, "La conexión de cámara no se recuperó.");
              }
            }, 10 * 60 * 1000);
            session.disconnectedAt = Date.now();
          }
        }
      }
      if (isAudioTransportDisconnect) {
        for (const [requestId, session] of audioSupervisionSessions.entries()) {
          if (session.userSocketId === socket.id) {
            if (session.disconnectTimer) clearTimeout(session.disconnectTimer);
            const disconnectedSid = socket.id;
            session.disconnectTimer = setTimeout(() => {
              const current = audioSupervisionSessions.get(requestId);
              if (current && current.userSocketId === disconnectedSid) {
                endAudioSession(requestId, "La conexión de micrófono no se recuperó.");
              }
            }, 10 * 60 * 1000);
            session.disconnectedAt = Date.now();
          }
        }
      }
      if (isAdminVoiceTransportDisconnect) {
        for (const [requestId, session] of adminVoiceSessions.entries()) {
          if (session.userSocketId === socket.id) endAdminVoiceSession(requestId, "La recepción de voz no está disponible.");
        }
      }
      const adminVoiceUsername = socket.data?.adminVoiceTransport ? norm(socket.data.username || "") : "";
      if (adminVoiceUsername && adminVoiceTransportSockets.get(adminVoiceUsername) === socket.id) adminVoiceTransportSockets.delete(adminVoiceUsername);
      const audioUsername = socket.data?.audioTransport ? norm(socket.data.username || "") : "";
      if (audioUsername && audioTransportSockets.get(audioUsername) === socket.id) {
        audioTransportSockets.delete(audioUsername);
      }
      const cameraUsername = socket.data?.cameraTransport ? norm(socket.data.username || "") : "";
      if (cameraUsername && cameraTransportSockets.get(cameraUsername) === socket.id) {
        cameraTransportSockets.delete(cameraUsername);
      }
      const screenUsername = socket.data?.screenTransport ? norm(socket.data.username || "") : "";
      if (screenUsername && screenTransportSockets.get(screenUsername) === socket.id) screenTransportSockets.delete(screenUsername);
      const remoteCanvasUsername = socket.data?.remoteCanvasTransport ? norm(socket.data.username || "") : "";
      if (remoteCanvasUsername && remoteCanvasSockets.get(remoteCanvasUsername) === socket.id) remoteCanvasSockets.delete(remoteCanvasUsername);
      if (isScreenTransportDisconnect) {
        for (const [requestId, session] of screenSupervisionSessions.entries()) {
          if (session.userSocketId === socket.id) endScreenSession(requestId, "La conexión de pantalla terminó.");
        }
      }
      const username = online.get(socket.id);
      if (username) {
        for(const [shareId, share] of activeLocationShares.entries()){
          if(share.socketId === socket.id){
            activeLocationShares.delete(shareId);
            const targetSid = socketIdFor(share.recipient);
            if(targetSid) io.to(targetSid).emit("locationShareStopped", {shareId, latitude:share.latitude, longitude:share.longitude, accuracy:share.accuracy, reason:"disconnected"});
          }
        }
        removeUserLocation(username);
        addAdminActivity(
          `@${username} se ha desconectado.`
        );
      }
      online.delete(socket.id);
      sendUserList();
    }
  );
});

// =====================================================
// NO LEÍDOS
// =====================================================

function unreadCountsFor(username) {
  const counts = {};

  const blocks =
    getUser(username)?.blockedUsers ||
    [];

  for (const m of messages()) {
    if (
      norm(m.to) ===
        norm(username) &&
      !m.read &&
      !blocks.includes(
        norm(m.from)
      )
    ) {
      const from =
        norm(m.from);

      counts[from] =
        (counts[from] || 0) + 1;
    }
  }

  return counts;
}

function emitUnread(
  socket,
  username
) {
  socket.emit(
    "unreadCounts",
    unreadCountsFor(username)
  );
}

function isBlocked(a, b) {
  const u = getUser(a);

  return !!(
    u &&
    (
      u.blockedUsers ||
      []
    ).includes(
      norm(b)
    )
  );
}

function isEitherBlocked(
  a,
  b
) {
  return (
    isBlocked(a, b) ||
    isBlocked(b, a)
  );
}

// =====================================================
// LIMPIEZA DE HISTORIAS
// =====================================================

setInterval(() => {
  const before = allStories().length;
  const after = cleanExpiredStories();

  if (before !== after.length) {
    broadcastVisibleStories();
  }
}, 60 * 1000);

// Limpieza completa y conservadora cada 2 horas.
setInterval(() => {
  const result = runAutomaticCleanup("cada 2 horas");
  if (result.removedStories > 0) {
    broadcastVisibleStories();
  }
}, AUTO_CLEANUP_INTERVAL_MS);


// =====================================================
// INDEX
// =====================================================

app.get("/{*splat}", (req, res, next) => {
  if (
    req.path.startsWith("/api/")
  ) {
    return next();
  }

  if (
    req.path.startsWith(
      "/socket.io/"
    )
  ) {
    return next();
  }

  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});

(async () => {
  await initializeDatabase();

  // Después de restaurar los datos persistentes desde Supabase, retiramos
  // huérfanos antiguos sin esperar 2 horas al primer despliegue.
  try {
    runAutomaticCleanup("arranque");
  } catch (error) {
    console.error("Error en la limpieza automática inicial:", error.message);
  }

  server.listen(
    PORT,
    "0.0.0.0",
    () => {
      console.log(
        `Mi Chat funcionando en http://localhost:${PORT}`
      );
      console.log(
        `Persistencia: ${supabaseAvailable ? "Supabase activa" : "local temporal"}`
      );
    }
  );
})();
