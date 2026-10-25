import fs from "node:fs/promises";
import WebSocket from "ws";

const email = process.env.IRCLOUD_EMAIL;
const password = process.env.IRCLOUD_PASSWORD;
if (!email || !password) throw new Error("Missing IRCLOUD_EMAIL or IRCLOUD_PASSWORD");

const rawNetwork = process.env.IRCLOUD_NETWORK || "";
if (!rawNetwork.trim()) throw new Error("Missing or empty IRCLOUD_NETWORK environment variable");
const targetNetwork = rawNetwork.toLowerCase();

const log = {
  info: (...args) => console.log(`[${new Date().toLocaleTimeString()}]`, ...args),
  warn: (...args) => console.warn(`[${new Date().toLocaleTimeString()}] [WARN]`, ...args)
};

let UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36";
try {
  const uaData = await fs.readFile("./ua.json", "utf-8");
  const uaList = JSON.parse(uaData);
  if (Array.isArray(uaList) && uaList.length > 0) {
    UserAgent = uaList[Math.floor(Math.random() * uaList.length)];
  }
} catch (err) {
  log.warn(`Could not load ua.json, using default fallback. Error: ${err.message}`);
}

async function login() {
  const tokenRes = await fetch("https://www.irccloud.com/chat/auth-formtoken", {
    method: "POST",
    headers: { 
      "content-length": "0",
      "User-Agent": UserAgent
    }
  });
  const token = await tokenRes.json();
  if (!tokenRes.ok || !token.success || !token.token) {
    throw new Error("Could not obtain IRCCloud auth token");
  }

  const body = new URLSearchParams({ email, password, token: token.token });
  const res = await fetch("https://www.irccloud.com/chat/login", {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      "User-Agent": UserAgent,
      "x-auth-formtoken": token.token
    },
    body
  });
  const data = await res.json();
  if (!res.ok || !data.success || !data.session) {
    throw new Error("IRCCloud login failed");
  }

  return {
    session: data.session,
    wsUrl: `wss://${data.websocket_host || "api.irccloud.com"}${data.websocket_path || "/"}`
  };
}

function jsonLines(text) {
  const raw = String(text).trim();
  if (!raw) return [];
  try {
    const whole = JSON.parse(raw);
    if (Array.isArray(whole)) return whole;
    if (whole && typeof whole === "object") return [whole];
  } catch {}
  return raw.split(/\r?\n/).flatMap(line => {
    try { return line.trim() ? [JSON.parse(line)] : []; }
    catch { return []; }
  });
}

class IRCCloud {
  constructor(session, wsUrl) {
    this.session = session;
    this.wsUrl = wsUrl;
    this.ws = null;
    this.req = 0;
    this.pending = new Map();
    this.connections = new Map();
    this.buffers = new Map();
    this.backlogDone = false;
    this.processingOob = false;
    this.queued = [];
    this.recentMessages = [];
    this.backlogTimer = null;
    this.statusListeners = new Set();
    this.backlogResolve = null;
    this.backlogReject = null;
  }

  async connect() {
    this.ws = new WebSocket(this.wsUrl, {
      headers: {
        Cookie: `session=${this.session}`,
        Origin: "https://api.irccloud.com",
        "User-Agent": UserAgent
      },
      perMessageDeflate: true
    });

    this.ws.on("error", err => {
      log.warn("WebSocket global error:", err.message);
      if (this.backlogReject) {
        if (this.backlogTimer) clearTimeout(this.backlogTimer);
        this.backlogReject?.(err);
        this.backlogReject = null;
        this.backlogResolve = null;
      }
    });

    this.ws.on("message", data => {
      for (const msg of jsonLines(data.toString())) {
        this.handle(msg).catch(e => log.warn("Error handling WS message:", e.message));
      }
    });

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error("WebSocket handshake timeout"));
      }, 30000);

      const onOpen = () => { cleanup(); resolve(); };
      const onError = (e) => { cleanup(); reject(e); };

      const cleanup = () => {
        clearTimeout(timer);
        this.ws.off("open", onOpen);
        this.ws.off("error", onError);
      };

      this.ws.once("open", onOpen);
      this.ws.once("error", onError);
    });

    await new Promise((resolve, reject) => {
      this.backlogResolve = resolve;
      this.backlogReject = reject;
      
      this.backlogTimer = setTimeout(() => {
        this.backlogResolve = null;
        this.backlogReject = null;
        reject(new Error("Timed out loading IRCCloud state backlog"));
      }, 30000);
    });
  }

  async handle(msg) {
    if (msg._reqid && this.pending.has(msg._reqid)) {
      const p = this.pending.get(msg._reqid);
      this.pending.delete(msg._reqid);
      if (msg.success === false || msg.error) p.reject(new Error(JSON.stringify(msg)));
      else p.resolve(msg);
      return;
    }

    if (msg.type === "oob_include") {
      this.processingOob = true;
      const wsUrlObj = new URL(this.wsUrl);
      const base = `https://${wsUrlObj.host}`;
      const backlogUrl = new URL(msg.url, base).href;
      log.info(`Loading IRCCloud backlog from ${new URL(backlogUrl).pathname}`);
      
      try {
        const r = await fetch(backlogUrl, {
          headers: {
            cookie: `session=${this.session}`,
            "User-Agent": UserAgent,
            "accept-encoding": "gzip, deflate"
          }
        });
        if (!r.ok) throw new Error(`Backlog HTTP ${r.status}`);
        
        const text = await r.text();
        const items = jsonLines(text);
        log.info(`Loaded ${items.length} backlog message(s).`);
        
        for (const item of items) this.update(item);
      } catch (e) {
        if (this.backlogTimer) clearTimeout(this.backlogTimer);
        const rejectFn = this.backlogReject;
        this.backlogResolve = null;
        this.backlogReject = null;
        rejectFn?.(e);
      } finally {
        this.processingOob = false;
        const currentQueue = this.queued.splice(0);
        for (const item of currentQueue) this.update(item);
        this.maybeReady();
      }
      return;
    }

    if (this.processingOob) {
      this.queued.push(msg);
      return;
    }

    this.update(msg);
  }

  update(msg) {
    if (msg.type === "buffer_msg") {
      this.recentMessages.push({ ...msg, receivedAt: Date.now() });
      if (this.recentMessages.length > 100) this.recentMessages.shift();
    }
    if (msg.type === "set_shard" && msg.cookie) this.session = msg.cookie;
    if (msg.type === "makeserver") this.connections.set(Number(msg.cid), msg);
    if (msg.type === "server_details_changed") {
      const cid = Number(msg.cid);
      this.connections.set(cid, { ...(this.connections.get(cid) || {}), ...msg });
    }
    if (msg.type === "status_changed") {
      const cid = Number(msg.cid);
      const c = this.connections.get(cid);
      if (c) {
        c.status = msg.new_status;
        for (const listener of this.statusListeners) {
          listener(cid, msg.new_status);
        }
      }
    }
    if (msg.type === "makebuffer") this.buffers.set(Number(msg.bid), msg);
    if (msg.type === "backlog_complete") this.backlogDone = true;
    this.maybeReady();
  }

  maybeReady() {
    if (this.backlogDone && !this.processingOob) {
      if (this.backlogTimer) {
        clearTimeout(this.backlogTimer);
        this.backlogTimer = null;
      }
      const resolveFn = this.backlogResolve;
      this.backlogResolve = null;
      this.backlogReject = null;
      resolveFn?.();
    }
  }

  rpc(method, data = {}, timeout = 20000) {
    const id = ++this.req;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`RPC timed out: ${method}`));
      }, timeout);
      this.pending.set(id, {
        resolve: value => { clearTimeout(timer); resolve(value); },
        reject: value => { clearTimeout(timer); reject(value); }
      });
      this.ws.send(JSON.stringify({ ...data, _method: method, _reqid: id }));
    });
  }

  async waitForStatus(cid, wanted, timeout = 40000) {
    if (this.connections.get(cid)?.status === wanted) return;
    
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.statusListeners.delete(listener);
        reject(new Error(`Connection ${cid} did not become ${wanted} (Timeout)`));
      }, timeout);

      const listener = (eventCid, status) => {
        if (eventCid === cid && status === wanted) {
          clearTimeout(timer);
          this.statusListeners.delete(listener);
          resolve();
        }
      };
      this.statusListeners.add(listener);
    });
  }

  close() {
    try { 
      if (this.ws) {
        if (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING) {
          this.ws.close();
        }
        this.ws.removeAllListeners();
      }
    } catch {}
  }
}
try {
  const auth = await login();
  const client = new IRCCloud(auth.session, auth.wsUrl);
  await client.connect();
  
  const target = [...client.connections.values()].find(c => {
    const name = String(c.name || "").toLowerCase();
    const hostname = String(c.hostname || "").toLowerCase();
    return name.includes(targetNetwork) || hostname.includes(targetNetwork);
  });

  if (!target) {
    client.close();
    throw new Error(
      `Could not find the connection. Available: ${[...client.connections.values()]
        .map(c => `cid=${c.cid} ${c.name || "unnamed"} (${c.hostname || ""})`)
        .join(", ") || "none"}`
    );
  }

  const cid = Number(target.cid);
  log.info(`Connection cid=${cid}, status=${target.status}`);

  if (target.status !== "connected_ready") {
    log.info("Ensuring network is connected_ready...");
    if (!["connected", "connected_joining"].includes(target.status)) {
      await client.rpc("reconnect", { cid });
    }
    await client.waitForStatus(cid, "connected_ready");
  }
client.close();
    log.info("Keepalive completed successfully.");
} catch (error) {
    log.warn(`Automation execution failed: ${error.message}`);
process.exit(1);
}
