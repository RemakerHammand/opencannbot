#!/usr/bin/env bash
set -euo pipefail

CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/opencode"
DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/opencode"
PLUGIN_DIR="$CONFIG_DIR/plugins"
PLUGIN_FILE="$PLUGIN_DIR/cannbot-auth.js"
OPENCODE_JSON="$CONFIG_DIR/opencode.json"
AUTH_JSON="$DATA_DIR/auth.json"

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
red()    { printf '\033[31m%s\033[0m\n' "$*"; }

bold "======================================="
bold "  CANNBOT Provider for OpenCode"
bold "======================================="
echo

command -v opencode >/dev/null 2>&1 || { red "opencode not found. Please install opencode first."; exit 1; }
command -v node >/dev/null 2>&1 || { red "node not found."; exit 1; }

mkdir -p "$PLUGIN_DIR" "$DATA_DIR"

# ── 1. Write plugin ─────────────────────────────────────────────────────

cat > "$PLUGIN_FILE" << 'PLUGIN_EOF'
/**
 * CANNBOT Gateway Auth Plugin for OpenCode
 */

import { homedir, networkInterfaces } from "os";
import { join } from "path";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";

const PLUGIN_ID = "cannbot-gateway-auth";
const PROVIDER_ID = "cannbot";
const GATEWAY_URL = "https://cannbot.hicann.cn/gateway/compatible-mode/v1";
const SESSION_PATH = join(homedir(), ".cannbot", "session.json");
const MODELS_API_URL = "https://cannbot.hicann.cn/cannbot/api/models/list";

const DEBUG_LOG_PATH = join(homedir(), ".local", "share", "opencode", "log", "cannbot-auth-plugin.log");

function debugLog(msg) {
  try {
    const dir = join(homedir(), ".local", "share", "opencode", "log");
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const ts = new Date().toISOString();
    writeFileSync(DEBUG_LOG_PATH, `[${ts}] ${msg}\n`, { flag: "a" });
  } catch {}
}

function readSession() {
  try {
    return JSON.parse(readFileSync(SESSION_PATH, "utf-8"));
  } catch {
    return null;
  }
}

function readAccessTokenFromAuthJson() {
  try {
    const XDG = process.env.XDG_DATA_HOME || join(homedir(), ".local", "share");
    const authJsonPath = join(XDG, "opencode", "auth.json");
    const authJson = JSON.parse(readFileSync(authJsonPath, "utf-8"));
    const entry = authJson["cannbot-cli"];
    if (entry?.type === "oauth" && entry.access) {
      return entry.access;
    }
  } catch {}
  return null;
}

const CAPABILITIES = {
  temperature: true,
  reasoning: true,
  attachment: true,
  toolcall: true,
  input: { text: true, audio: false, image: true, video: false, pdf: false },
  output: { text: true, audio: false, image: false, video: false, pdf: false },
  interleaved: false,
};

const LIMIT = { context: 131072, output: 8192 };
const COST = { input: 0, output: 0, cache: { read: 0, write: 0 } };

const KNOWN_MODELS = {
  "glm-5": { name: "GLM 5", family: "glm" },
  "glm-5.1": { name: "GLM 5.1", family: "glm" },
  "qwen3.6-plus": { name: "Qwen 3.6 Plus", family: "qwen" },
  "qwen3.7-max": { name: "Qwen 3.7 Max", family: "qwen" },
};

function buildModels() {
  return Object.fromEntries(
    Object.entries(KNOWN_MODELS).map(([id, info]) => [
      id,
      {
        id,
        name: info.name,
        family: info.family,
        api: { id, url: GATEWAY_URL, npm: "@ai-sdk/openai-compatible" },
        capabilities: { ...CAPABILITIES },
        limit: { ...LIMIT },
        cost: { input: COST.input, output: COST.output, cache: { ...COST.cache } },
        status: "active",
        options: {},
        headers: {},
        release_date: "",
      },
    ]),
  );
}

async function fetchModelsFromAPI() {
  const session = readSession();
  const token = session?.accessToken || readAccessTokenFromAuthJson();
  if (!token) return null;
  try {
    const res = await fetch(`${MODELS_API_URL}?page=1&size=100`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!res.ok) return null;
    const json = await res.json();
    const active = json.models?.filter((m) => m.status === 1) ?? [];
    return active.length > 0 ? active : null;
  } catch {
    return null;
  }
}

async function buildModelsDynamic() {
  const apiModels = await fetchModelsFromAPI();
  if (apiModels && apiModels.length > 0) {
    return Object.fromEntries(
      apiModels.map((m) => {
        const id = m.model;
        return [
          id,
          {
            id,
            name: m.title,
            family: "cannbot",
            api: { id, url: GATEWAY_URL, npm: "@ai-sdk/openai-compatible" },
            capabilities: { ...CAPABILITIES },
            limit: { context: m.contextLength, output: m.maxTokens },
            cost: { input: COST.input, output: COST.output, cache: { ...COST.cache } },
            status: "active",
            options: {},
            headers: {},
            release_date: "",
          },
        ];
      }),
    );
  }
  return buildModels();
}

function getMac() {
  try {
    for (const name of Object.keys(networkInterfaces())) {
      for (const iface of networkInterfaces()[name] || []) {
        if (iface.mac && iface.mac !== "00:00:00:00:00:00" && !iface.internal) return iface.mac;
      }
    }
  } catch {}
  return "00:00:00:00:00:00";
}

let cachedVKey = null;
let cachedJwt = null;

function writeAuthJson(XDG, updates) {
  try {
    const authPath = join(XDG, "opencode", "auth.json");
    let authJson = {};
    try { authJson = JSON.parse(readFileSync(authPath, "utf-8")); } catch {}
    Object.assign(authJson, updates);
    writeFileSync(authPath, JSON.stringify(authJson, null, 2) + "\n");
  } catch (e) {
    debugLog("writeAuthJson failed: " + e?.message);
  }
}

async function exchangeVkJwt(vk) {
  try {
    const mac = getMac();
    const res = await fetch("https://cannbot.hicann.cn/cannbot/api/auth/authenticate", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-vkey": vk },
      body: JSON.stringify({ type: "cli", mac }),
    });
    if (!res.ok) {
      debugLog("VK->JWT exchange failed: " + res.status);
      return null;
    }
    const json = await res.json();
    debugLog("VK->JWT exchange OK, expiresIn=" + json.expiresIn);
    return json;
  } catch (e) {
    debugLog("VK->JWT exchange error: " + e?.message);
    return null;
  }
}

export default async function (input) {
  return {
    config: async function (cfg) {
      cfg.provider = cfg.provider ?? {};
      cfg.provider[PROVIDER_ID] = {
        name: "CANNBOT",
        npm: "@ai-sdk/openai-compatible",
        options: { baseURL: GATEWAY_URL },
        models: await buildModelsDynamic(),
      };
    },

    auth: {
      provider: PROVIDER_ID,
      methods: [
        {
          type: "api",
          label: "CANNBOT Virtual Key (VK)",
          async authorize(inputs) {
            return { type: "success", key: inputs?.key ?? "" };
          },
        },
      ],
      async loader(getAuth) {
        debugLog("=== auth.loader start ===");
        const info = await getAuth();
        debugLog(`getAuth(): type=${info?.type}, hasKey=${!!info?.key}, keyPreview=${info?.key?.substring(0, 8) || "none"}`);

        let vk = null;
        if (info?.type === "api" && info.key) {
          vk = info.key;
          debugLog("Scenario A: got VK from getAuth()");
        }

        const XDG = process.env.XDG_DATA_HOME || join(homedir(), ".local", "share");

        if (!vk) {
          try {
            const authJsonPath = join(XDG, "opencode", "auth.json");
            const authJson = JSON.parse(readFileSync(authJsonPath, "utf-8"));
            const entry = authJson["cannbot-vk"] || authJson["cannbot"];
            if (entry?.type === "api" && entry.key) {
              vk = entry.key;
              debugLog("Scenario B: got VK from auth.json");
            }
          } catch (e) {
            debugLog("read auth.json failed: " + e?.message);
          }
        }

        cachedVKey = vk || null;

        if (vk) {
          let existingJwt = null;
          try {
            const authJsonPath = join(XDG, "opencode", "auth.json");
            const authJson = JSON.parse(readFileSync(authJsonPath, "utf-8"));
            const cli = authJson["cannbot-cli"];
            if (cli?.type === "oauth" && cli.access && cli.expires && cli.expires > Math.floor(Date.now() / 1000)) {
              existingJwt = cli.access;
              debugLog("Existing valid JWT found in auth.json");
            }
          } catch {}

          if (existingJwt) {
            cachedJwt = existingJwt;
          } else {
            debugLog("No valid JWT, exchanging VK...");
            const result = await exchangeVkJwt(vk);
            if (result?.accessToken) {
              cachedJwt = result.accessToken;
              writeAuthJson(XDG, {
                "cannbot-cli": {
                  type: "oauth",
                  access: result.accessToken,
                  refresh: vk,
                  expires: Math.floor(Date.now() / 1000) + (result.expiresIn || 3600),
                },
              });
              debugLog("JWT stored to auth.json as cannbot-cli");
            } else {
              cachedJwt = null;
              debugLog("VK->JWT exchange failed, no Bearer token available");
            }
          }
        }

        debugLog(`cachedVKey: ${cachedVKey ? cachedVKey.substring(0, 8) + "..." : "null"}`);
        debugLog(`cachedJwt: ${cachedJwt ? cachedJwt.substring(0, 20) + "..." : "null"}`);
        debugLog("=== auth.loader done ===");
        return {};
      },
    },

    "chat.headers": async function (input, output) {
      if (input.model.providerID !== PROVIDER_ID) return;

      if (cachedVKey) {
        output.headers["x-api-vkey"] = cachedVKey;
      }

      const session = readSession();
      const bearerToken = session?.accessToken || cachedJwt || readAccessTokenFromAuthJson();
      if (bearerToken) {
        output.headers["Authorization"] = `Bearer ${bearerToken}`;
        debugLog("chat.headers: injected Authorization: Bearer");
      }
    },
  };
};
PLUGIN_EOF

green "[1/2] Plugin installed -> $PLUGIN_FILE"

# ── 2. Update opencode.json ─────────────────────────────────────────────

PLUGIN_URI="file://$PLUGIN_FILE"

if [ -f "$OPENCODE_JSON" ]; then
  node -e "
    const fs = require('fs');
    const cfg = JSON.parse(fs.readFileSync('$OPENCODE_JSON', 'utf-8'));
    const plugins = cfg.plugin || [];
    const uri = '$PLUGIN_URI';
    if (!plugins.includes(uri)) plugins.push(uri);
    cfg.plugin = plugins;
    fs.writeFileSync('$OPENCODE_JSON', JSON.stringify(cfg, null, 2) + '\n');
  "
else
  node -e "
    const fs = require('fs');
    const cfg = {
      '\$schema': 'https://opencode.ai/config.json',
      plugin: ['$PLUGIN_URI']
    };
    fs.writeFileSync('$OPENCODE_JSON', JSON.stringify(cfg, null, 2) + '\n');
  "
fi

green "[2/2] opencode.json updated -> $OPENCODE_JSON"

echo
bold "Done! Restart opencode, then run:"
echo
echo "  /connect"
echo
echo "Select 'CANNBOT' and enter your Virtual Key (VK)."
echo "Get your VK at: https://cannbot.hicann.cn -> Settings -> API Keys"
echo
