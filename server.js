// Point AI — 3-way call bridge server.
import express from "express";
import { WebSocketServer, WebSocket } from "ws";

const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const PORT = process.env.PORT || 8080;
const BRIDGE_API_KEY = process.env.BRIDGE_API_KEY;
const POINT_START_URL = process.env.POINT_START_URL;
const POINT_PUSH_URL = process.env.POINT_PUSH_URL;

if (!BRIDGE_API_KEY || !POINT_START_URL || !POINT_PUSH_URL) {
  console.error("Missing required env vars. See .env.example.");
  process.exit(1);
}

const publicHost = (req) => process.env.PUBLIC_URL || req.headers.host;

app.post("/twiml", (req, res) => {
  res.type("xml").send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Gather numDigits="6" action="/gather" method="POST" finishOnKey="#" timeout="30">
    <Say voice="Polly.Joanna">Welcome to Point AI. Please enter your six digit PIN followed by pound.</Say>
  </Gather>
  <Say voice="Polly.Joanna">No PIN received. Goodbye.</Say>
</Response>`);
});

app.post("/gather", (req, res) => {
  const pin = req.body?.Digits || "";
  const host = publicHost(req);
  res.type("xml").send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://${host}/stream" name="ptai-bridge">
      <Parameter name="pin" value="${pin}"/>
    </Stream>
  </Connect>
</Response>`);
});

app.get("/health", (req, res) => res.json({ ok: true }));

const server = app.listen(PORT, () =>
  console.log(`Point AI bridge listening on :${PORT}`)
);
const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  if (req.url.startsWith("/stream")) return handleStream(ws, req);
  ws.close();
});

async function handleStream(ws) {
  let state = null;
  ws.on("message", async (msg) => {
    let event;
    try { event = JSON.parse(msg.toString()); } catch { return; }
    if (event.event === "start") {
      const pin = event.start?.customParameters?.pin || "";
      try {
        const r = await fetch(POINT_START_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-bridge-key": BRIDGE_API_KEY },
          body: JSON.stringify({ pin }),
        });
        const s = await r.json();
        if (!r.ok || !s.sessionId || !s.deepgramUrl || !s.deepgramToken) {
          console.error("Session claim failed:", s);
          try { ws.close(); } catch {}
          return;
        }
        const dg = new WebSocket(s.deepgramUrl, ["token", s.deepgramToken]);
        state = { dg, sessionId: s.sessionId, ownerId: s.ownerId, seq: 0 };
        dg.on("open", () => console.log(`Deepgram open — session ${state.sessionId}`));
        dg.on("error", (e) => console.error("Deepgram error:", e.message));
        dg.on("message", async (dgMsg) => {
          let rj;
          try { rj = JSON.parse(dgMsg.toString()); } catch { return; }
          if (rj.type !== "Results" || !rj.is_final) return;
          const alt = rj.channel?.alternatives?.[0];
          const text = alt?.transcript?.trim();
          if (!text) return;
          const spk = alt?.words?.[0]?.speaker ?? 0;
          const speaker = spk === 0 ? "agent" : "client";
          state.seq += 1;
          try {
            await fetch(POINT_PUSH_URL, {
              method: "POST",
              headers: { "Content-Type": "application/json", "x-bridge-key": BRIDGE_API_KEY },
              body: JSON.stringify({ sessionId: state.sessionId, ownerId: state.ownerId, speaker, text, seq: state.seq }),
            });
          } catch (e) { console.error("Push failed:", e.message); }
        });
      } catch (e) {
        console.error("Claim error:", e.message);
        try { ws.close(); } catch {}
      }
      return;
    }
    if (event.event === "media") {
      if (state?.dg?.readyState === WebSocket.OPEN) {
        state.dg.send(Buffer.from(event.media.payload, "base64"));
      }
      return;
    }
    if (event.event === "stop") {
      if (state?.dg) { try { state.dg.close(); } catch {} }
    }
  });
  ws.on("close", () => {
    if (state?.dg) { try { state.dg.close(); } catch {} }
  });
}
