#!/usr/bin/env python3
import os
import time
import signal
import logging
import threading
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Optional, List

TVH_BASE = "http://127.0.0.1:9981/stream/service"
PROFILE = "pass"

LOG_PATH = "/var/log/orf_wrapper.log"
BIND_ADDR = os.environ.get("ORF2_BIND", "0.0.0.0")
BIND_PORT = int(os.environ.get("ORF2_PORT", "8800"))

RESTART_BACKOFF_SEC = 1.0
STALL_TIMEOUT_SEC = 6.0
CHUNK_SIZE = 188 * 50

SERVICE_MAP: Dict[str, str] = {
    "/orf2k.ts":  "62b2d62afef48b374003b91d03341959",
    "/orf2st.ts": "1c991f2fcf9ec1e6f25de43ec84c91bc",
    "/orf2b.ts":  "0e86eecf524585e6fe8bb27d9bbe05ec",
    "/orf2o.ts":  "a02a4f4168239be22b4e2070c4027cc3",
    "/orf2s.ts":  "c8187e84cfc1af8bcd94a2f5913d61b7",
    "/orf2t.ts":  "be6e1693dc15631ed4194a3ff76e4e3f",
    "/orf2v.ts":  "e1d798361dee619fc2a7a28d0946a8cb",
    "/orf2n.ts":  "920b04e52ac34b3b0d99775420d9446e",
    "/orf2w.ts":  "10ef17c5bcc0c9e44a22cd673d1facd4",
}

def setup_logging() -> logging.Logger:
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    logger = logging.getLogger("orf_wrapper")
    logger.setLevel(logging.ERROR)
    if not logger.handlers:
        fh = logging.FileHandler(LOG_PATH)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger

LOGGER = setup_logging()

def build_ffmpeg_cmd(service_uuid: str) -> List[str]:
    input_url = f"{TVH_BASE}/{service_uuid}?profile={PROFILE}"

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-fflags", "+genpts+igndts+discardcorrupt",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-probesize", "2M",
        "-analyzeduration", "2000000",
        "-i", input_url,

        # Dynamisches Mapping – robust gegen ORF-PMT/PID Änderungen
        "-map", "0:v?",
        "-map", "0:a?",

        "-sn", "-dn",
        "-c", "copy",
        "-mpegts_flags", "+resend_headers",
        "-pat_period", "0.2",
        "-f", "mpegts",
        "pipe:1",
    ]

    return cmd

class EndpointStreamer:
    def __init__(self, path: str, service_uuid: str):
        self.path = path
        self.service_uuid = service_uuid
        self._lock = threading.Lock()
        self._clients: Dict[int, "ClientSink"] = {}
        self._next_client_id = 1
        self._worker_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def add_client(self, sink: "ClientSink") -> int:
        with self._lock:
            cid = self._next_client_id
            self._next_client_id += 1
            self._clients[cid] = sink

            if self._worker_thread is None or not self._worker_thread.is_alive():
                self._stop_event.clear()
                self._worker_thread = threading.Thread(target=self._run, daemon=True)
                self._worker_thread.start()

            return cid

    def remove_client(self, client_id: int) -> None:
        with self._lock:
            self._clients.pop(client_id, None)
            if not self._clients:
                self._stop_event.set()

    def _broadcast(self, data: bytes) -> None:
        dead = []
        with self._lock:
            for cid, sink in self._clients.items():
                if not sink.write(data):
                    dead.append(cid)
        for cid in dead:
            self.remove_client(cid)

    def _run(self):
        last_bytes_time = time.time()

        while not self._stop_event.is_set():
            cmd = build_ffmpeg_cmd(self.service_uuid)

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                text=False,
            )

            stop_stderr = threading.Event()
            t = threading.Thread(target=self._stderr_to_log, args=(proc, stop_stderr), daemon=True)
            t.start()

            try:
                while not self._stop_event.is_set():
                    chunk = proc.stdout.read(CHUNK_SIZE)
                    if chunk:
                        last_bytes_time = time.time()
                        self._broadcast(chunk)
                    else:
                        if proc.poll() is not None:
                            break
                        if time.time() - last_bytes_time > STALL_TIMEOUT_SEC:
                            LOGGER.error(f"{self.path}: ffmpeg stalled, restarting")
                            break
                        time.sleep(0.05)
            finally:
                try: proc.send_signal(signal.SIGTERM)
                except: pass
                try: proc.wait(timeout=2)
                except:
                    try: proc.kill()
                    except: pass
                stop_stderr.set()

            with self._lock:
                if not self._clients:
                    break

            time.sleep(RESTART_BACKOFF_SEC)

        with self._lock:
            self._worker_thread = None

    def _stderr_to_log(self, proc, stop_event):
        try:
            while not stop_event.is_set():
                line = proc.stderr.readline()
                if not line:
                    break
                s = line.decode("utf-8", errors="replace").rstrip()
                if s:
                    LOGGER.error(f"{self.path} ffmpeg[{proc.pid}] {s}")
        except Exception as e:
            LOGGER.error(f"{self.path}: stderr logger exception: {e}")

class ClientSink:
    def __init__(self, handler: BaseHTTPRequestHandler):
        self._handler = handler
        self._dead = False
        self._lock = threading.Lock()

    def write(self, data: bytes) -> bool:
        if self._dead:
            return False
        with self._lock:
            if self._dead:
                return False
            try:
                self._handler.wfile.write(data)
                self._handler.wfile.flush()
                return True
            except:
                self._dead = True
                return False

STREAMERS = {path: EndpointStreamer(path, uuid) for path, uuid in SERVICE_MAP.items()}

class OrfHandler(BaseHTTPRequestHandler):
    server_version = "orf-wrapper/5.0"

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self._handle_index()
            return

        if self.path not in STREAMERS:
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "video/MP2T")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        streamer = STREAMERS[self.path]
        sink = ClientSink(self)
        cid = streamer.add_client(sink)

        try:
            while not sink._dead:
                time.sleep(0.5)
        finally:
            streamer.remove_client(cid)

    def _handle_index(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        lines = ["Available endpoints:"]
        for p in sorted(STREAMERS.keys()):
            lines.append(f"  {p}")
        self.wfile.write(("\n".join(lines) + "\n").encode("utf-8"))

    def log_message(self, fmt, *args):
        return

def main():
    LOGGER.error(f"Starting ORF wrapper on {BIND_ADDR}:{BIND_PORT} (log: {LOG_PATH})")
    srv = ThreadingHTTPServer((BIND_ADDR, BIND_PORT), OrfHandler)
    srv.serve_forever()

if __name__ == "__main__":
    main()
