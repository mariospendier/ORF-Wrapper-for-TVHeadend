#!/usr/bin/env python3
# save this file to /usr/local/bin/orf_vlc_wrapper.py
import os
import sys
import time
import signal
import socket
import logging
import threading
import subprocess
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Dict, Optional, Set, Tuple

# -------------------------
# Config
# -------------------------

BIND_HOST = "0.0.0.0"
BIND_PORT = 8800

LOG_FILE = "/var/log/orf_vlc_wrapper.log"

# If False: log lines will be "YYYY-mm-dd HH:MM:SS,ms <message>" (no INFO/ERROR)
# If True:  log lines will include level "INFO/ERROR"
LOG_INCLUDE_LEVEL = False

# Keep Python internal logging level at INFO; errors still appear as ERROR.
LOG_LEVEL = logging.INFO

# TVHeadend base stream (pass profile)
TVH_BASE = "http://127.0.0.1:9981/stream/service"
TVH_PROFILE = "pass"

# VLC
VLC_BIN = "/usr/bin/cvlc"
VLC_PLUGIN_PATH = "/usr/lib/x86_64-linux-gnu/vlc/plugins"

# Stop VLC after this many seconds without any client connected to that endpoint.
IDLE_STOP_SECONDS = 20

# If we don't get bytes from VLC for this long while clients exist, restart VLC.
STALL_SECONDS = 15

# Socket read chunk size from VLC stdout
READ_CHUNK = 188 * 50  # ~ 9.4KB

# ORF2 service UUIDs
SERVICES = {
    "/orf2k.ts": ("ORF2K HD", "62b2d62afef48b374003b91d03341959"),
    "/orf2st.ts": ("ORF2St HD", "1c991f2fcf9ec1e6f25de43ec84c91bc"),
    "/orf2b.ts": ("ORF2B HD", "0e86eecf524585e6fe8bb27d9bbe05ec"),
    "/orf2o.ts": ("ORF2O HD", "a02a4f4168239be22b4e2070c4027cc3"),
    "/orf2s.ts": ("ORF2S HD", "c8187e84cfc1af8bcd94a2f5913d61b7"),
    "/orf2t.ts": ("ORF2T HD", "be6e1693dc15631ed4194a3ff76e4e3f"),
    "/orf2v.ts": ("ORF2V HD", "e1d798361dee619fc2a7a28d0946a8cb"),
    "/orf2n.ts": ("ORF2N HD", "920b04e52ac34b3b0d99775420d9446e"),
    "/orf2w.ts": ("ORF2W HD", "10ef17c5bcc0c9e44a22cd673d1facd4"),
}

# -------------------------
# Globals
# -------------------------

SHUTDOWN_EVENT = threading.Event()

# -------------------------
# Logging
# -------------------------

logger = logging.getLogger("orf_vlc_wrapper")
logger.setLevel(LOG_LEVEL)

if LOG_INCLUDE_LEVEL:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
else:
    fmt = logging.Formatter("%(asctime)s %(message)s")

fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(fmt)
logger.addHandler(fh)

# keep console output minimal; can be removed if you want file only
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
logger.addHandler(sh)


# -------------------------
# HTTP server class
# -------------------------

class DaemonThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


# -------------------------
# VLC Stream Worker
# -------------------------

class VLCStream:
    """
    One on-demand VLC pipeline per endpoint.
    Multiple HTTP clients can subscribe; VLC starts when first client arrives.
    VLC stops after idle timeout.
    """

    def __init__(self, path: str, service_name: str, service_uuid: str):
        self.path = path
        self.service_name = service_name
        self.service_uuid = service_uuid

        self._lock = threading.Lock()
        self._clients: Set[socket.socket] = set()

        self._proc: Optional[subprocess.Popen] = None
        self._reader_thread: Optional[threading.Thread] = None

        self._last_client_left_ts = 0.0
        self._last_bytes_ts = 0.0

    def tvh_url(self) -> str:
        return f"{TVH_BASE}/{self.service_uuid}?profile={TVH_PROFILE}"

    def _build_vlc_cmd(self) -> Tuple[Dict[str, str], list]:
        env = os.environ.copy()
        env["VLC_PLUGIN_PATH"] = VLC_PLUGIN_PATH

        # VLC: input TS -> remux TS to stdout (dst=-)
        cmd = [
            VLC_BIN,
            "-I", "dummy",
            "--demux", "ts",
            "--no-video-title-show",
            "--quiet",
            "--network-caching=150",
            self.tvh_url(),
            "--sout", "#standard{access=file,mux=ts,dst=-}",
            "--sout-keep",
        ]
        return env, cmd

    def _start_vlc_locked(self) -> None:
        if SHUTDOWN_EVENT.is_set():
            return
        if self._proc and self._proc.poll() is None:
            return

        env, cmd = self._build_vlc_cmd()
        try:
            # start_new_session=True -> own process group, so we can kill the whole group
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                bufsize=0,
                start_new_session=True,
            )
            self._last_bytes_ts = time.time()
            logger.info("%s starting VLC for %s (%s)", self.path, self.service_name, self.service_uuid)

            self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
            self._reader_thread.start()

            threading.Thread(target=self._stderr_drain, daemon=True).start()

        except Exception as e:
            logger.error("%s failed to start VLC: %s", self.path, e)
            self._proc = None

    def _stop_vlc_locked(self) -> None:
        proc = self._proc
        if not proc:
            return
        if proc.poll() is not None:
            self._proc = None
            return

        logger.info("%s stopping VLC (idle or shutdown)", self.path)

        try:
            # kill whole process group
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except Exception:
                proc.terminate()

            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    proc.kill()
        except Exception as e:
            logger.error("%s error stopping VLC: %s", self.path, e)
        finally:
            self._proc = None

    def close_all_clients(self) -> None:
        with self._lock:
            clients = list(self._clients)
            self._clients.clear()

        for c in clients:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                c.close()
            except Exception:
                pass

    def add_client(self, sock: socket.socket) -> None:
        with self._lock:
            self._clients.add(sock)
            self._last_client_left_ts = 0.0
            self._start_vlc_locked()

    def remove_client(self, sock: socket.socket) -> None:
        with self._lock:
            if sock in self._clients:
                self._clients.remove(sock)
            if not self._clients:
                self._last_client_left_ts = time.time()

    def _broadcast(self, data: bytes) -> None:
        dead = []
        with self._lock:
            for c in list(self._clients):
                try:
                    c.sendall(data)
                except Exception:
                    dead.append(c)

            for c in dead:
                try:
                    self._clients.remove(c)
                except KeyError:
                    pass
                try:
                    c.close()
                except Exception:
                    pass

            if not self._clients and self._last_client_left_ts == 0.0:
                self._last_client_left_ts = time.time()

    def _reader_loop(self) -> None:
        while not SHUTDOWN_EVENT.is_set():
            with self._lock:
                proc = self._proc
                clients = len(self._clients)
                last_left = self._last_client_left_ts

            if not proc:
                return

            # idle stop
            if clients == 0 and last_left and (time.time() - last_left) >= IDLE_STOP_SECONDS:
                with self._lock:
                    self._stop_vlc_locked()
                return

            if clients == 0:
                time.sleep(0.2)
                continue

            try:
                chunk = proc.stdout.read(READ_CHUNK) if proc.stdout else b""
            except Exception:
                chunk = b""

            if chunk:
                self._last_bytes_ts = time.time()
                self._broadcast(chunk)
                continue

            # no bytes
            if proc.poll() is not None:
                rc = proc.returncode
                # rc=0 can be normal (stop path), so keep it INFO
                if rc == 0:
                    logger.info("%s VLC exited rc=0", self.path)
                else:
                    logger.error("%s VLC exited rc=%s", self.path, rc)
                with self._lock:
                    self._proc = None
                return

            # stall -> restart
            if (time.time() - self._last_bytes_ts) > STALL_SECONDS:
                logger.error("%s VLC stalled, restarting", self.path)
                with self._lock:
                    self._stop_vlc_locked()
                    if self._clients and not SHUTDOWN_EVENT.is_set():
                        self._start_vlc_locked()
                time.sleep(0.2)
            else:
                time.sleep(0.05)

    def _stderr_drain(self) -> None:
        with self._lock:
            proc = self._proc
        if not proc or not proc.stderr:
            return
        try:
            for line in iter(proc.stderr.readline, b""):
                if SHUTDOWN_EVENT.is_set():
                    break
                if not line:
                    break
                msg = line.decode("utf-8", errors="replace").rstrip()
                if msg:
                    # Treat VLC stderr as ERROR because you asked for error-level only
                    logger.error("%s vlc[%s] %s", self.path, proc.pid, msg)
                with self._lock:
                    if self._proc != proc:
                        break
        except Exception:
            return


# -------------------------
# HTTP Handler
# -------------------------

STREAMS: Dict[str, VLCStream] = {
    path: VLCStream(path, name, uuid)
    for path, (name, uuid) in SERVICES.items()
}

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path not in STREAMS:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found\n")
            return

        stream = STREAMS[self.path]

        self.send_response(200)
        self.send_header("Content-Type", "video/MP2T")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()

        sock = self.connection
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(1.0)  # critical: do not block forever

        stream.add_client(sock)
        try:
            while not SHUTDOWN_EVENT.is_set():
                try:
                    data = sock.recv(1)
                    if not data:
                        break
                except socket.timeout:
                    continue
        except Exception:
            pass
        finally:
            stream.remove_client(sock)
            try:
                sock.close()
            except Exception:
                pass

    def log_message(self, format, *args):
        return


# -------------------------
# Main
# -------------------------

def main():
    logger.info("Starting orf_vlc_wrapper on %s:%d", BIND_HOST, BIND_PORT)
    httpd = DaemonThreadingHTTPServer((BIND_HOST, BIND_PORT), Handler)

    def _shutdown(signum, frame):
        if SHUTDOWN_EVENT.is_set():
            return
        SHUTDOWN_EVENT.set()
        logger.info("Shutdown requested")

        # stop accepting new connections
        try:
            httpd.shutdown()
            httpd.server_close()
        except Exception:
            pass

        # close clients + stop vlc
        for s in STREAMS.values():
            try:
                s.close_all_clients()
            except Exception:
                pass
            with s._lock:
                s._stop_vlc_locked()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        _shutdown(signal.SIGINT, None)

if __name__ == "__main__":
    main()
