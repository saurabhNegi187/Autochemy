"""
HPC backend for AutoChemy's Job Manager.

No GUI code lives here, so everything can be tested on its own.

Pieces
------
* ``ClusterProfile`` / ``ProfileStore``  - any user can add their own cluster
  (host, user, key, scheduler, remote folder ...). Saved to
  ``AutoChemy_User_Data/hpc_profiles.json``. Passwords / OTPs are never saved.
* ``SSHConnection``   - paramiko based SSH + SFTP. Auth order: SSH key file,
  ssh-agent, default ~/.ssh keys, keyboard-interactive (password + OTP/2FA),
  password. Unknown host keys are confirmed once and remembered; a *changed*
  host key is refused.
* ``LocalConnection`` - same interface but runs on this machine. Useful when
  AutoChemy itself runs on a login node, and for tests.
* Scheduler adapters  - SLURM, PBS (Torque / PBS Pro / OpenPBS), SGE, LSF and
  "Custom" (user supplied commands).
* ``JobStore``        - submitted job history in
  ``AutoChemy_User_Data/hpc_job_history.json``.
* ``submit_job`` / ``refresh_jobs`` / ``cancel_job`` / ``list_remote_dir`` /
  ``read_remote_tail`` / ``download_paths`` - the operations the UI uses.
"""
from __future__ import annotations

import base64
import datetime as _dt
import hashlib
import json
import os
import posixpath
import re
import shlex
import shutil
import socket
import stat as _stat
import subprocess
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USER_DATA_DIR = os.path.join(_BASE_DIR, "AutoChemy_User_Data")
PROFILES_PATH = os.path.join(USER_DATA_DIR, "hpc_profiles.json")
JOB_HISTORY_PATH = os.path.join(USER_DATA_DIR, "hpc_job_history.json")
KNOWN_HOSTS_PATH = os.path.join(USER_DATA_DIR, "hpc_known_hosts")
STAGING_DIR = os.path.join(USER_DATA_DIR, "hpc_staging")

SCHEDULERS = ("SLURM", "PBS", "SGE", "LSF", "Custom")

# Normalised job states shown in the UI
QUEUED = "Queued"
HELD = "Held"
RUNNING = "Running"
COMPLETED = "Completed"
FAILED = "Failed"
CANCELLED = "Cancelled"
FINISHED = "Finished"      # left the queue, exit status unknown
UNKNOWN = "Unknown"
SUBMITTING = "Submitting"
TERMINAL_STATES = {COMPLETED, FAILED, CANCELLED, FINISHED}

ORCA_SUCCESS_MARKER = "ORCA TERMINATED NORMALLY"


class HPCError(Exception):
    """Any error the UI should show to the user as a plain message."""


class HostKeyChanged(HPCError):
    pass


class AuthCancelled(HPCError):
    pass


def _now() -> float:
    return time.time()


def fmt_time(ts: Optional[float]) -> str:
    if not ts:
        return ""
    return _dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def _atomic_write_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def _read_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def safe_name(text: str, fallback: str = "job") -> str:
    """Folder/file friendly name: letters, digits, dot, dash, underscore."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", (text or "").strip()).strip("._")
    return cleaned[:80] or fallback


# --------------------------------------------------------------------------
# Cluster profiles
# --------------------------------------------------------------------------

@dataclass
class ClusterProfile:
    name: str
    host: str = ""
    username: str = ""
    port: int = 22
    key_path: str = ""
    scheduler: str = "SLURM"
    remote_base_dir: str = "~/autochemy_jobs"
    # Extra text added before every scheduler command, e.g.
    # "source /etc/profile; module load slurm"
    pre_command: str = ""
    # Only for scheduler == "Custom". Placeholders: {script} {job_id} {dir}
    submit_cmd: str = ""
    status_cmd: str = ""
    cancel_cmd: str = ""
    job_id_regex: str = r"(\d+(?:\.[\w.-]+)?)"
    # "ssh" = connect over SSH, "local" = run on this computer
    connection: str = "ssh"
    notes: str = ""

    def validate(self) -> List[str]:
        problems = []
        if not self.name.strip():
            problems.append("Give the cluster a name.")
        if self.connection == "ssh":
            if not self.host.strip():
                problems.append("Host (address of the HPC login node) is required.")
            if not self.username.strip():
                problems.append("Username is required.")
            if not (1 <= int(self.port or 0) <= 65535):
                problems.append("Port must be between 1 and 65535.")
        if self.scheduler not in SCHEDULERS:
            problems.append(f"Scheduler must be one of: {', '.join(SCHEDULERS)}.")
        if self.scheduler == "Custom":
            if "{script}" not in self.submit_cmd:
                problems.append("Custom submit command must contain {script}.")
            if "{job_id}" not in self.status_cmd:
                problems.append("Custom status command must contain {job_id}.")
        if not self.remote_base_dir.strip():
            problems.append("Remote job folder is required.")
        return problems

    def label(self) -> str:
        if self.connection == "local":
            return f"{self.name}  (this computer, {self.scheduler})"
        return f"{self.name}  ({self.username}@{self.host}, {self.scheduler})"

    @classmethod
    def from_dict(cls, data: dict) -> "ClusterProfile":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        clean = {k: v for k, v in (data or {}).items() if k in known}
        # never load secrets even if someone hand-edited the file
        prof = cls(**clean)
        try:
            prof.port = int(prof.port or 22)
        except (TypeError, ValueError):
            prof.port = 22
        return prof

    def to_dict(self) -> dict:
        return asdict(self)


class ProfileStore:
    def __init__(self, path: str = PROFILES_PATH):
        self.path = path
        self._lock = threading.RLock()
        self.profiles: Dict[str, ClusterProfile] = {}
        self.load()

    def load(self) -> None:
        with self._lock:
            raw = _read_json(self.path, {})
            items = raw.get("profiles", raw) if isinstance(raw, dict) else {}
            self.profiles = {}
            if isinstance(items, dict):
                for name, data in items.items():
                    if isinstance(data, dict):
                        data = dict(data)
                        data.setdefault("name", name)
                        prof = ClusterProfile.from_dict(data)
                        self.profiles[prof.name] = prof

    def save(self) -> None:
        with self._lock:
            payload = {
                "version": 1,
                "profiles": {n: p.to_dict() for n, p in sorted(self.profiles.items())},
            }
            _atomic_write_json(self.path, payload)

    def names(self) -> List[str]:
        with self._lock:
            return sorted(self.profiles)

    def get(self, name: str) -> Optional[ClusterProfile]:
        with self._lock:
            return self.profiles.get(name)

    def upsert(self, profile: ClusterProfile, old_name: Optional[str] = None) -> None:
        problems = profile.validate()
        if problems:
            raise HPCError("\n".join(problems))
        with self._lock:
            if old_name and old_name != profile.name:
                self.profiles.pop(old_name, None)
            self.profiles[profile.name] = profile
            self.save()

    def delete(self, name: str) -> None:
        with self._lock:
            self.profiles.pop(name, None)
            self.save()

    def find(self, host: str, username: str) -> Optional[ClusterProfile]:
        host, username = (host or "").strip().lower(), (username or "").strip()
        with self._lock:
            for p in self.profiles.values():
                if p.host.strip().lower() == host and p.username.strip() == username:
                    return p
        return None


# --------------------------------------------------------------------------
# Connections
# --------------------------------------------------------------------------

@dataclass
class RemoteEntry:
    name: str
    path: str
    is_dir: bool
    size: int = 0
    mtime: float = 0.0


# prompt(text, secret) -> str  (None/"" means the user cancelled)
PromptFn = Callable[[str, bool], Optional[str]]
# confirm(text) -> bool
ConfirmFn = Callable[[str], bool]


class Connection:
    """Interface shared by SSHConnection and LocalConnection."""

    def run(self, command: str, timeout: float = 120) -> Tuple[int, str, str]:
        raise NotImplementedError

    def home(self) -> str:
        raise NotImplementedError

    def mkdir_p(self, path: str) -> None:
        rc, _out, err = self.run(f"mkdir -p {shlex.quote(path)}")
        if rc != 0:
            raise HPCError(f"Could not create folder {path}:\n{err.strip()}")

    def put_bytes(self, remote_path: str, data: bytes, mode: Optional[int] = None) -> None:
        raise NotImplementedError

    def listdir(self, path: str) -> List[RemoteEntry]:
        raise NotImplementedError

    def read_tail(self, path: str, max_bytes: int) -> Tuple[bytes, int]:
        """Return (last max_bytes of file, total size)."""
        raise NotImplementedError

    def get_file(self, remote_path: str, local_path: str,
                 progress: Optional[Callable[[int, int], None]] = None) -> None:
        raise NotImplementedError

    def is_alive(self) -> bool:
        return True

    def close(self) -> None:
        pass

    # helpers -------------------------------------------------------------
    def expand(self, path: str) -> str:
        path = (path or "").strip()
        if path == "~":
            return self.home()
        if path.startswith("~/"):
            return posixpath.join(self.home(), path[2:])
        return path


class LocalConnection(Connection):
    """Runs everything on this computer (login-node use, and tests)."""

    def __init__(self, env: Optional[Dict[str, str]] = None):
        self.env = env

    def run(self, command: str, timeout: float = 120) -> Tuple[int, str, str]:
        bash = shutil.which("bash") or "/bin/sh"
        try:
            proc = subprocess.run(
                [bash, "-lc", command] if bash.endswith("bash") else [bash, "-c", command],
                capture_output=True, text=True, timeout=timeout,
                env=self.env,
            )
        except subprocess.TimeoutExpired:
            return 124, "", f"Command timed out after {timeout:.0f}s"
        return proc.returncode, proc.stdout, proc.stderr

    def home(self) -> str:
        return os.path.expanduser("~")

    def mkdir_p(self, path: str) -> None:
        os.makedirs(self.expand(path), exist_ok=True)

    def put_bytes(self, remote_path: str, data: bytes, mode: Optional[int] = None) -> None:
        remote_path = self.expand(remote_path)
        os.makedirs(os.path.dirname(remote_path) or ".", exist_ok=True)
        with open(remote_path, "wb") as fh:
            fh.write(data)
        if mode is not None:
            os.chmod(remote_path, mode)

    def listdir(self, path: str) -> List[RemoteEntry]:
        path = self.expand(path)
        out = []
        for name in os.listdir(path):
            full = os.path.join(path, name)
            try:
                st = os.stat(full)
            except OSError:
                continue
            out.append(RemoteEntry(name, full, _stat.S_ISDIR(st.st_mode), st.st_size, st.st_mtime))
        return out

    def read_tail(self, path: str, max_bytes: int) -> Tuple[bytes, int]:
        path = self.expand(path)
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size > max_bytes:
                fh.seek(size - max_bytes)
            return fh.read(), size

    def get_file(self, remote_path, local_path, progress=None) -> None:
        remote_path = self.expand(remote_path)
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        shutil.copy2(remote_path, local_path)
        if progress:
            size = os.path.getsize(local_path)
            progress(size, size)


def _paramiko():
    try:
        import paramiko  # type: ignore
        return paramiko
    except Exception as exc:  # pragma: no cover - depends on install
        raise HPCError(
            "The 'paramiko' package is needed for HPC connections.\n"
            "Install it with:  python -m pip install paramiko\n"
            f"(details: {exc})"
        )


def host_key_fingerprint(key) -> str:
    digest = hashlib.sha256(key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode().rstrip("=")


class SSHConnection(Connection):
    """SSH + SFTP over one paramiko Transport (one login, many operations)."""

    def __init__(self, profile: ClusterProfile, prompt: Optional[PromptFn] = None,
                 confirm: Optional[ConfirmFn] = None, known_hosts_path: str = KNOWN_HOSTS_PATH):
        self.profile = profile
        self.prompt = prompt
        self.confirm = confirm
        self.known_hosts_path = known_hosts_path
        self._transport = None
        self._sftp = None
        self._home: Optional[str] = None
        self._lock = threading.RLock()

    # -- connect -----------------------------------------------------------
    def connect(self) -> "SSHConnection":
        paramiko = _paramiko()
        p = self.profile
        host, port, user = p.host.strip(), int(p.port or 22), p.username.strip()
        try:
            sock = socket.create_connection((host, port), timeout=20)
        except OSError as exc:
            raise HPCError(
                f"Cannot reach {host}:{port} ({exc}).\n"
                "Check the address, your network, and whether a VPN is needed."
            )
        transport = paramiko.Transport(sock)
        transport.banner_timeout = 30
        try:
            transport.start_client(timeout=30)
            self._verify_host_key(transport, host, port)
            self._authenticate(transport, user)
        except Exception:
            transport.close()
            raise
        transport.set_keepalive(30)
        self._transport = transport
        self._sftp = paramiko.SFTPClient.from_transport(transport)
        return self

    def _host_id(self, host: str, port: int) -> str:
        return host if port == 22 else f"[{host}]:{port}"

    def _verify_host_key(self, transport, host: str, port: int) -> None:
        paramiko = _paramiko()
        key = transport.get_remote_server_key()
        host_id = self._host_id(host, port)
        stores = []
        sys_known = os.path.expanduser("~/.ssh/known_hosts")
        for path in (sys_known, self.known_hosts_path):
            if os.path.isfile(path):
                try:
                    stores.append(paramiko.HostKeys(path))
                except Exception:
                    pass
        known_any = False
        for hk in stores:
            entry = hk.lookup(host_id)
            if not entry:
                continue
            known_any = True
            stored = entry.get(key.get_name())
            if stored is not None:
                if stored.asbytes() == key.asbytes():
                    return
                raise HostKeyChanged(
                    f"The host key for {host_id} has CHANGED.\n"
                    f"New fingerprint: {host_key_fingerprint(key)}\n\n"
                    "This can mean the cluster was reinstalled, or that someone is "
                    "intercepting the connection. Confirm with your HPC admins, then "
                    "use 'Forget host key' on this cluster to trust the new key."
                )
        # Not known yet (or only other key types known): trust on first use.
        msg = (
            f"First connection to {host_id}.\n"
            f"Host key ({key.get_name()}): {host_key_fingerprint(key)}\n\n"
            "Trust this host and remember its key?"
        )
        if known_any:
            msg = msg.replace("First connection to", "New key type offered by")
        if self.confirm is not None and not self.confirm(msg):
            raise AuthCancelled("Host key not trusted - connection cancelled.")
        hk = paramiko.HostKeys()
        if os.path.isfile(self.known_hosts_path):
            try:
                hk.load(self.known_hosts_path)
            except Exception:
                pass
        hk.add(host_id, key.get_name(), key)
        os.makedirs(os.path.dirname(self.known_hosts_path), exist_ok=True)
        hk.save(self.known_hosts_path)

    def _ask(self, text: str, secret: bool) -> str:
        if self.prompt is None:
            raise AuthCancelled("A password/OTP is required but no prompt is available.")
        answer = self.prompt(text, secret)
        if answer is None:
            raise AuthCancelled("Login cancelled.")
        return answer

    def _load_key(self, path: str):
        paramiko = _paramiko()
        key_classes = [getattr(paramiko, n) for n in
                       ("Ed25519Key", "ECDSAKey", "RSAKey", "DSSKey") if hasattr(paramiko, n)]
        passphrase = None
        last_error = None
        for attempt in range(2):
            for cls in key_classes:
                try:
                    return cls.from_private_key_file(path, password=passphrase)
                except paramiko.PasswordRequiredException:
                    if attempt == 0:
                        passphrase = self._ask(f"Passphrase for key {os.path.basename(path)}:", True)
                        break
                except Exception as exc:
                    last_error = exc
            else:
                break
        raise HPCError(f"Could not read SSH key {path}: {last_error}. "
                       "If it is a PuTTY .ppk key, export it to OpenSSH format first.")

    def _authenticate(self, transport, user: str) -> None:
        paramiko = _paramiko()
        allowed: Optional[List[str]] = None

        def try_key(pkey) -> None:
            nonlocal allowed
            try:
                remaining = transport.auth_publickey(user, pkey)
                if remaining:
                    allowed = list(remaining)   # partial success (key + OTP)
            except paramiko.BadAuthenticationType as exc:
                allowed = list(exc.allowed_types)
            except paramiko.AuthenticationException:
                pass

        key_path = os.path.expanduser(self.profile.key_path.strip()) if self.profile.key_path else ""
        if key_path:
            if not os.path.isfile(key_path):
                raise HPCError(f"SSH key file not found: {key_path}")
            try_key(self._load_key(key_path))
        if not transport.is_authenticated() and (allowed is None or "publickey" in allowed):
            try:
                for agent_key in paramiko.Agent().get_keys():
                    try_key(agent_key)
                    if transport.is_authenticated() or allowed:
                        break
            except Exception:
                pass
        if not transport.is_authenticated() and not key_path and (allowed is None or "publickey" in allowed):
            for fname in ("id_ed25519", "id_ecdsa", "id_rsa"):
                path = os.path.expanduser(f"~/.ssh/{fname}")
                if os.path.isfile(path):
                    try:
                        try_key(self._load_key(path))
                    except HPCError:
                        continue
                    if transport.is_authenticated() or allowed:
                        break

        if transport.is_authenticated():
            return

        if allowed is None:
            try:
                transport.auth_none(user)
            except paramiko.BadAuthenticationType as exc:
                allowed = list(exc.allowed_types)
            except Exception:
                allowed = ["keyboard-interactive", "password"]
            if transport.is_authenticated():
                return

        if "keyboard-interactive" in allowed:
            def handler(title, instructions, prompt_list):
                answers = []
                for text, echo in prompt_list:
                    header = "\n".join(x for x in (title, instructions) if x)
                    question = (f"{header}\n{text}" if header else text).strip() or "Password:"
                    answers.append(self._ask(question, not echo))
                return answers
            try:
                transport.auth_interactive(user, handler)
            except paramiko.AuthenticationException as exc:
                if transport.is_authenticated():
                    return
                if "password" not in allowed:
                    raise HPCError(f"Login failed for {user}: {exc}")
            if transport.is_authenticated():
                return

        if "password" in allowed:
            pw = self._ask(f"Password for {user}@{self.profile.host}:", True)
            try:
                remaining = transport.auth_password(user, pw)
            except paramiko.AuthenticationException as exc:
                raise HPCError(f"Login failed for {user}: {exc}")
            if transport.is_authenticated():
                return
            if remaining and "keyboard-interactive" in remaining:
                transport.auth_interactive(user, lambda t, i, pl: [self._ask(p, not e) for p, e in pl])
                if transport.is_authenticated():
                    return

        raise HPCError(
            f"Could not log in as {user}. Server accepts: {', '.join(allowed or []) or 'unknown'}.\n"
            "Check the username and SSH key, or that your password/OTP is correct."
        )

    # -- operations --------------------------------------------------------
    def is_alive(self) -> bool:
        t = self._transport
        return bool(t is not None and t.is_active() and t.is_authenticated())

    def close(self) -> None:
        with self._lock:
            for obj in (self._sftp, self._transport):
                try:
                    if obj is not None:
                        obj.close()
                except Exception:
                    pass
            self._sftp = None
            self._transport = None

    def _need(self):
        if not self.is_alive():
            raise HPCError("Not connected (the SSH session ended). Connect again.")

    def run(self, command: str, timeout: float = 120) -> Tuple[int, str, str]:
        self._need()
        chan = self._transport.open_session()
        try:
            chan.settimeout(timeout)
            # login shell so module systems / scheduler PATHs are loaded
            chan.exec_command("bash -lc " + shlex.quote(command))
            out, err = bytearray(), bytearray()
            deadline = _now() + timeout
            while True:
                progressed = False
                while chan.recv_ready():
                    out += chan.recv(65536)
                    progressed = True
                while chan.recv_stderr_ready():
                    err += chan.recv_stderr(65536)
                    progressed = True
                if chan.exit_status_ready() and not chan.recv_ready() and not chan.recv_stderr_ready():
                    break
                if _now() > deadline:
                    return 124, out.decode("utf-8", "replace"), f"Command timed out after {timeout:.0f}s"
                if not progressed:
                    time.sleep(0.05)
            rc = chan.recv_exit_status()
            return rc, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")
        finally:
            chan.close()

    def home(self) -> str:
        if self._home is None:
            self._need()
            with self._lock:
                self._home = self._sftp.normalize(".")
        return self._home

    def put_bytes(self, remote_path: str, data: bytes, mode: Optional[int] = None) -> None:
        self._need()
        remote_path = self.expand(remote_path)
        with self._lock:
            with self._sftp.open(remote_path, "wb") as fh:
                fh.set_pipelined(True)
                fh.write(data)
            if mode is not None:
                self._sftp.chmod(remote_path, mode)

    def listdir(self, path: str) -> List[RemoteEntry]:
        self._need()
        path = self.expand(path)
        with self._lock:
            attrs = self._sftp.listdir_attr(path)
        return [RemoteEntry(a.filename, posixpath.join(path, a.filename),
                            _stat.S_ISDIR(a.st_mode or 0), a.st_size or 0, a.st_mtime or 0)
                for a in attrs]

    def read_tail(self, path: str, max_bytes: int) -> Tuple[bytes, int]:
        self._need()
        path = self.expand(path)
        with self._lock:
            size = self._sftp.stat(path).st_size or 0
            with self._sftp.open(path, "rb") as fh:
                if size > max_bytes:
                    fh.seek(size - max_bytes)
                fh.prefetch()
                return fh.read(), size

    def get_file(self, remote_path, local_path, progress=None) -> None:
        self._need()
        remote_path = self.expand(remote_path)
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        with self._lock:
            self._sftp.get(remote_path, local_path, callback=progress)


class ConnectionPool:
    """One live connection per cluster profile, so OTP is asked once."""

    def __init__(self):
        self._conns: Dict[str, Connection] = {}
        self._lock = threading.RLock()

    def get(self, profile: ClusterProfile, prompt: Optional[PromptFn] = None,
            confirm: Optional[ConfirmFn] = None) -> Connection:
        with self._lock:
            conn = self._conns.get(profile.name)
            if conn is not None and conn.is_alive():
                if not isinstance(conn, SSHConnection) or self._same_target(conn.profile, profile):
                    return conn
            if conn is not None:
                conn.close()
            if profile.connection == "local":
                conn = LocalConnection()
            else:
                conn = SSHConnection(profile, prompt=prompt, confirm=confirm).connect()
            self._conns[profile.name] = conn
            return conn

    @staticmethod
    def _same_target(a: ClusterProfile, b: ClusterProfile) -> bool:
        return (a.host, a.username, int(a.port), a.key_path) == (b.host, b.username, int(b.port), b.key_path)

    def is_connected(self, name: str) -> bool:
        # Lock-free on purpose: the UI thread calls this while a worker may be
        # holding the lock waiting for the user to type an OTP.
        conn = self._conns.get(name)
        return bool(conn and conn.is_alive())

    def drop(self, name: str) -> None:
        with self._lock:
            conn = self._conns.pop(name, None)
        if conn:
            conn.close()

    def close_all(self) -> None:
        with self._lock:
            names = list(self._conns)
        for n in names:
            self.drop(n)


def forget_host_key(profile: ClusterProfile, known_hosts_path: str = KNOWN_HOSTS_PATH) -> bool:
    """Remove the remembered key for this profile's host (AutoChemy's own file)."""
    if not os.path.isfile(known_hosts_path):
        return False
    host_id = profile.host if int(profile.port) == 22 else f"[{profile.host}]:{profile.port}"
    with open(known_hosts_path, "r", encoding="utf-8") as fh:
        lines = fh.readlines()
    keep = [ln for ln in lines if host_id not in ln.split(" ", 1)[0].split(",")]
    with open(known_hosts_path, "w", encoding="utf-8") as fh:
        fh.writelines(keep)
    return len(keep) != len(lines)


# --------------------------------------------------------------------------
# Schedulers
# --------------------------------------------------------------------------

class Scheduler:
    name = "base"

    def __init__(self, profile: ClusterProfile):
        self.profile = profile

    def _wrap(self, cmd: str) -> str:
        pre = (self.profile.pre_command or "").strip()
        return f"{pre}; {cmd}" if pre else cmd

    def submit_command(self, remote_dir: str, script: str) -> str:
        raise NotImplementedError

    def parse_job_id(self, stdout: str) -> str:
        raise NotImplementedError

    def status(self, conn: Connection, job_ids: Sequence[str]) -> Dict[str, Tuple[str, str]]:
        """Return {job_id: (normalised_state, raw_state)}. Missing id = unknown."""
        raise NotImplementedError

    def cancel_command(self, job_id: str) -> str:
        raise NotImplementedError

    def submit(self, conn: Connection, remote_dir: str, script: str) -> Tuple[str, str]:
        cmd = self._wrap(self.submit_command(remote_dir, script))
        rc, out, err = conn.run(cmd, timeout=120)
        if rc != 0:
            raise HPCError(f"Submission failed (exit {rc}).\n{(err or out).strip()}")
        job_id = self.parse_job_id(out)
        if not job_id:
            raise HPCError(f"Job was sent but no job ID was found in the reply:\n{(out + err).strip()}")
        return job_id, (out + err).strip()

    def cancel(self, conn: Connection, job_id: str) -> str:
        rc, out, err = conn.run(self._wrap(self.cancel_command(job_id)), timeout=60)
        if rc != 0:
            raise HPCError(f"Cancel failed (exit {rc}).\n{(err or out).strip()}")
        return (out + err).strip()


def _cd(remote_dir: str) -> str:
    return f"cd {shlex.quote(remote_dir)}"


class SlurmScheduler(Scheduler):
    name = "SLURM"
    _MAP = {
        "PENDING": QUEUED, "CONFIGURING": QUEUED, "REQUEUED": QUEUED, "RESV_DEL_HOLD": HELD,
        "REQUEUE_HOLD": HELD, "SUSPENDED": HELD, "STOPPED": HELD,
        "RUNNING": RUNNING, "COMPLETING": RUNNING, "STAGE_OUT": RUNNING, "SIGNALING": RUNNING,
        "COMPLETED": COMPLETED,
        "CANCELLED": CANCELLED, "REVOKED": CANCELLED,
        "FAILED": FAILED, "TIMEOUT": FAILED, "OUT_OF_MEMORY": FAILED, "NODE_FAIL": FAILED,
        "BOOT_FAIL": FAILED, "DEADLINE": FAILED, "PREEMPTED": FAILED, "SPECIAL_EXIT": FAILED,
    }

    def submit_command(self, remote_dir, script):
        return f"{_cd(remote_dir)} && sbatch --parsable {shlex.quote(script)}"

    def parse_job_id(self, stdout):
        for line in stdout.splitlines():
            line = line.strip()
            m = re.match(r"^(\d+(?:_\d+)?)(?:;\S+)?$", line)
            if m:
                return m.group(1)
            m = re.search(r"Submitted batch job (\d+)", line)
            if m:
                return m.group(1)
        return ""

    def map_state(self, raw: str) -> str:
        key = (raw or "").strip().split()[0].rstrip("+").upper() if raw and raw.strip() else ""
        if key.startswith("CANCELLED"):
            return CANCELLED
        return self._MAP.get(key, UNKNOWN)

    def status(self, conn, job_ids):
        result: Dict[str, Tuple[str, str]] = {}
        ids = [j for j in job_ids if j]
        if not ids:
            return result
        joined = ",".join(ids)
        rc, out, _err = conn.run(self._wrap(f"squeue -h -o '%i %T' -j {shlex.quote(joined)} 2>/dev/null"), timeout=60)
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0] in ids:
                result[parts[0]] = (self.map_state(parts[1]), parts[1])
        missing = [j for j in ids if j not in result]
        if missing:
            rc, out, _err = conn.run(self._wrap(
                f"sacct -n -X -P -o JobID,State -j {shlex.quote(','.join(missing))} 2>/dev/null"), timeout=60)
            for line in out.splitlines():
                parts = line.strip().split("|")
                if len(parts) >= 2 and parts[0] in missing:
                    result[parts[0]] = (self.map_state(parts[1]), parts[1])
        for j in ids:
            result.setdefault(j, (FINISHED, "not in queue"))
        return result

    def cancel_command(self, job_id):
        return f"scancel {shlex.quote(job_id)}"


class PBSScheduler(Scheduler):
    """Torque, PBS Pro and OpenPBS."""
    name = "PBS"
    _MAP = {"Q": QUEUED, "W": QUEUED, "T": QUEUED, "M": QUEUED, "S": HELD, "U": HELD,
            "H": HELD, "R": RUNNING, "E": RUNNING, "B": RUNNING, "X": FINISHED}

    def submit_command(self, remote_dir, script):
        return f"{_cd(remote_dir)} && qsub {shlex.quote(script)}"

    def parse_job_id(self, stdout):
        for line in stdout.splitlines():
            line = line.strip()
            m = re.match(r"^(\d+(?:\[\])?(?:\.[\w.-]+)?)$", line)
            if m:
                return m.group(1)
        return ""

    @staticmethod
    def _parse_full(text: str) -> Dict[str, Dict[str, str]]:
        jobs: Dict[str, Dict[str, str]] = {}
        current = None
        for raw in text.splitlines():
            m = re.match(r"^Job Id:\s*(\S+)", raw)
            if m:
                current = m.group(1)
                jobs[current] = {}
                continue
            if current and "=" in raw:
                k, _, v = raw.strip().partition("=")
                jobs[current][k.strip()] = v.strip()
        return jobs

    @staticmethod
    def _same(a: str, b: str) -> bool:
        return a == b or a.split(".")[0] == b.split(".")[0]

    def _state_from_attrs(self, attrs: Dict[str, str]) -> Tuple[str, str]:
        raw = attrs.get("job_state", "")
        if raw in ("C", "F"):
            code = attrs.get("Exit_status", attrs.get("exit_status", ""))
            if code == "":
                return FINISHED, raw
            try:
                n = int(code)
            except ValueError:
                return FINISHED, f"{raw} exit={code}"
            if n == 0:
                return COMPLETED, f"{raw} exit=0"
            if n in (-1, 271, 265):   # killed by qdel / signal 15 / 9
                return CANCELLED, f"{raw} exit={n}"
            return FAILED, f"{raw} exit={n}"
        return self._MAP.get(raw, UNKNOWN), raw

    def status(self, conn, job_ids):
        result: Dict[str, Tuple[str, str]] = {}
        ids = [j for j in job_ids if j]
        if not ids:
            return result
        quoted = " ".join(shlex.quote(j) for j in ids)
        # PBS Pro/OpenPBS keep finished jobs with -x; Torque doesn't know -x.
        for flags in ("-x -f", "-f"):
            rc, out, err = conn.run(self._wrap(f"qstat {flags} {quoted} 2>&1"), timeout=60)
            parsed = self._parse_full(out)
            if parsed:
                for want in ids:
                    for got, attrs in parsed.items():
                        if self._same(want, got):
                            result[want] = self._state_from_attrs(attrs)
                if flags == "-x -f" or len(result) == len(ids):
                    break
            elif "invalid option" not in out.lower() and "illegal option" not in out.lower():
                break
        for j in ids:
            result.setdefault(j, (FINISHED, "not in queue"))
        return result

    def cancel_command(self, job_id):
        return f"qdel {shlex.quote(job_id)}"


class SGEScheduler(Scheduler):
    name = "SGE"

    def submit_command(self, remote_dir, script):
        return f"{_cd(remote_dir)} && qsub -terse -cwd {shlex.quote(script)}"

    def parse_job_id(self, stdout):
        for line in stdout.splitlines():
            m = re.match(r"^\s*(\d+)(?:\.\S+)?\s*$", line) or re.search(r"Your job (\d+)", line)
            if m:
                return m.group(1)
        return ""

    @staticmethod
    def map_state(raw: str) -> str:
        s = raw or ""
        if "E" in s:
            return FAILED
        if "d" in s:
            return CANCELLED
        if "h" in s or "s" in s or "S" in s or "T" in s:
            return HELD
        if "r" in s or "t" in s or "R" in s:
            return RUNNING
        if "w" in s:
            return QUEUED
        return UNKNOWN

    def status(self, conn, job_ids):
        result: Dict[str, Tuple[str, str]] = {}
        ids = [j for j in job_ids if j]
        if not ids:
            return result
        rc, out, _ = conn.run(self._wrap("qstat -u \"$USER\" 2>/dev/null"), timeout=60)
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 5 and parts[0] in ids:
                result[parts[0]] = (self.map_state(parts[4]), parts[4])
        for j in ids:
            if j in result:
                continue
            rc, out, _ = conn.run(self._wrap(f"qacct -j {shlex.quote(j)} 2>/dev/null"), timeout=60)
            exit_m = re.search(r"^exit_status\s+(\d+)", out, re.M)
            failed_m = re.search(r"^failed\s+(\d+)", out, re.M)
            if exit_m:
                ok = exit_m.group(1) == "0" and (not failed_m or failed_m.group(1) == "0")
                result[j] = (COMPLETED if ok else FAILED, f"exit={exit_m.group(1)}")
            else:
                result[j] = (FINISHED, "not in queue")
        return result

    def cancel_command(self, job_id):
        return f"qdel {shlex.quote(job_id)}"


class LSFScheduler(Scheduler):
    name = "LSF"
    _MAP = {"PEND": QUEUED, "WAIT": QUEUED, "PROV": QUEUED, "RUN": RUNNING,
            "PSUSP": HELD, "USUSP": HELD, "SSUSP": HELD, "DONE": COMPLETED,
            "EXIT": FAILED, "ZOMBI": FAILED, "UNKWN": UNKNOWN}

    def submit_command(self, remote_dir, script):
        return f"{_cd(remote_dir)} && bsub < {shlex.quote(script)}"

    def parse_job_id(self, stdout):
        m = re.search(r"Job <(\d+)>", stdout)
        return m.group(1) if m else ""

    def status(self, conn, job_ids):
        result: Dict[str, Tuple[str, str]] = {}
        ids = [j for j in job_ids if j]
        if not ids:
            return result
        quoted = " ".join(shlex.quote(j) for j in ids)
        rc, out, _ = conn.run(self._wrap(f"bjobs -a -noheader -o 'jobid stat' {quoted} 2>/dev/null"), timeout=60)
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0] in ids:
                result[parts[0]] = (self._MAP.get(parts[1].upper(), UNKNOWN), parts[1])
        for j in ids:
            result.setdefault(j, (FINISHED, "not in queue"))
        return result

    def cancel_command(self, job_id):
        return f"bkill {shlex.quote(job_id)}"


class CustomScheduler(Scheduler):
    """User supplied commands for anything else (or wrapper scripts)."""
    name = "Custom"

    def _fill(self, template: str, **values) -> str:
        out = template
        for k, v in values.items():
            out = out.replace("{" + k + "}", shlex.quote(v))
        return out

    def submit_command(self, remote_dir, script):
        return f"{_cd(remote_dir)} && " + self._fill(self.profile.submit_cmd, script=script, dir=remote_dir)

    def parse_job_id(self, stdout):
        try:
            m = re.search(self.profile.job_id_regex or r"(\d+)", stdout)
        except re.error as exc:
            raise HPCError(f"Job ID pattern is not a valid regular expression: {exc}")
        if not m:
            return ""
        return m.group(1) if m.groups() else m.group(0)

    @staticmethod
    def map_state(text: str) -> str:
        t = (text or "").lower()
        for words, state in (
            (("cancel", "killed", "deleted"), CANCELLED),
            (("fail", "error", "exit", "timeout"), FAILED),
            (("complete", "done", "finished", "success"), COMPLETED),
            (("run", "executing", "active"), RUNNING),
            (("hold", "held", "suspend"), HELD),
            (("pend", "queue", "wait", "qw"), QUEUED),
        ):
            if any(w in t for w in words):
                return state
        return UNKNOWN if t.strip() else FINISHED

    def status(self, conn, job_ids):
        result = {}
        for j in job_ids:
            if not j:
                continue
            rc, out, err = conn.run(self._wrap(self._fill(self.profile.status_cmd, job_id=j)), timeout=60)
            raw = (out or err).strip().splitlines()
            raw_text = raw[-1] if raw else ""
            result[j] = (self.map_state(raw_text), raw_text or "no output")
        return result

    def cancel_command(self, job_id):
        if not self.profile.cancel_cmd.strip():
            raise HPCError("No cancel command is set for this cluster.")
        return self._fill(self.profile.cancel_cmd, job_id=job_id)


def get_scheduler(profile: ClusterProfile) -> Scheduler:
    return {
        "SLURM": SlurmScheduler, "PBS": PBSScheduler, "SGE": SGEScheduler,
        "LSF": LSFScheduler, "Custom": CustomScheduler,
    }.get(profile.scheduler, SlurmScheduler)(profile)


# --------------------------------------------------------------------------
# Job history
# --------------------------------------------------------------------------

@dataclass
class JobRecord:
    id: str
    name: str
    profile: str
    scheduler: str
    remote_dir: str
    script: str
    files: List[str] = field(default_factory=list)
    output_file: str = ""
    scheduler_id: str = ""
    state: str = SUBMITTING
    raw_state: str = ""
    submitted_at: float = 0.0
    updated_at: float = 0.0
    finished_at: float = 0.0
    local_dir: str = ""
    note: str = ""

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @classmethod
    def from_dict(cls, d: dict) -> "JobRecord":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in d.items() if k in known})


class JobStore:
    def __init__(self, path: str = JOB_HISTORY_PATH):
        self.path = path
        self._lock = threading.RLock()
        self.jobs: Dict[str, JobRecord] = {}
        self.load()

    def load(self) -> None:
        with self._lock:
            raw = _read_json(self.path, {})
            items = raw.get("jobs", []) if isinstance(raw, dict) else []
            self.jobs = {}
            for d in items:
                if isinstance(d, dict) and d.get("id"):
                    try:
                        rec = JobRecord.from_dict(d)
                    except TypeError:
                        continue
                    self.jobs[rec.id] = rec

    def save(self) -> None:
        with self._lock:
            ordered = sorted(self.jobs.values(), key=lambda r: r.submitted_at or 0, reverse=True)
            _atomic_write_json(self.path, {"version": 1, "jobs": [asdict(r) for r in ordered]})

    def all(self) -> List[JobRecord]:
        with self._lock:
            return sorted(self.jobs.values(), key=lambda r: r.submitted_at or 0, reverse=True)

    def get(self, job_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self.jobs.get(job_id)

    def upsert(self, rec: JobRecord) -> None:
        with self._lock:
            self.jobs[rec.id] = rec
            self.save()

    def remove(self, job_id: str) -> None:
        with self._lock:
            self.jobs.pop(job_id, None)
            self.save()


# --------------------------------------------------------------------------
# Operations used by the UI
# --------------------------------------------------------------------------

def _to_lf(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


def guess_output_file(files: Iterable[str]) -> str:
    """ORCA writes <name>.out next to <name>.inp in AutoChemy's scripts."""
    for f in files:
        base, ext = os.path.splitext(os.path.basename(f))
        if ext.lower() == ".inp":
            return base + ".out"
    for f in files:
        base, ext = os.path.splitext(os.path.basename(f))
        if ext.lower() in (".gjf", ".com"):
            return base + ".log"
    return ""


def submit_job(conn: Connection, profile: ClusterProfile, store: JobStore,
               local_files: Sequence[str], script_name: str, job_name: str,
               log: Optional[Callable[[str], None]] = None) -> JobRecord:
    """Upload files into a new folder on the cluster and submit the script."""
    say = log or (lambda _m: None)
    files = [os.path.abspath(f) for f in local_files]
    if not files:
        raise HPCError("No files selected.")
    for f in files:
        if not os.path.isfile(f):
            raise HPCError(f"File not found: {f}")
    names = [os.path.basename(f) for f in files]
    if len(set(names)) != len(names):
        raise HPCError("Two selected files have the same name; rename one of them.")
    if script_name not in names:
        raise HPCError(f"The batch script '{script_name}' is not among the selected files.")

    sched = get_scheduler(profile)
    stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    folder = f"{safe_name(job_name)}_{stamp}"
    base = conn.expand(profile.remote_base_dir)
    remote_dir = posixpath.join(base, folder)

    rec = JobRecord(
        id=uuid.uuid4().hex[:12], name=job_name or folder, profile=profile.name,
        scheduler=profile.scheduler, remote_dir=remote_dir, script=script_name,
        files=names, output_file=guess_output_file(names),
        submitted_at=_now(), updated_at=_now(),
    )

    say(f"Creating {remote_dir}")
    conn.mkdir_p(remote_dir)
    for path, name in zip(files, names):
        with open(path, "rb") as fh:
            data = fh.read()
        is_text_script = name == script_name or name.lower().endswith((".sh", ".pbs", ".slurm", ".inp", ".gjf", ".com", ".xyz"))
        if is_text_script:
            data = _to_lf(data)   # Windows line endings break bash and ORCA
        say(f"Uploading {name} ({len(data):,} bytes)")
        conn.put_bytes(posixpath.join(remote_dir, name), data,
                       mode=0o750 if name == script_name else None)

    say(f"Submitting with {sched.name}")
    try:
        sched_id, reply = sched.submit(conn, remote_dir, script_name)
    except HPCError as exc:
        rec.state, rec.raw_state, rec.note = FAILED, "submit error", str(exc)
        rec.finished_at = _now()
        store.upsert(rec)
        raise
    rec.scheduler_id = sched_id
    rec.state, rec.raw_state = QUEUED, "submitted"
    rec.note = reply[:500]
    rec.updated_at = _now()
    store.upsert(rec)
    say(f"Submitted: job ID {sched_id}")
    return rec


def _refine_with_output(conn: Connection, rec: JobRecord) -> None:
    """If the scheduler only says 'finished', check the ORCA output for success."""
    if rec.state != FINISHED or not rec.output_file:
        return
    path = posixpath.join(rec.remote_dir, rec.output_file)
    rc, out, _ = conn.run(f"tail -c 20000 {shlex.quote(path)} 2>/dev/null", timeout=60)
    if rc != 0 or not out:
        return
    if ORCA_SUCCESS_MARKER in out:
        rec.state = COMPLETED
        rec.raw_state += " (output: normal termination)"
    elif re.search(r"aborting the run|ORCA finished by error|error termination", out, re.I):
        rec.state = FAILED
        rec.raw_state += " (output: error)"


def refresh_jobs(conn: Connection, profile: ClusterProfile, store: JobStore,
                 records: Sequence[JobRecord]) -> List[JobRecord]:
    """Ask the scheduler about every unfinished job on this cluster."""
    pending = [r for r in records if r.profile == profile.name and r.scheduler_id and not r.is_terminal]
    if not pending:
        return []
    sched = get_scheduler(profile)
    states = sched.status(conn, [r.scheduler_id for r in pending])
    changed = []
    for rec in pending:
        state, raw = states.get(rec.scheduler_id, (UNKNOWN, ""))
        old = (rec.state, rec.raw_state)
        rec.state, rec.raw_state = state, raw
        _refine_with_output(conn, rec)
        rec.updated_at = _now()
        if rec.is_terminal and not rec.finished_at:
            rec.finished_at = _now()
        store.upsert(rec)
        if (rec.state, rec.raw_state) != old:
            changed.append(rec)
    return changed


def cancel_job(conn: Connection, profile: ClusterProfile, store: JobStore, rec: JobRecord) -> str:
    if not rec.scheduler_id:
        raise HPCError("This job has no scheduler ID (it was never submitted).")
    reply = get_scheduler(profile).cancel(conn, rec.scheduler_id)
    rec.state, rec.raw_state = CANCELLED, "cancel requested"
    rec.updated_at = rec.finished_at = _now()
    store.upsert(rec)
    return reply


def list_remote_dir(conn: Connection, path: str) -> List[RemoteEntry]:
    try:
        entries = conn.listdir(path)
    except FileNotFoundError:
        raise HPCError(f"Folder not found on the cluster: {path}")
    except PermissionError:
        raise HPCError(f"No permission to read: {path}")
    except OSError as exc:
        raise HPCError(f"Could not list {path}: {exc}")
    return sorted(entries, key=lambda e: (not e.is_dir, e.name.lower()))


def read_remote_tail(conn: Connection, path: str, max_bytes: int = 256 * 1024) -> Tuple[str, int, bool]:
    """Return (text, total_size, truncated). Binary files are reported, not shown."""
    try:
        data, size = conn.read_tail(path, max_bytes)
    except OSError as exc:
        raise HPCError(f"Could not read {path}: {exc}")
    if b"\x00" in data[:4096]:
        return f"[binary file, {size:,} bytes - download it to open]", size, False
    return data.decode("utf-8", "replace"), size, size > max_bytes


def _walk(conn: Connection, path: str) -> Iterable[Tuple[str, RemoteEntry]]:
    """Yield (relative_path, entry) for every file under path."""
    stack = [("", path)]
    while stack:
        rel, cur = stack.pop()
        for e in conn.listdir(cur):
            child_rel = posixpath.join(rel, e.name) if rel else e.name
            if e.is_dir:
                stack.append((child_rel, e.path))
            else:
                yield child_rel, e


def download_paths(conn: Connection, entries: Sequence[RemoteEntry], local_dir: str,
                   log: Optional[Callable[[str], None]] = None,
                   max_total_bytes: int = 5 * 1024 ** 3) -> List[str]:
    """Download files/folders into local_dir. Returns local paths written."""
    say = log or (lambda _m: None)
    plan: List[Tuple[str, str, int]] = []
    for e in entries:
        if e.is_dir:
            for rel, sub in _walk(conn, e.path):
                plan.append((sub.path, os.path.join(local_dir, e.name, *rel.split("/")), sub.size))
        else:
            plan.append((e.path, os.path.join(local_dir, e.name), e.size))
    total = sum(p[2] for p in plan)
    if total > max_total_bytes:
        raise HPCError(f"Selection is {total / 1024 ** 3:.1f} GB; pick fewer files "
                       f"(limit {max_total_bytes / 1024 ** 3:.0f} GB).")
    written = []
    root = os.path.abspath(local_dir)
    for remote, local, size in plan:
        local_abs = os.path.abspath(local)
        if not local_abs.startswith(root + os.sep) and local_abs != root:
            continue   # never write outside the chosen folder
        say(f"Downloading {posixpath.basename(remote)} ({size:,} bytes)")
        conn.get_file(remote, local_abs)
        written.append(local_abs)
    return written


def stage_files(job_name: str, contents: Dict[str, bytes], extra_files: Sequence[str] = ()) -> List[str]:
    """Write generated files (e.g. .inp + .sh from Input Creator) to a local
    staging folder and return their paths, ready for submit_job()."""
    stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    folder = os.path.join(STAGING_DIR, f"{safe_name(job_name)}_{stamp}")
    os.makedirs(folder, exist_ok=True)
    paths = []
    for name, data in contents.items():
        p = os.path.join(folder, os.path.basename(name))
        with open(p, "wb") as fh:
            fh.write(data)
        paths.append(p)
    for src in extra_files:
        if src and os.path.isfile(src):
            dst = os.path.join(folder, os.path.basename(src))
            if os.path.abspath(dst) not in map(os.path.abspath, paths):
                shutil.copy2(src, dst)
                paths.append(dst)
    return paths
