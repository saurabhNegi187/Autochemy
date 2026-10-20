"""
Job Manager - submit jobs to any HPC cluster, follow their status and
browse / download their files.

Every user adds their own cluster(s) with "Add cluster" (host, username,
SSH key, scheduler, remote folder). Passwords and OTP codes are asked when
connecting and are never saved.

All network work runs in background threads; results come back to the Tk
thread through a queue that is polled with ``after`` (Tk is not thread safe).
"""
from __future__ import annotations

import os
import posixpath
import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Callable, Dict, List, Optional, Sequence

from modules.base_module import BaseModule
from modules import hpc_backend as hb

STATE_COLORS = {
    hb.QUEUED: "#b7791f",
    hb.HELD: "#b7791f",
    hb.RUNNING: "#2b6cb0",
    hb.COMPLETED: "#2f855a",
    hb.FAILED: "#c53030",
    hb.CANCELLED: "#718096",
    hb.FINISHED: "#2c7a7b",
    hb.UNKNOWN: "#718096",
    hb.SUBMITTING: "#718096",
}

SCRIPT_EXTS = (".sh", ".pbs", ".slurm", ".job", ".sbatch", ".qsub", ".lsf")


def _fmt_size(n: int) -> str:
    size = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


class JobManagerModule(BaseModule):
    AUTO_REFRESH_DEFAULT_S = 60

    def __init__(self, parent_frame):
        super().__init__(parent_frame)
        self.profiles = hb.ProfileStore()
        self.jobs = hb.JobStore()
        self.pool = hb.ConnectionPool()
        self._ui_q: "queue.Queue" = queue.Queue()
        self._pump_started = False
        self._auto_after = None
        self._refresh_running = False
        self._file_entries: Dict[str, hb.RemoteEntry] = {}
        self._editors: List[tk.Text] = []

    def get_name(self) -> str:
        return "Job Manager"

    # ------------------------------------------------------------------ UI
    def create_ui(self):
        self.main_frame = ttk.Frame(self.parent_frame, padding=10)
        root = self.main_frame

        head = ttk.Frame(root)
        head.pack(fill=tk.X)
        ttk.Label(head, text="HPC Job Manager", font=("Segoe UI", 14, "bold")).pack(side=tk.LEFT)
        ttk.Label(
            head,
            text="Add your cluster once, then submit jobs, watch their status and fetch results.",
            font=("Segoe UI", 9, "italic"),
        ).pack(side=tk.LEFT, padx=(12, 0))

        # --- cluster row -------------------------------------------------
        cl = ttk.LabelFrame(root, text="Cluster", padding=(8, 6))
        cl.pack(fill=tk.X, pady=(8, 6))
        self.profile_var = tk.StringVar()
        self.profile_combo = ttk.Combobox(cl, textvariable=self.profile_var, state="readonly", width=34)
        self.profile_combo.pack(side=tk.LEFT)
        self.profile_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_profile_changed())
        for text, cmd in (("Add cluster", self._add_profile), ("Edit", self._edit_profile),
                          ("Delete", self._delete_profile)):
            ttk.Button(cl, text=text, command=cmd).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Separator(cl, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)
        self.btn_connect = ttk.Button(cl, text="Connect", command=self._connect_selected)
        self.btn_connect.pack(side=tk.LEFT)
        self.btn_disconnect = ttk.Button(cl, text="Disconnect", command=self._disconnect_selected)
        self.btn_disconnect.pack(side=tk.LEFT, padx=(6, 0))
        self.conn_status = tk.StringVar(value="Not connected.")
        ttk.Label(cl, textvariable=self.conn_status, font=("Segoe UI", 9, "italic")).pack(side=tk.LEFT, padx=(10, 0))

        panes = ttk.PanedWindow(root, orient=tk.VERTICAL)
        panes.pack(fill=tk.BOTH, expand=True)

        # --- jobs table --------------------------------------------------
        jobs_box = ttk.LabelFrame(panes, text="Jobs", padding=(8, 6))
        panes.add(jobs_box, weight=3)
        bar = ttk.Frame(jobs_box)
        bar.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(bar, text="New submission...", command=lambda: self.open_submit_dialog()).pack(side=tk.LEFT)
        ttk.Button(bar, text="Refresh status", command=lambda: self.refresh_status(manual=True)).pack(side=tk.LEFT, padx=(6, 0))
        self.auto_var = tk.BooleanVar(value=True)
        self.auto_secs = tk.StringVar(value=str(self.AUTO_REFRESH_DEFAULT_S))
        ttk.Checkbutton(bar, text="Auto-refresh every", variable=self.auto_var,
                        command=self._schedule_auto_refresh).pack(side=tk.LEFT, padx=(12, 2))
        ttk.Spinbox(bar, from_=15, to=3600, increment=15, width=5, textvariable=self.auto_secs,
                    command=self._schedule_auto_refresh).pack(side=tk.LEFT)
        ttk.Label(bar, text="s").pack(side=tk.LEFT, padx=(2, 12))
        self.show_all_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(bar, text="Show jobs from all clusters", variable=self.show_all_var,
                        command=self._render_jobs).pack(side=tk.LEFT)

        bar2 = ttk.Frame(jobs_box)
        bar2.pack(fill=tk.X, pady=(0, 6))
        for text, cmd in (("Browse files", self._browse_selected_job),
                          ("View output", self._view_selected_output),
                          ("Download job folder", self._download_selected_job),
                          ("Cancel job", self._cancel_selected_job),
                          ("Resubmit", self._resubmit_selected_job),
                          ("Remove from list", self._remove_selected_job)):
            ttk.Button(bar2, text=text, command=cmd).pack(side=tk.LEFT, padx=(0, 6))

        cols = ("name", "cluster", "jobid", "state", "detail", "submitted", "updated", "folder")
        heads = ("Name", "Cluster", "Job ID", "State", "Scheduler says", "Submitted", "Last checked", "Remote folder")
        widths = (170, 110, 150, 85, 150, 120, 120, 320)
        wrap = ttk.Frame(jobs_box)
        wrap.pack(fill=tk.BOTH, expand=True)
        self.jobs_tree = ttk.Treeview(wrap, columns=cols, show="headings", selectmode="browse", height=8)
        for c, h, w in zip(cols, heads, widths):
            self.jobs_tree.heading(c, text=h)
            self.jobs_tree.column(c, width=w, minwidth=60, stretch=(c == "folder"))
        ysb = ttk.Scrollbar(wrap, orient=tk.VERTICAL, command=self.jobs_tree.yview)
        self.jobs_tree.configure(yscrollcommand=ysb.set)
        self.jobs_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb.pack(side=tk.LEFT, fill=tk.Y)
        for state, color in STATE_COLORS.items():
            self.jobs_tree.tag_configure(state, foreground=color)
        self.jobs_tree.bind("<Double-1>", lambda _e: self._browse_selected_job())

        # --- remote files ------------------------------------------------
        files_box = ttk.LabelFrame(panes, text="Files on the cluster", padding=(8, 6))
        panes.add(files_box, weight=3)
        path_row = ttk.Frame(files_box)
        path_row.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(path_row, text="Up", width=4, command=self._files_up).pack(side=tk.LEFT)
        self.path_var = tk.StringVar(value="")
        path_entry = ttk.Entry(path_row, textvariable=self.path_var)
        path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        path_entry.bind("<Return>", lambda _e: self.browse(self.path_var.get()))
        ttk.Button(path_row, text="Go", command=lambda: self.browse(self.path_var.get())).pack(side=tk.LEFT)
        ttk.Button(path_row, text="Home", command=lambda: self.browse("~")).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(path_row, text="Refresh", command=lambda: self.browse(self.path_var.get())).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(path_row, text="Download selected", command=self._download_selected_files).pack(side=tk.LEFT, padx=(6, 0))

        split = ttk.PanedWindow(files_box, orient=tk.HORIZONTAL)
        split.pack(fill=tk.BOTH, expand=True)
        left = ttk.Frame(split)
        split.add(left, weight=2)
        self.files_tree = ttk.Treeview(left, columns=("size", "mtime"), selectmode="extended", height=8)
        self.files_tree.heading("#0", text="Name")
        self.files_tree.heading("size", text="Size")
        self.files_tree.heading("mtime", text="Modified")
        self.files_tree.column("#0", width=240)
        self.files_tree.column("size", width=80, anchor="e")
        self.files_tree.column("mtime", width=120)
        fsb = ttk.Scrollbar(left, orient=tk.VERTICAL, command=self.files_tree.yview)
        self.files_tree.configure(yscrollcommand=fsb.set)
        self.files_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        fsb.pack(side=tk.LEFT, fill=tk.Y)
        self.files_tree.bind("<Double-1>", lambda _e: self._open_file_entry())
        self.files_tree.bind("<Return>", lambda _e: self._open_file_entry())

        right = ttk.Frame(split)
        split.add(right, weight=3)
        prev_bar = ttk.Frame(right)
        prev_bar.pack(fill=tk.X)
        self.preview_title = tk.StringVar(value="Double-click a file to preview it (last 256 KB).")
        ttk.Label(prev_bar, textvariable=self.preview_title, font=("Segoe UI", 9, "italic")).pack(side=tk.LEFT)
        ttk.Button(prev_bar, text="Reload", command=self._reload_preview).pack(side=tk.RIGHT)
        self.preview = tk.Text(right, wrap="none", font=("Consolas", 9), height=10)
        psy = ttk.Scrollbar(right, orient=tk.VERTICAL, command=self.preview.yview)
        psx = ttk.Scrollbar(right, orient=tk.HORIZONTAL, command=self.preview.xview)
        self.preview.configure(yscrollcommand=psy.set, xscrollcommand=psx.set, state=tk.DISABLED)
        psx.pack(side=tk.BOTTOM, fill=tk.X)
        self.preview.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        psy.pack(side=tk.LEFT, fill=tk.Y)
        self._editors.append(self.preview)
        self._preview_path: Optional[str] = None

        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(root, textvariable=self.status_var, anchor="w").pack(fill=tk.X, pady=(6, 0))

        self._reload_profiles()
        self._render_jobs()
        self._start_pump()
        self._schedule_auto_refresh()

    def apply_app_theme(self, ctx):
        try:
            from modules import app_theme
            for w in self._editors:
                if w.winfo_exists():
                    app_theme.apply_editor_style(w, ctx)
        except Exception:
            pass

    def cleanup(self):
        try:
            self.pool.close_all()
        finally:
            super().cleanup()

    # ------------------------------------------------- thread plumbing
    def _start_pump(self):
        if not self._pump_started:
            self._pump_started = True
            self.main_frame.after(100, self._pump)

    def _pump(self):
        try:
            while True:
                item = self._ui_q.get_nowait()
                kind = item[0]
                try:
                    if kind == "call":
                        item[1](*item[2])
                    elif kind == "prompt":
                        _k, text, secret, reply = item
                        reply.put(simpledialog.askstring(
                            "Cluster login", text, show="*" if secret else "",
                            parent=self.main_frame.winfo_toplevel()))
                    elif kind == "confirm":
                        _k, text, reply = item
                        reply.put(messagebox.askyesno(
                            "Cluster host key", text, parent=self.main_frame.winfo_toplevel()))
                except Exception as exc:   # never let one bad callback stop the pump
                    print(f"[Job Manager] UI callback error: {exc}")
        except queue.Empty:
            pass
        if self.main_frame is not None and self.main_frame.winfo_exists():
            self.main_frame.after(100, self._pump)

    def _ui(self, fn: Callable, *args):
        """Run fn(*args) on the Tk thread (callable from any thread)."""
        self._ui_q.put(("call", fn, args))

    def _prompt_from_worker(self, text: str, secret: bool) -> Optional[str]:
        reply: "queue.Queue" = queue.Queue()
        self._ui_q.put(("prompt", text, secret, reply))
        try:
            return reply.get(timeout=600)
        except queue.Empty:
            return None

    def _confirm_from_worker(self, text: str) -> bool:
        reply: "queue.Queue" = queue.Queue()
        self._ui_q.put(("confirm", text, reply))
        try:
            return bool(reply.get(timeout=600))
        except queue.Empty:
            return False

    def _bg(self, label: str, work: Callable, done: Optional[Callable] = None,
            failed: Optional[Callable] = None, quiet: bool = False):
        if label:
            self.status_var.set(label + " ...")

        def runner():
            try:
                result = work()
            except Exception as exc:
                self._ui(self._handle_error, label, exc, failed, quiet)
                return
            self._ui(self._handle_done, label, result, done)

        threading.Thread(target=runner, daemon=True).start()

    def _handle_done(self, label, result, done):
        if label:
            self.status_var.set(label + " - done.")
        self._update_conn_status()
        if done:
            done(result)

    def _handle_error(self, label, exc, failed, quiet):
        self.status_var.set(f"{label or 'Operation'} failed: {str(exc).splitlines()[0] if str(exc) else exc}")
        self._update_conn_status()
        if failed:
            failed(exc)
        if not quiet and not isinstance(exc, hb.AuthCancelled):
            msg = str(exc) if isinstance(exc, hb.HPCError) else f"{type(exc).__name__}: {exc}"
            messagebox.showerror("Job Manager", msg, parent=self.main_frame.winfo_toplevel())

    def _conn(self, profile: hb.ClusterProfile) -> hb.Connection:
        """Worker-thread only: get (or open) the connection for a profile."""
        return self.pool.get(profile, prompt=self._prompt_from_worker, confirm=self._confirm_from_worker)

    # ------------------------------------------------------- profiles
    def _reload_profiles(self, select: Optional[str] = None):
        names = self.profiles.names()
        self.profile_combo["values"] = names
        current = select or self.profile_var.get()
        if current in names:
            self.profile_var.set(current)
        elif names:
            self.profile_var.set(names[0])
        else:
            self.profile_var.set("")
        self._on_profile_changed()

    def _selected_profile(self, warn: bool = True) -> Optional[hb.ClusterProfile]:
        prof = self.profiles.get(self.profile_var.get())
        if prof is None and warn:
            messagebox.showinfo("Job Manager", "Add a cluster first (button 'Add cluster').",
                                parent=self.main_frame.winfo_toplevel())
        return prof

    def _on_profile_changed(self):
        self._update_conn_status()
        self._render_jobs()

    def _update_conn_status(self):
        prof = self.profiles.get(self.profile_var.get())
        if prof is None:
            self.conn_status.set("No cluster added yet.")
            return
        if self.pool.is_connected(prof.name):
            self.conn_status.set(f"Connected: {prof.label()}")
        else:
            self.conn_status.set(f"Not connected: {prof.label()}")

    def _add_profile(self, prefill: Optional[dict] = None, on_saved: Optional[Callable] = None):
        ProfileDialog(self, None, prefill=prefill, on_saved=on_saved)

    def _edit_profile(self):
        prof = self._selected_profile()
        if prof:
            ProfileDialog(self, prof)

    def _delete_profile(self):
        prof = self._selected_profile()
        if not prof:
            return
        if not messagebox.askyesno("Delete cluster",
                                   f"Delete cluster '{prof.name}'?\nIts job history stays in the list.",
                                   parent=self.main_frame.winfo_toplevel()):
            return
        self.pool.drop(prof.name)
        self.profiles.delete(prof.name)
        self._reload_profiles()

    def _connect_selected(self):
        prof = self._selected_profile()
        if not prof:
            return
        self._bg(f"Connecting to {prof.name}", lambda: self._conn(prof).home(),
                 done=lambda home: (self._update_conn_status(),
                                    self.browse(prof.remote_base_dir, quiet_missing=True, fallback=home)))

    def _disconnect_selected(self):
        prof = self._selected_profile(warn=False)
        if prof:
            self.pool.drop(prof.name)
            self._update_conn_status()
            self.status_var.set(f"Disconnected from {prof.name}.")

    # ----------------------------------------------------------- jobs
    def _render_jobs(self):
        if not hasattr(self, "jobs_tree"):
            return
        keep = self.jobs_tree.selection()
        self.jobs_tree.delete(*self.jobs_tree.get_children())
        only = None if self.show_all_var.get() else self.profile_var.get()
        for rec in self.jobs.all():
            if only and rec.profile != only:
                continue
            self.jobs_tree.insert(
                "", tk.END, iid=rec.id, tags=(rec.state,),
                values=(rec.name, rec.profile, rec.scheduler_id or "-", rec.state, rec.raw_state,
                        hb.fmt_time(rec.submitted_at), hb.fmt_time(rec.updated_at), rec.remote_dir),
            )
        for iid in keep:
            if self.jobs_tree.exists(iid):
                self.jobs_tree.selection_set(iid)

    def _selected_job(self, warn: bool = True) -> Optional[hb.JobRecord]:
        sel = self.jobs_tree.selection()
        rec = self.jobs.get(sel[0]) if sel else None
        if rec is None and warn:
            messagebox.showinfo("Job Manager", "Select a job in the list first.",
                                parent=self.main_frame.winfo_toplevel())
        return rec

    def _profile_for(self, rec: hb.JobRecord) -> Optional[hb.ClusterProfile]:
        prof = self.profiles.get(rec.profile)
        if prof is None:
            messagebox.showerror("Job Manager",
                                 f"Cluster '{rec.profile}' no longer exists. Add it again with the same name.",
                                 parent=self.main_frame.winfo_toplevel())
        return prof

    def refresh_status(self, manual: bool = False):
        if self._refresh_running:
            if manual:
                self.status_var.set("A status refresh is already running ...")
            return
        open_jobs = [r for r in self.jobs.all() if not r.is_terminal and r.scheduler_id]
        by_profile: Dict[str, List[hb.JobRecord]] = {}
        for r in open_jobs:
            by_profile.setdefault(r.profile, []).append(r)
        targets = []
        for name, recs in by_profile.items():
            prof = self.profiles.get(name)
            if prof is None:
                continue
            # Auto-refresh never triggers a login (no surprise OTP pop-ups).
            if manual or self.pool.is_connected(name) or prof.connection == "local":
                targets.append((prof, recs))
        if not targets:
            if manual:
                self.status_var.set("No running or queued jobs to check.")
            return
        self._refresh_running = True

        def work():
            changed, errors = [], []
            for prof, recs in targets:
                try:
                    changed += hb.refresh_jobs(self._conn(prof), prof, self.jobs, recs)
                except Exception as exc:
                    errors.append(f"{prof.name}: {exc}")
            return changed, errors

        def done(result):
            self._refresh_running = False
            changed, errors = result
            self._render_jobs()
            finished = [r for r in changed if r.is_terminal]
            msg = f"Status checked ({len(changed)} changed)."
            if finished:
                msg += "  Finished: " + ", ".join(f"{r.name} -> {r.state}" for r in finished)
            if errors:
                msg += "  Problems: " + "; ".join(e.splitlines()[0] for e in errors)
                if manual:
                    messagebox.showwarning("Job Manager", "\n\n".join(errors),
                                           parent=self.main_frame.winfo_toplevel())
            self.status_var.set(msg)

        self._bg("Checking job status" if manual else "", work, done,
                 failed=lambda _e: setattr(self, "_refresh_running", False), quiet=not manual)

    def _schedule_auto_refresh(self):
        if self.main_frame is None:
            return
        if self._auto_after is not None:
            try:
                self.main_frame.after_cancel(self._auto_after)
            except Exception:
                pass
            self._auto_after = None
        if not self.auto_var.get():
            return
        try:
            secs = max(15, int(float(self.auto_secs.get())))
        except ValueError:
            secs = self.AUTO_REFRESH_DEFAULT_S

        def tick():
            self._auto_after = None
            self.refresh_status(manual=False)
            self._schedule_auto_refresh()

        self._auto_after = self.main_frame.after(secs * 1000, tick)

    def _browse_selected_job(self):
        rec = self._selected_job()
        if rec and self._profile_for(rec):
            self.profile_var.set(rec.profile)
            self._update_conn_status()
            self.browse(rec.remote_dir)

    def _view_selected_output(self):
        rec = self._selected_job()
        if not rec or not self._profile_for(rec):
            return
        self.profile_var.set(rec.profile)
        target = posixpath.join(rec.remote_dir, rec.output_file) if rec.output_file else None
        self.browse(rec.remote_dir, then_preview=target)

    def _download_selected_job(self):
        rec = self._selected_job()
        prof = rec and self._profile_for(rec)
        if not prof:
            return
        dest_root = filedialog.askdirectory(parent=self.main_frame.winfo_toplevel(),
                                            title="Download job folder into...",
                                            initialdir=rec.local_dir or os.path.expanduser("~"))
        if not dest_root:
            return
        entry = hb.RemoteEntry(posixpath.basename(rec.remote_dir.rstrip("/")), rec.remote_dir, True)

        def work():
            return hb.download_paths(self._conn(prof), [entry], dest_root,
                                     log=lambda m: self._ui(self.status_var.set, m))

        def done(paths):
            rec.local_dir = dest_root
            self.jobs.upsert(rec)
            messagebox.showinfo("Download", f"Downloaded {len(paths)} file(s) to\n"
                                            f"{os.path.join(dest_root, entry.name)}",
                                parent=self.main_frame.winfo_toplevel())

        self._bg(f"Downloading {rec.name}", work, done)

    def _cancel_selected_job(self):
        rec = self._selected_job()
        prof = rec and self._profile_for(rec)
        if not prof:
            return
        if rec.is_terminal:
            messagebox.showinfo("Cancel", f"This job is already {rec.state.lower()}.",
                                parent=self.main_frame.winfo_toplevel())
            return
        if not messagebox.askyesno("Cancel job",
                                   f"Cancel job {rec.scheduler_id} ('{rec.name}') on {prof.name}?",
                                   parent=self.main_frame.winfo_toplevel()):
            return
        self._bg(f"Cancelling {rec.scheduler_id}",
                 lambda: hb.cancel_job(self._conn(prof), prof, self.jobs, rec),
                 done=lambda _r: self._render_jobs())

    def _resubmit_selected_job(self):
        rec = self._selected_job()
        prof = rec and self._profile_for(rec)
        if not prof:
            return
        if not messagebox.askyesno("Resubmit",
                                   f"Submit '{rec.script}' again in the same folder?\n{rec.remote_dir}",
                                   parent=self.main_frame.winfo_toplevel()):
            return

        def work():
            conn = self._conn(prof)
            sched_id, reply = hb.get_scheduler(prof).submit(conn, rec.remote_dir, rec.script)
            new = hb.JobRecord(id=hb.uuid.uuid4().hex[:12], name=rec.name, profile=prof.name,
                               scheduler=prof.scheduler, remote_dir=rec.remote_dir, script=rec.script,
                               files=list(rec.files), output_file=rec.output_file, scheduler_id=sched_id,
                               state=hb.QUEUED, raw_state="submitted", submitted_at=hb._now(),
                               updated_at=hb._now(), note=reply[:500])
            self.jobs.upsert(new)
            return new

        self._bg(f"Resubmitting {rec.name}", work, done=lambda new: (
            self._render_jobs(), self.status_var.set(f"Resubmitted as job {new.scheduler_id}.")))

    def _remove_selected_job(self):
        rec = self._selected_job()
        if not rec:
            return
        msg = f"Remove '{rec.name}' from this list?\n\nNothing is deleted on the cluster."
        if not rec.is_terminal:
            msg += "\nThe job keeps running; it just won't be tracked here."
        if messagebox.askyesno("Remove", msg, parent=self.main_frame.winfo_toplevel()):
            self.jobs.remove(rec.id)
            self._render_jobs()

    # ---------------------------------------------------------- files
    def browse(self, path: str, quiet_missing: bool = False, fallback: Optional[str] = None,
               then_preview: Optional[str] = None):
        prof = self._selected_profile()
        if not prof:
            return
        path = (path or "").strip() or "~"

        def work():
            conn = self._conn(prof)
            target = conn.expand(path)
            try:
                return target, hb.list_remote_dir(conn, target)
            except hb.HPCError:
                if quiet_missing and fallback:
                    return fallback, hb.list_remote_dir(conn, fallback)
                raise

        def done(result):
            target, entries = result
            self.path_var.set(target)
            self._file_entries = {}
            self.files_tree.delete(*self.files_tree.get_children())
            for e in entries:
                iid = self.files_tree.insert(
                    "", tk.END, text=("[dir]  " if e.is_dir else "") + e.name,
                    values=("" if e.is_dir else _fmt_size(e.size), hb.fmt_time(e.mtime)))
                self._file_entries[iid] = e
            if then_preview:
                self.preview_file(then_preview)

        self._bg(f"Listing {path}", work, done)

    def _files_up(self):
        cur = self.path_var.get().rstrip("/")
        if cur:
            self.browse(posixpath.dirname(cur) or "/")

    def _open_file_entry(self):
        sel = self.files_tree.selection()
        if not sel:
            return
        e = self._file_entries.get(sel[0])
        if e is None:
            return
        if e.is_dir:
            self.browse(e.path)
        else:
            self.preview_file(e.path)

    def preview_file(self, path: str):
        prof = self._selected_profile()
        if not prof:
            return
        self._preview_path = path

        def done(result):
            text, size, truncated = result
            note = f"{posixpath.basename(path)}  ({_fmt_size(size)})"
            if truncated:
                note += "  - showing the end of the file"
            self.preview_title.set(note)
            self.preview.configure(state=tk.NORMAL)
            self.preview.delete("1.0", tk.END)
            self.preview.insert("1.0", text)
            self.preview.configure(state=tk.DISABLED)
            self.preview.see(tk.END)

        self._bg(f"Reading {posixpath.basename(path)}",
                 lambda: hb.read_remote_tail(self._conn(prof), path), done)

    def _reload_preview(self):
        if self._preview_path:
            self.preview_file(self._preview_path)

    def _download_selected_files(self):
        prof = self._selected_profile()
        if not prof:
            return
        entries = [self._file_entries[i] for i in self.files_tree.selection() if i in self._file_entries]
        if not entries:
            messagebox.showinfo("Download", "Select files or folders in the list first.",
                                parent=self.main_frame.winfo_toplevel())
            return
        dest = filedialog.askdirectory(parent=self.main_frame.winfo_toplevel(), title="Download into...")
        if not dest:
            return

        def work():
            return hb.download_paths(self._conn(prof), entries, dest,
                                     log=lambda m: self._ui(self.status_var.set, m))

        self._bg("Downloading", work, done=lambda paths: messagebox.showinfo(
            "Download", f"Downloaded {len(paths)} file(s) to\n{dest}", parent=self.main_frame.winfo_toplevel()))

    # ----------------------------------------------------- submission
    def open_submit_dialog(self, files: Sequence[str] = (), script: str = "", job_name: str = "",
                           ssh_prefill: Optional[dict] = None):
        """Public entry point (also used by Input Creator)."""
        if self.main_frame is None:
            self.create_ui()
        SubmitDialog(self, files=files, script=script, job_name=job_name, ssh_prefill=ssh_prefill)

    def _after_submit(self, rec: hb.JobRecord):
        self.profile_var.set(rec.profile)
        self._update_conn_status()
        self._render_jobs()
        if self.jobs_tree.exists(rec.id):
            self.jobs_tree.selection_set(rec.id)
            self.jobs_tree.see(rec.id)
        self.browse(rec.remote_dir)
        self.status_var.set(f"Submitted '{rec.name}' as job {rec.scheduler_id} on {rec.profile}.")


# ======================================================================
# Dialogs
# ======================================================================

class ProfileDialog(tk.Toplevel):
    """Add / edit a cluster. Anyone can describe their own HPC here."""

    def __init__(self, manager: JobManagerModule, profile: Optional[hb.ClusterProfile],
                 prefill: Optional[dict] = None, on_saved: Optional[Callable] = None):
        super().__init__(manager.main_frame.winfo_toplevel())
        self.manager = manager
        self.old_name = profile.name if profile else None
        self.on_saved = on_saved
        self.title("Edit cluster" if profile else "Add cluster")
        self.transient(manager.main_frame.winfo_toplevel())
        self.resizable(True, False)

        p = profile or hb.ClusterProfile(name="", **{k: v for k, v in (prefill or {}).items()
                                                      if k in hb.ClusterProfile.__dataclass_fields__ and k != "name"})
        if prefill and not profile and prefill.get("name"):
            p.name = prefill["name"]
        self.v = {
            "name": tk.StringVar(value=p.name),
            "connection": tk.StringVar(value="This computer" if p.connection == "local" else "SSH"),
            "host": tk.StringVar(value=p.host),
            "port": tk.StringVar(value=str(p.port)),
            "username": tk.StringVar(value=p.username),
            "key_path": tk.StringVar(value=p.key_path),
            "scheduler": tk.StringVar(value=p.scheduler),
            "remote_base_dir": tk.StringVar(value=p.remote_base_dir),
            "pre_command": tk.StringVar(value=p.pre_command),
            "submit_cmd": tk.StringVar(value=p.submit_cmd),
            "status_cmd": tk.StringVar(value=p.status_cmd),
            "cancel_cmd": tk.StringVar(value=p.cancel_cmd),
            "job_id_regex": tk.StringVar(value=p.job_id_regex),
            "notes": tk.StringVar(value=p.notes),
        }

        f = ttk.Frame(self, padding=12)
        f.pack(fill=tk.BOTH, expand=True)
        f.columnconfigure(1, weight=1)
        row = 0

        def add(label, widget, hint=""):
            nonlocal row
            ttk.Label(f, text=label).grid(row=row, column=0, sticky="w", pady=3, padx=(0, 8))
            widget.grid(row=row, column=1, sticky="ew", pady=3)
            if hint:
                ttk.Label(f, text=hint, font=("Segoe UI", 8, "italic")).grid(row=row, column=2, sticky="w", padx=(8, 0))
            row += 1
            return widget

        add("Name", ttk.Entry(f, textvariable=self.v["name"]), "e.g. IITR-HPC")
        conn_combo = add("Connect via", ttk.Combobox(f, textvariable=self.v["connection"], state="readonly",
                                                     values=["SSH", "This computer"]),
                         "'This computer' if AutoChemy runs on the login node")
        self.ssh_widgets = [
            add("Host / address", ttk.Entry(f, textvariable=self.v["host"]), "login node, e.g. hpc.iitr.ac.in"),
            add("Port", ttk.Entry(f, textvariable=self.v["port"], width=8), "usually 22"),
            add("Username", ttk.Entry(f, textvariable=self.v["username"])),
        ]
        key_row = ttk.Frame(f)
        ttk.Entry(key_row, textvariable=self.v["key_path"]).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(key_row, text="Browse", command=self._browse_key).pack(side=tk.LEFT, padx=(6, 0))
        self.ssh_widgets.append(add("SSH key (optional)", key_row, "leave empty to use password/OTP"))
        sched_combo = add("Scheduler", ttk.Combobox(f, textvariable=self.v["scheduler"], state="readonly",
                                                    values=list(hb.SCHEDULERS)), "SLURM = sbatch, PBS = qsub")
        add("Remote job folder", ttk.Entry(f, textvariable=self.v["remote_base_dir"]),
            "each job gets a sub-folder here")
        add("Run before commands", ttk.Entry(f, textvariable=self.v["pre_command"]),
            "optional, e.g. module load slurm")

        self.custom_box = ttk.LabelFrame(f, text="Custom scheduler commands", padding=(8, 4))
        self.custom_box.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(6, 3))
        row += 1
        self.custom_box.columnconfigure(1, weight=1)
        for i, (label, key, hint) in enumerate((
                ("Submit", "submit_cmd", "{script} = script file name"),
                ("Status", "status_cmd", "{job_id}; output must contain the state"),
                ("Cancel", "cancel_cmd", "{job_id}"),
                ("Job ID pattern", "job_id_regex", "regex; first group = ID"))):
            ttk.Label(self.custom_box, text=label).grid(row=i, column=0, sticky="w", pady=2, padx=(0, 8))
            ttk.Entry(self.custom_box, textvariable=self.v[key]).grid(row=i, column=1, sticky="ew", pady=2)
            ttk.Label(self.custom_box, text=hint, font=("Segoe UI", 8, "italic")).grid(row=i, column=2, sticky="w", padx=(8, 0))
        add("Notes", ttk.Entry(f, textvariable=self.v["notes"]))

        ttk.Label(f, text="Passwords and OTP codes are asked when you connect and are never saved.",
                  font=("Segoe UI", 8, "italic")).grid(row=row, column=0, columnspan=3, sticky="w", pady=(6, 0))
        row += 1

        btns = ttk.Frame(f)
        btns.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(12, 0))
        self.test_status = tk.StringVar(value="")
        ttk.Button(btns, text="Test connection", command=self._test).pack(side=tk.LEFT)
        ttk.Button(btns, text="Forget host key", command=self._forget_key).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Label(btns, textvariable=self.test_status, font=("Segoe UI", 9, "italic")).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(btns, text="Save", command=self._save).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side=tk.RIGHT, padx=(0, 6))

        conn_combo.bind("<<ComboboxSelected>>", lambda _e: self._sync())
        sched_combo.bind("<<ComboboxSelected>>", lambda _e: self._sync())
        self._sync()
        self.grab_set()

    def _sync(self):
        ssh = self.v["connection"].get() == "SSH"
        for w in self.ssh_widgets:
            for child in [w] + list(w.winfo_children()):
                try:
                    child.configure(state=tk.NORMAL if ssh else tk.DISABLED)
                except tk.TclError:
                    pass
        if self.v["scheduler"].get() == "Custom":
            self.custom_box.grid()
        else:
            self.custom_box.grid_remove()

    def _browse_key(self):
        path = filedialog.askopenfilename(parent=self, title="Select SSH private key",
                                          initialdir=os.path.expanduser("~/.ssh"))
        if path:
            self.v["key_path"].set(path)

    def _profile(self) -> hb.ClusterProfile:
        try:
            port = int(self.v["port"].get().strip() or "22")
        except ValueError:
            port = 0
        return hb.ClusterProfile(
            name=self.v["name"].get().strip(),
            connection="local" if self.v["connection"].get() == "This computer" else "ssh",
            host=self.v["host"].get().strip(), port=port,
            username=self.v["username"].get().strip(), key_path=self.v["key_path"].get().strip(),
            scheduler=self.v["scheduler"].get(), remote_base_dir=self.v["remote_base_dir"].get().strip(),
            pre_command=self.v["pre_command"].get().strip(), submit_cmd=self.v["submit_cmd"].get().strip(),
            status_cmd=self.v["status_cmd"].get().strip(), cancel_cmd=self.v["cancel_cmd"].get().strip(),
            job_id_regex=self.v["job_id_regex"].get().strip() or r"(\d+)", notes=self.v["notes"].get().strip(),
        )

    def _save(self):
        prof = self._profile()
        problems = prof.validate()
        if prof.name != self.old_name and self.manager.profiles.get(prof.name):
            problems.append(f"A cluster named '{prof.name}' already exists.")
        if problems:
            messagebox.showerror("Cluster", "\n".join(problems), parent=self)
            return
        if self.old_name:
            self.manager.pool.drop(self.old_name)
        self.manager.profiles.upsert(prof, old_name=self.old_name)
        self.manager._reload_profiles(select=prof.name)
        self.destroy()
        if self.on_saved:
            self.on_saved(prof)

    def _test(self):
        prof = self._profile()
        problems = [p for p in prof.validate() if "name" not in p.lower()]
        if problems:
            messagebox.showerror("Cluster", "\n".join(problems), parent=self)
            return
        check = {"SLURM": "sbatch", "PBS": "qsub", "SGE": "qsub", "LSF": "bsub"}.get(prof.scheduler, "")
        cmd = "echo HOST=$(hostname); echo HOME=$HOME"
        if check:
            cmd += f"; command -v {check} >/dev/null && echo SCHED=ok || echo SCHED=missing"
        sched = hb.get_scheduler(prof)

        def work():
            if prof.connection == "local":
                conn = hb.LocalConnection()
            else:
                conn = hb.SSHConnection(prof, prompt=self.manager._prompt_from_worker,
                                        confirm=self.manager._confirm_from_worker).connect()
            try:
                return conn.run(sched._wrap(cmd), timeout=60)
            finally:
                if prof.connection != "local":
                    conn.close()

        def done(res):
            rc, out, err = res
            info = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
            text = f"Logged in OK - host {info.get('HOST', '?')}"
            if info.get("SCHED") == "missing":
                text += f".  '{check}' not found: set 'Run before commands' (e.g. module load ...)."
            elif info.get("SCHED") == "ok":
                text += f", '{check}' found."
            if self.winfo_exists():
                self.test_status.set(text)

        def failed(exc):
            if self.winfo_exists():
                self.test_status.set("Test failed.")

        self.test_status.set("Testing ...")
        self.manager._bg("Testing connection", work, done, failed)

    def _forget_key(self):
        prof = self._profile()
        if hb.forget_host_key(prof):
            messagebox.showinfo("Host key", f"Forgot the saved key for {prof.host}. "
                                            "You'll be asked to trust it on next connect.", parent=self)
        else:
            messagebox.showinfo("Host key", f"No key saved by AutoChemy for {prof.host}.\n"
                                            "(Keys in ~/.ssh/known_hosts are managed by 'ssh-keygen -R'.)",
                                parent=self)


class SubmitDialog(tk.Toplevel):
    """Pick cluster + files + batch script, check the script, submit."""

    def __init__(self, manager: JobManagerModule, files: Sequence[str] = (), script: str = "",
                 job_name: str = "", ssh_prefill: Optional[dict] = None):
        super().__init__(manager.main_frame.winfo_toplevel())
        self.manager = manager
        self.ssh_prefill = ssh_prefill or {}
        self.title("Submit job to HPC")
        self.transient(manager.main_frame.winfo_toplevel())
        self.geometry("760x620")
        self.files: List[str] = []
        self._script_original = ""
        self._script_loaded_for = ""

        f = ttk.Frame(self, padding=12)
        f.pack(fill=tk.BOTH, expand=True)

        top = ttk.Frame(f)
        top.pack(fill=tk.X)
        ttk.Label(top, text="Cluster").grid(row=0, column=0, sticky="w")
        self.profile_var = tk.StringVar()
        self.profile_combo = ttk.Combobox(top, textvariable=self.profile_var, state="readonly", width=40)
        self.profile_combo.grid(row=0, column=1, sticky="w", padx=6)
        ttk.Button(top, text="Add cluster", command=self._add_cluster).grid(row=0, column=2, sticky="w")
        ttk.Label(top, text="Job name").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.name_var = tk.StringVar(value=job_name)
        ttk.Entry(top, textvariable=self.name_var, width=42).grid(row=1, column=1, sticky="w", padx=6, pady=(6, 0))
        self.hint_var = tk.StringVar(value="")
        ttk.Label(f, textvariable=self.hint_var, foreground="#b7791f", wraplength=720).pack(fill=tk.X, pady=(6, 0))

        fl = ttk.LabelFrame(f, text="Files to upload (input, structure, batch script ...)", padding=(8, 6))
        fl.pack(fill=tk.X, pady=(8, 6))
        self.files_list = tk.Listbox(fl, height=5, selectmode=tk.EXTENDED)
        self.files_list.pack(side=tk.LEFT, fill=tk.X, expand=True)
        fb = ttk.Frame(fl)
        fb.pack(side=tk.LEFT, padx=(8, 0), fill=tk.Y)
        ttk.Button(fb, text="Add files...", command=self._add_files).pack(fill=tk.X)
        ttk.Button(fb, text="Remove", command=self._remove_files).pack(fill=tk.X, pady=(6, 0))

        sf = ttk.LabelFrame(f, text="Batch script (you can edit it before submitting)", padding=(8, 6))
        sf.pack(fill=tk.BOTH, expand=True)
        srow = ttk.Frame(sf)
        srow.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(srow, text="Script file:").pack(side=tk.LEFT)
        self.script_var = tk.StringVar()
        self.script_combo = ttk.Combobox(srow, textvariable=self.script_var, state="readonly", width=40)
        self.script_combo.pack(side=tk.LEFT, padx=6)
        self.script_combo.bind("<<ComboboxSelected>>", lambda _e: self._load_script())
        self.script_text = tk.Text(sf, height=12, font=("Consolas", 9), wrap="none", undo=True)
        ssb = ttk.Scrollbar(sf, orient=tk.VERTICAL, command=self.script_text.yview)
        self.script_text.configure(yscrollcommand=ssb.set)
        self.script_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ssb.pack(side=tk.LEFT, fill=tk.Y)
        manager._editors.append(self.script_text)

        self.log = tk.Text(f, height=5, font=("Consolas", 9), state=tk.DISABLED)
        self.log.pack(fill=tk.X, pady=(8, 0))
        manager._editors.append(self.log)

        btns = ttk.Frame(f)
        btns.pack(fill=tk.X, pady=(10, 0))
        self.btn_submit = ttk.Button(btns, text="Submit", command=self._submit)
        self.btn_submit.pack(side=tk.RIGHT)
        ttk.Button(btns, text="Close", command=self.destroy).pack(side=tk.RIGHT, padx=(0, 6))

        self._reload_profiles()
        for p in files:
            self._add_path(p)
        if script:
            base = os.path.basename(script)
            if base in self.script_combo["values"]:
                self.script_var.set(base)
        self._load_script()
        try:
            from modules import app_theme
            top_app = getattr(manager.main_frame.winfo_toplevel(), "_orca_app", None)
            if top_app is not None:
                ctx = app_theme.build_context(top_app.theme_mode)
                for w in (self.script_text, self.log):
                    app_theme.apply_editor_style(w, ctx)
        except Exception:
            pass

    # profiles -----------------------------------------------------------
    def _reload_profiles(self, select: Optional[str] = None):
        store = self.manager.profiles
        names = store.names()
        self.profile_combo["values"] = names
        pre = self.ssh_prefill
        match = store.find(pre.get("host", ""), pre.get("username", "")) if pre.get("host") else None
        choice = select or (match.name if match else None) or self.manager.profile_var.get()
        self.profile_var.set(choice if choice in names else (names[0] if names else ""))
        if not names:
            self.hint_var.set("No cluster yet - click 'Add cluster' and enter your HPC address.")
        elif pre.get("host") and not match and not select:
            self.hint_var.set(f"{pre.get('username', '')}@{pre['host']} from Input Creator isn't saved as a cluster "
                              "yet. Click 'Add cluster' to save it (fields are pre-filled).")
        else:
            self.hint_var.set("")

    def _add_cluster(self):
        prefill = None
        if self.ssh_prefill.get("host"):
            prefill = dict(self.ssh_prefill)
            prefill.setdefault("name", prefill["host"].split(".")[0].upper())
        self.manager._add_profile(prefill=prefill, on_saved=lambda p: self._reload_profiles(select=p.name))

    # files --------------------------------------------------------------
    def _add_path(self, path: str):
        path = os.path.abspath(path)
        if path in self.files or not os.path.isfile(path):
            return
        if os.path.basename(path) in [os.path.basename(x) for x in self.files]:
            messagebox.showwarning("Files", f"A file named {os.path.basename(path)} is already in the list.", parent=self)
            return
        self.files.append(path)
        self.files_list.insert(tk.END, path)
        self._update_script_choices()

    def _add_files(self):
        for p in filedialog.askopenfilenames(parent=self, title="Select files to upload"):
            self._add_path(p)
        if not self.name_var.get().strip() and self.files:
            self.name_var.set(os.path.splitext(os.path.basename(self.files[0]))[0])
        self._load_script()

    def _remove_files(self):
        for idx in reversed(self.files_list.curselection()):
            self.files_list.delete(idx)
            del self.files[idx]
        self._update_script_choices()
        self._load_script()

    def _update_script_choices(self):
        names = [os.path.basename(p) for p in self.files]
        scripts = [n for n in names if n.lower().endswith(SCRIPT_EXTS)] or names
        self.script_combo["values"] = scripts
        if self.script_var.get() not in scripts:
            self.script_var.set(scripts[0] if scripts else "")

    def _script_path(self) -> Optional[str]:
        name = self.script_var.get()
        for p in self.files:
            if os.path.basename(p) == name:
                return p
        return None

    def _load_script(self):
        path = self._script_path()
        if path == self._script_loaded_for:
            return
        self._script_loaded_for = path or ""
        self.script_text.delete("1.0", tk.END)
        self._script_original = ""
        if path:
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    self._script_original = fh.read()
            except OSError as exc:
                self._script_original = f"# could not read {path}: {exc}"
            self.script_text.insert("1.0", self._script_original)
        self.script_text.edit_reset()

    # submit -------------------------------------------------------------
    def _say(self, msg: str):
        if not self.winfo_exists():
            return
        self.log.configure(state=tk.NORMAL)
        self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    def _warn_script(self, prof: hb.ClusterProfile, text: str) -> Optional[str]:
        t = text.lower()
        if prof.scheduler == "SLURM" and "#pbs" in t and "#sbatch" not in t:
            return "This script has #PBS lines but the cluster uses SLURM."
        if prof.scheduler == "PBS" and "#sbatch" in t and "#pbs" not in t:
            return "This script has #SBATCH lines but the cluster uses PBS."
        if "{job_name}" in text or "{nprocs}" in text:
            return "The script still contains template placeholders like {job_name}."
        return None

    def _submit(self):
        prof = self.manager.profiles.get(self.profile_var.get())
        if prof is None:
            messagebox.showerror("Submit", "Choose or add a cluster first.", parent=self)
            return
        if not self.files:
            messagebox.showerror("Submit", "Add the files to upload (at least the batch script).", parent=self)
            return
        script_path = self._script_path()
        if not script_path:
            messagebox.showerror("Submit", "Pick which file is the batch script.", parent=self)
            return
        job_name = self.name_var.get().strip() or os.path.splitext(os.path.basename(script_path))[0]
        text = self.script_text.get("1.0", "end-1c")
        warn = self._warn_script(prof, text)
        if warn and not messagebox.askyesno("Check script", warn + "\n\nSubmit anyway?", parent=self):
            return

        files = list(self.files)
        if text != self._script_original:
            # user edited the script: upload an edited copy, leave their file alone
            staged = hb.stage_files(job_name, {os.path.basename(script_path): text.encode("utf-8")})
            files[files.index(script_path)] = staged[0]
            self._say("Using your edited script (original file not changed).")

        self.btn_submit.configure(state=tk.DISABLED)
        say = lambda m: self.manager._ui(self._say, m)  # noqa: E731

        def work():
            conn = self.manager._conn(prof)
            return hb.submit_job(conn, prof, self.manager.jobs, files,
                                 os.path.basename(script_path), job_name, log=say)

        def done(rec):
            self._say(f"Done. Job {rec.scheduler_id} is in the queue; follow it in the Jobs list.")
            self.manager._after_submit(rec)
            if self.winfo_exists():
                self.btn_submit.configure(state=tk.NORMAL)

        def failed(exc):
            self.manager._render_jobs()
            if self.winfo_exists():
                self._say(f"Failed: {exc}")
                self.btn_submit.configure(state=tk.NORMAL)

        self.manager._bg(f"Submitting {job_name} to {prof.name}", work, done, failed)
