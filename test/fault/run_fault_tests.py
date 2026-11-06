#!/usr/bin/env python3
"""Fault-injection tests of the scheduler's worker liveness, on the LinearTransform problem. See README.md.

Runs inside a Slurm allocation, on one machine, with two pretend nodes: the master's own, and one reached through a
stand-in ssh, so that hyperion takes its remote launch path. Every process it kills runs this experiment's executable
or is that executable's child, in this allocation.
"""
import argparse, json, os, re, shutil, signal, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
LONG = 3607   # seconds of the native task a test kills or leaves behind: distinctive, so it can be found
HANG = 86407  # inject.sh's hang


def arm(name, want, why, env=None, kill=None, text=None, limit=15, slow=None, wait=150, retry=False, every=False,
        workers_stay=False):
    return dict(name=name, want=want, why=why, env=env or {}, kill=kill, text=text, limit=limit, slow=slow, wait=wait,
                retry=retry, every=every, workers_stay=workers_stay)


# Order of launch inside the job, with two nodes: the second node's launcher is 0, then each service for the master's
# node and then for the pretend node. These are the pretend node's.
SERVICES = {"filesender": r"/0/2\.log$", "manager": r"/0/4\.log$", "supervisor": r"/0/6\.log$"}
SLOT = r"fakeb\.0\.log$"
SUPERVISOR_LOG = r"/0/6\.log$"  # A reusable worker has the slot's log too; this is who launches the slot's own workers.
OFF = {"HYPERION_SCHEDULER_NODE_SUPERVISORS": "0"}


def fault(kind, log_re, **more):
    d = {"FAULT_KIND": kind, "FAULT_LOG_RE": log_re}
    d.update({k: str(v) for k, v in more.items()})
    return d


ARMS = [
    arm("nofault", "success", "control: no fault"),
    arm("nofault-nosup", "success", "control: no fault, the master launches task workers", env=OFF),
    arm("long-task", "success", "control: a healthy native task runs three times the connect limit", slow=45),
    arm("long-task-nosup", "success", "the same, the master launches task workers", env=OFF, slow=45),
    arm("slot-exit1", "success", "a task worker exits before connecting: relaunched",
        env=fault("exit1", SLOT, FAULT_PARENT_RE=SUPERVISOR_LOG)),
    arm("slot-vanish", "success", "a task worker leaves an empty log, exit 0: relaunched at the limit",
        env=fault("vanish", SLOT, FAULT_PARENT_RE=SUPERVISOR_LOG)),
    arm("slot-badport", "success", "a task worker's real 'Couldn't connect to master': relaunched",
        env=fault("badport", SLOT, FAULT_PARENT_RE=SUPERVISOR_LOG), limit=120),
    arm("slot-hang-3x", "fail", "three launches in a row on one slot hang: the run fails",
        env=fault("hang", SLOT, FAULT_PARENT_RE=SUPERVISOR_LOG, FAULT_COUNT=3), text=r"cannot start a worker"),
    arm("filesender-vanish", "fail", "job 55544734's shape: a FileSender's worker leaves an empty log",
        env=fault("vanish", SERVICES["filesender"]), text=r"FileSender.*did not start"),
    arm("manager-hang", "fail", "a NodeLocalFileManager's worker never connects",
        env=fault("hang", SERVICES["manager"]), text=r"NodeLocalFileManager.*did not start"),
    arm("supervisor-hang", "fail", "a NodeSupervisor's worker never connects",
        env=fault("hang", SERVICES["supervisor"]), text=r"NodeSupervisor.*did not start"),
    arm("reusable-hang", "fail", "a ReusableWorker's worker never connects",
        env=fault("hang", r"fakeb\.[0-9]+\.log$", FAULT_PARENT_RE=r"/0/0\.log$"), text=r"ReusableWorker.*did not start"),
    arm("nosup-badport", "fail", "job 55556183's shape: the master launches, the worker can't connect",
        env={**OFF, **fault("badport", SLOT, FAULT_AT=2)}, text=r"WorkerConnectionTimeout"),
    arm("launcher-hang", "success", "the pretend node's launcher never connects: hyperion's own nodeLauncherTimeoutRetries, "
        "set by this test's site, launches it again",
        env=fault("hang", r"/0/0\.log$")),
    arm("task-kill", "fail", "SIGKILL of a worker in a native task on the pretend node",
        kill="task", slow=LONG, every=True, text=r"worker died|Task failed on slot"),
    arm("task-kill-retry", "success", "the same, then hyperion's retry on the held job: the answer must be right",
        kill="task", slow=LONG, every=True, text=r"worker died|Task failed on slot", retry=True),
    arm("task-kill-short", "fail", "the same with a 40 s native: when is it noticed?",
        kill="task", slow=40, every=True, text=r"worker died|Task failed on slot"),
    arm("task-kill-nosup", "fail", "the same, the master launches task workers",
        env=OFF, kill="task", slow=40, every=True, text=r"RemoteError|Died|died"),
    arm("ready-kill", "ended", "SIGKILL of an idle ready worker on the pretend node: no hang and no wrong answer",
        kill="ready", slow=40),
    arm("reusable-kill", "ended", "SIGKILL of a reusable worker on the pretend node: no hang and no wrong answer",
        kill="reusable", slow=40),
    arm("launcher-kill", "ended", "SIGKILL of the pretend node's launcher: no hang and no wrong answer",
        kill="launcher", slow=40),
    arm("manager-kill", "fail", "SIGKILL of a NodeLocalFileManager's worker in the run",
        kill="manager", slow=40, text=r"NodeLocalFileManager.*lost"),
    arm("filesender-kill", "fail", "SIGKILL of a FileSender's worker in the run",
        kill="filesender", slow=40, text=r"FileSender.*lost"),
    arm("supervisor-kill", "fail", "SIGKILL of a NodeSupervisor's worker in the run: nothing is left to stop its workers",
        kill="supervisor", slow=40, text=r"NodeSupervisor.*(lost|died)", workers_stay=True),
]


def cgroup_of(pid):
    try:
        return Path(f"/proc/{pid}/cgroup").read_text()
    except OSError:
        return ""


def cmdline(pid):
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip()
    except OSError:
        return ""


def ppid(pid):
    try:
        return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[1])
    except (OSError, IndexError, ValueError):
        return 0


def mine(armdir, job):
    """Processes of this arm: its executable's copy or a log of its own in the command line, in this allocation."""
    out = {}
    tag = f"job_{job}"
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        pid = int(d)
        if pid == os.getpid() or tag not in cgroup_of(pid):
            continue
        c = cmdline(pid)
        if str(armdir) in c or re.fullmatch(rf"sleep ({LONG}|{HANG})", c) or re.fullmatch(r"sleep \d+", c):
            out[pid] = c
    return out


def kill_worker_in_task(armdir, job, seconds):
    """A worker on a pretend node that is the parent of a long native task. The master's own node differs: there
    hyperion links the launch to the master, so a worker's exit is noticed whatever the scheduler does."""
    for pid, c in mine(armdir, job).items():
        if re.fullmatch(rf"sleep {seconds}", c):
            parent = ppid(pid)
            if str(armdir) in cmdline(parent) and re.search(r"--logFile \S*/fake[a-z]\.\d+\.log", cmdline(parent)):
                os.kill(parent, signal.SIGKILL)
                return parent
    return None


def kill_helper(armdir, job, parent_re):
    """An idle worker on the pretend node with the given launcher: one with no child, so not in a native task."""
    procs = mine(armdir, job)
    parents = {ppid(p) for p in os.listdir("/proc") if p.isdigit()}
    for pid, c in procs.items():
        if " worker " in c and re.search(r"--logFile \S*/fakeb\.\d+\.log", c) and pid not in parents \
                and re.search(parent_re, cmdline(ppid(pid))):
            os.kill(pid, signal.SIGKILL)
            return pid
    return None


def kill_launcher(armdir, job):
    """The pretend node's launcher, the first worker of the job."""
    for pid, c in mine(armdir, job).items():
        if " worker " in c and re.search(r"--logFile \S*/0/0\.log( |$)", c):
            os.kill(pid, signal.SIGKILL)
            return pid
    return None


def service_log(armdir, role):
    """A service's worker log: the launch line after its start line, for the pretend node."""
    start = {"manager": r'Start remote NodeLocalFileManager: RemoteAddr "fakeb"',
             "filesender": r'Start remote FileSender: RemoteAddr "fakeb"',
             "supervisor": None}[role]
    logs = sorted(Path(armdir).glob("logs/*/*/0.log"))
    if not logs:
        return None
    lines = logs[0].read_text(errors="replace").splitlines()
    deployed = [i for i, l in enumerate(lines) if "Deployed worker" in l]
    sid = None
    if start:
        for i, l in enumerate(lines):
            if re.search(start, l):
                after = [j for j in deployed if j > i]
                if after:
                    sid = re.search(r'ServiceId "([^"]+)"', lines[after[0]]).group(1)
    else:  # the supervisors start last, the pretend node's second
        starts = [i for i, l in enumerate(lines) if "Started NodeSupervisor" in l]
        if len(starts) >= 2:
            before = [j for j in deployed if j < starts[1]]
            sid = re.search(r'ServiceId "([^"]+)"', lines[before[-1]]).group(1)
    return sid


def kill_service(armdir, job, role):
    sid = service_log(armdir, role)
    if sid is None:
        return None
    for pid, c in mine(armdir, job).items():
        if re.search(rf"--logFile \S*/{re.escape(sid)}\.log( |$)", c) and " worker " in c:
            os.kill(pid, signal.SIGKILL)
            return pid
    return None


def read_logs(armdir):
    text = ""
    for f in Path(armdir).glob("logs/**/*.log"):
        try:
            text += f.read_text(errors="replace")
        except OSError:
            pass
    return text


def run_arm(a, args, job):
    armdir = Path(args.out) / a["name"]
    # The first pretend node is the one faults are aimed at; the others only widen the run.
    nodes = ["agentbox", "fakeb"] + [f"fake{chr(ord('c') + i)}" for i in range(args.pretend_nodes - 1)]
    shutil.rmtree(armdir, ignore_errors=True)
    (armdir / "fault").mkdir(parents=True)
    done_file = armdir / "slow-ran"
    env = {k: v for k, v in os.environ.items() if not k.startswith("FAULT_")}
    env.update({
        "PATH": f"{HERE}/bin:{env['PATH']}",
        "HYPERION_SCHEDULER_TEST_SITE": "local-slurm",
        "HYPERION_SCHEDULER_TEST_WORKER_WRAPPER": str(HERE / "inject.sh"),
        "HYPERION_SCHEDULER_TEST_LOCAL_STORAGE": str(armdir / "node_local"),
        "HYPERION_SCHEDULER_WORKER_CONNECT_TIMEOUT": str(a["limit"]),
        "FAULT_DIR": str(armdir / "fault"),
        "FAULT_NODES": " ".join(nodes),
        "SLURM_JOB_ID": str(job),
        "SLURM_NTASKS_PER_NODE": "2",
        "SLURM_JOB_NUM_NODES": str(len(nodes)),
        "SLURM_JOB_NODELIST": ",".join(nodes),
        "SLURMD_NODENAME": "agentbox",
    })
    env.pop("HYPERION_SCHEDULER_NODE_SUPERVISORS", None)
    env.update(a["env"])
    if a["slow"]:
        env["HYPERION_SCHEDULER_TEST_SLOW_ELEMENT"] = f"1:{'*' if a['every'] else 0}:{a['slow']}"
        env["HYPERION_SCHEDULER_TEST_SLOW_ELEMENT_DONE"] = str(done_file)
    cmd = [args.exe, "master", "--shift", "2", "--dim", "3", "--nodes", str(len(nodes)), "--ntasks-per-node", "2",
           "--time", "60", "--mem", "1G", "--base-dir", str(armdir)]
    t0 = time.time()
    with open(armdir / "master.out", "w") as out:
        master = subprocess.Popen(cmd, env=env, stdout=out, stderr=subprocess.STDOUT, cwd=armdir)
    fault_t, killed, outcome, detect_t, held_t = None, None, None, None, None
    deadline = t0 + a["wait"] + (a["slow"] if a["slow"] and a["slow"] != LONG else 0)
    while time.time() < deadline:
        now = time.time()
        if a["kill"] and killed is None and now - t0 > 2:
            if a["kill"] == "task":
                killed = kill_worker_in_task(armdir, job, a["slow"])
            elif any(re.fullmatch(r"sleep \d+", c) for c in mine(armdir, job).values()):
                # In the run: a task is running, and the services are up.
                if a["kill"] == "launcher":
                    killed = kill_launcher(armdir, job)
                elif a["kill"] == "ready":
                    killed = kill_helper(armdir, job, SUPERVISOR_LOG)
                elif a["kill"] == "reusable":
                    killed = kill_helper(armdir, job, r"/0/0\.log")
                else:
                    killed = kill_service(armdir, job, a["kill"])
            if killed:
                fault_t = time.time()
        rc = master.poll()
        if rc is not None:
            outcome, detect_t = ("success" if rc == 0 else f"master-exit-{rc}"), time.time()
            break
        held = "".join(f.read_text(errors="replace") for f in Path(armdir).glob("logs/*/*.log"))
        if "put on hold because of an error" in held and held_t is None:
            held_t = time.time()
            if a["retry"]:
                # The long task isn't to run again, and its orphan isn't the point here.
                done_file.write_text("killed once\n")
                for pid, c in mine(armdir, job).items():
                    if re.fullmatch(rf"sleep {LONG}", c):
                        os.kill(pid, signal.SIGKILL)
                url = re.findall(r"localhost:\d+/retry/[A-Za-z0-9]+", held)[-1]
                subprocess.run(["curl", "-s", "-m", "30", f"http://{url}"], stdout=subprocess.DEVNULL, check=False)
            else:
                outcome, detect_t = "fail", held_t
                break
        time.sleep(0.5)
    if outcome is None:
        outcome, detect_t = "hung", None
    records = (armdir / "fault" / "faults.txt")
    if fault_t is None and records.exists():
        fault_t = float(records.read_text().split()[0])
    # Who is left after the run has ended, once it has had time to shut down.
    time.sleep(args.settle)
    left = {p: c for p, c in mine(armdir, job).items() if p != master.pid}
    native = [c for c in left.values() if c.startswith("sleep ")]
    workers = [c for c in left.values() if " worker " in c]
    left_logs = sorted(re.sub(r".*--logFile \S*/", "", c) for c in workers)
    logs = read_logs(armdir)
    errors = [l[:300] for l in logs.splitlines() if re.search(r"ERROR|did not start|lost:|cannot start a worker", l)]
    text_ok = bool(re.search(a["text"], logs)) if a["text"] else None
    wrong = "Wrong output vector" in logs
    computed = len(re.findall(r"Computed output vector", logs))
    # Clean up: only this arm's processes in this allocation.
    if master.poll() is None:
        master.terminate()
    time.sleep(1)
    for p in mine(armdir, job):
        try:
            os.kill(p, signal.SIGKILL)
        except OSError:
            pass
    verdict = {
        "arm": a["name"], "why": a["why"], "want": a["want"], "got": outcome, "wall_s": round(time.time() - t0, 1),
        "fault_to_end_s": round((held_t or detect_t) - fault_t, 1) if ((held_t or detect_t) and fault_t) else None,
        "start_to_end_s": round(detect_t - t0, 1) if detect_t else None,
        "faults_recorded": records.read_text().count("\n") if records.exists() else 0,
        "killed_pid": killed, "diagnostic_found": text_ok, "wrong_output": wrong, "passes_computed": computed,
        "left_after_settle": {"workers": len(workers), "native": len(native)}, "left_logs": left_logs,
        "errors": errors[:6],
    }
    return verdict


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--exe", required=True, help="hyperion-scheduler-test")
    ap.add_argument("--out", required=True, help="an empty or disposable folder, not on /tmp")
    ap.add_argument("--arms", default="", help="comma-separated names; all by default")
    ap.add_argument("--settle", type=float, default=40, help="seconds to wait before counting what is left; a worker that can't connect gives up after about 53 s")
    ap.add_argument("--json", default="", help="write the verdicts here")
    ap.add_argument("--pretend-nodes", type=int, default=1, help="how many pretend nodes; the service-start cases assume 1")
    args = ap.parse_args()
    args.exe = str(Path(args.exe).resolve())
    args.out = str(Path(args.out).resolve())
    job = os.environ.get("SLURM_JOB_ID")
    if not job:
        sys.exit("run this inside a Slurm allocation: it kills processes only there")
    chosen = set(filter(None, args.arms.split(",")))
    results = []
    for a in ARMS:
        if chosen and a["name"] not in chosen:
            continue
        v = run_arm(a, args, job)
        ended = v["got"] in ("success", "fail")
        v["pass"] = (v["got"] == a["want"] or (a["want"] == "ended" and ended)) and not v["wrong_output"] \
            and v["diagnostic_found"] is not False and (v["got"] != "success" or v["passes_computed"] == 2) \
            and (a["workers_stay"] or v["got"] == "hung" or v["left_after_settle"]["workers"] == 0)
        results.append(v)
        print(json.dumps(v), flush=True)
        if args.json:
            Path(args.json).write_text(json.dumps(results, indent=1))
    passed = sum(v["pass"] for v in results)
    print(f"{passed} of {len(results)} arms as wanted")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
