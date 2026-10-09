#!/usr/bin/env python3
"""
Mississippi full harvest by title-prefix trie. Repaired 2026-09-13.

WHAT WAS WRONG WITH THE MARCH VERSION
  1. It measured with `title,begins_with,<prefix>*`. Ex Libris changed wildcard
     handling after 2026-03-30: the `*` form now returns ~1% of the true count
     (a -> 86 instead of 9,385), so every planned bucket was wrong, downward,
     silently.
  2. THRESHOLD was 5,000. The gateway now refuses offset+limit > 500, so any
     bucket over 500 was truncated with no error.
  3. An empty page was treated as "bucket exhausted". The endpoint intermittently
     returns HTTP 200 with docs:[] -- reproduced 2026-09-13 -- which silently
     retired prefixes mid-harvest. This is the likely cause of March's 7,843
     shortfall, not the `[remainder]` gap.

WHAT IS NOT WRONG (measured, so nobody re-litigates it)
  - `begins_with` without the `*` is a true CHARACTER prefix here (l 21,406 ->
    le 18,626 -> letter 17,252), unlike Getty where it matches whole words.
  - The `[remainder]` is ~0. Children reconcile to their parent: letter 17,252 =
    143 direct + 17,103 spaced, remainder 6. Punctuation is stripped from the
    index as well as from queries, so "Letter, 1863" normalises to "letter 1863"
    and the space-extension already reaches it.
  - Root [a-z0-9] covers 138,781 of 138,818 -- residual 37 (0.03%).
"""
import json, os, queue, re, sys, threading, time, urllib.parse, urllib.request
from pathlib import Path

KEY="l8xx87aeef957145450eaf117a6c1f0d8c71"
API="https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VIEW={"vid":"01USM_INST:MDL","tab":"MDL","scope":"MDL","lang":"eng"}

CHARS      = list("abcdefghijklmnopqrstuvwxyz0123456789")
THRESHOLD  = 450          # gateway refuses offset+limit > 500
PAGE       = 100
OFFSETS    = (0, 100, 200, 300, 400)   # deepest record reached = 500
WORKERS    = 6
REST       = 0.4          # measured 8.8/s at 6 workers
MAX_DEPTH  = 30   # 12 stranded 7,068 records in 10 nodes at 14-15 chars
EMPTY_RETRY= 3            # an empty page is retried, never trusted first time

D = Path(os.environ.get("MS_DIR", "/home/ec2-user/mississippi-2026"))
D.mkdir(parents=True, exist_ok=True)
PLAN, OUT, DONE, LOG = D/"plan.json", D/"harvest.jsonl", D/"done.txt", D/"run.log"

def log(msg):
    line=f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with LOG.open("a") as fh: fh.write(line+"\n")

def notify(text):
    creds=Path("/home/ec2-user/.getty-slack")
    if not creds.exists(): return
    env=dict(l.split("=",1) for l in creds.read_text().splitlines() if "=" in l and not l.startswith("#"))
    tok,usr=env.get("SLACK_BOT_TOKEN","").strip(), env.get("SLACK_ALERT_USER_ID","").strip()
    if not (tok and usr): return
    try:
        urllib.request.urlopen(urllib.request.Request(
            "https://slack.com/api/chat.postMessage",
            data=json.dumps({"channel":usr,"text":text}).encode(),
            headers={"Authorization":"Bearer "+tok,"Content-type":"application/json"}), timeout=20).read()
    except Exception: pass

def call(params, tries=4):
    p=dict(VIEW); p.update({"apikey":KEY}); p.update(params)
    url=API+"?"+urllib.parse.urlencode(p)
    for a in range(tries):
        if a: time.sleep(REST*6*a)
        try:
            with urllib.request.urlopen(url, timeout=90) as r:
                body=r.read()
            time.sleep(REST)          # pace AFTER the read; see Getty ECONNRESET
            return json.loads(body)
        except Exception:
            pass
    return None

def count(prefix):
    d=call({"q":f"title,begins_with,{prefix}","limit":"1","offset":"0"})
    return None if d is None else d.get("info",{}).get("total")

def fetch_page(prefix, offset):
    """One page. An empty result is retried -- the endpoint returns 200 with
    docs:[] intermittently, and trusting it silently truncates the bucket."""
    for attempt in range(EMPTY_RETRY):
        d=call({"q":f"title,begins_with,{prefix}","limit":str(PAGE),"offset":str(offset)})
        if d is None: continue
        docs=d.get("docs") or []
        if docs or offset>0: return docs, d.get("info",{}).get("total")
        time.sleep(REST*4)
    return [], None

# ---------------------------------------------------------------- planning
PROBED = D/"probed.json"

def plan():
    """Parallel trie expansion with checkpointing.

    Single-threaded planning ran at 1.6 probes/s against a measured capacity of
    8.8/s, and only wrote the plan at the very end -- so a crash three hours in
    lost everything. Workers share the frontier; an in-flight counter (not an
    empty queue) decides termination, because a queue can be momentarily empty
    while a worker is still about to push children.
    """
    buckets, unsplittable, seen, fails = {}, [], set(), {}
    if PROBED.exists():
        st = json.loads(PROBED.read_text())
        buckets = st.get("buckets", {}); seen = set(st.get("seen", []))
        unsplittable = [tuple(x) for x in st.get("unsplittable", [])]
        log(f"  resuming plan: {len(seen)} prefixes already probed, {len(buckets)} buckets")

    work = queue.Queue()
    for c in CHARS:
        if c not in seen: work.put((c, 0))
    lock = threading.Lock()
    inflight = [0]
    probes = [0]

    def save():
        PROBED.write_text(json.dumps({"buckets": buckets, "seen": sorted(seen),
                                      "unsplittable": unsplittable}))

    def worker():
        while True:
            try:
                prefix, depth = work.get(timeout=2)
            except queue.Empty:
                with lock:
                    if inflight[0] == 0 and work.empty():
                        return
                continue
            with lock:
                inflight[0] += 1
                if prefix in seen:
                    inflight[0] -= 1; work.task_done(); continue
            n = count(prefix)
            children = []
            with lock:
                probes[0] += 1
                if n is None:
                    # A failed probe is NOT an empty node. The first version
                    # marked the prefix seen before probing and dropped the whole
                    # subtree on failure -- 7,617 records vanished that way, and a
                    # resume could never revisit them. Re-queue instead, and only
                    # record the prefix as seen once it has actually answered.
                    fails[prefix] = fails.get(prefix, 0) + 1
                    if fails[prefix] <= 5:
                        children = [(prefix, depth)]
                    else:
                        unsplittable.append((prefix, -1))
                        log(f"  !! '{prefix}' unprobeable after {fails[prefix]} attempts")
                        seen.add(prefix)
                elif n == 0:
                    seen.add(prefix)
                elif n <= THRESHOLD:
                    seen.add(prefix); buckets[prefix] = n
                elif depth >= MAX_DEPTH:
                    seen.add(prefix); unsplittable.append((prefix, n))
                    log(f"  !! cannot split '{prefix}' ({n})")
                else:
                    seen.add(prefix)
                    children = [(prefix + c, depth + 1) for c in CHARS] + \
                               [(prefix + " " + c, depth + 1) for c in CHARS]
                if probes[0] % 250 == 0:
                    save()
                    log(f"  planning: {probes[0]} probes, {len(buckets)} buckets, "
                        f"~{work.qsize()} queued")
            for ch in children:
                work.put(ch)
            with lock:
                inflight[0] -= 1
            work.task_done()

    ts = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    save()
    PLAN.write_text(json.dumps({"buckets": buckets, "unsplittable": unsplittable,
                                "planned_total": sum(buckets.values())}, indent=1))
    covered = sum(buckets.values()) + sum(n for _, n in unsplittable if n > 0)
    log(f"PLAN: {len(buckets)} buckets, planned total {sum(buckets.values()):,}, "
        f"unsplittable {len(unsplittable)}, probes {probes[0]}")
    log(f"PLAN COVERAGE: {covered:,} of root-reachable 138,781 "
        f"(shortfall {138781 - covered:,})")
    if unsplittable:
        for pre, n in sorted(unsplittable, key=lambda x: -x[1])[:10]:
            log(f"    unsplit: {n:>7}  {pre!r}")
    return buckets, unsplittable

# ---------------------------------------------------------------- harvest
def harvest(buckets):
    done=set(DONE.read_text().split("\n")) if DONE.exists() else set()
    todo=[b for b in buckets if b not in done]
    log(f"HARVEST: {len(todo)} buckets outstanding of {len(buckets)}")
    seen=set()
    if OUT.exists():
        for line in OUT.open():
            m=re.search(r'"recordid"\s*:\s*\[?\s*"([^"]+)"', line)
            if m: seen.add(m.group(1))
        log(f"  resuming with {len(seen):,} records already held")
    work=queue.Queue()
    for b in todo: work.put(b)
    lock=threading.Lock(); stats={"b":0,"rec":0,"short":0}
    out_fh=OUT.open("a"); done_fh=DONE.open("a")
    def worker():
        while True:
            try: pre=work.get_nowait()
            except queue.Empty: return
            got=[]
            for off in OFFSETS:
                docs,_=fetch_page(pre, off)
                got.extend(docs)
                if len(docs)<PAGE: break
            with lock:
                added=0
                for doc in got:
                    rid=(((doc.get("pnx") or {}).get("control") or {}).get("recordid") or [""])[0]
                    if rid and rid not in seen:
                        seen.add(rid); out_fh.write(json.dumps(doc)+"\n"); added+=1
                stats["b"]+=1; stats["rec"]+=added
                if len(got) < min(buckets[pre], 500): stats["short"]+=1
                done_fh.write(pre+"\n")
                if stats["b"] % 50 == 0:
                    out_fh.flush(); done_fh.flush()
                    log(f"  {stats['b']}/{len(todo)} buckets, {len(seen):,} distinct records")
    ts=[threading.Thread(target=worker,daemon=True) for _ in range(WORKERS)]
    [t.start() for t in ts]; [t.join() for t in ts]
    out_fh.close(); done_fh.close()
    return seen, stats

if __name__ == "__main__":
    t0=time.time()
    universe=count("")  or 0
    live=call({"q":"any,contains,**","limit":"1","offset":"0"})
    universe=(live or {}).get("info",{}).get("total")
    log(f"START universe={universe}")
    notify(f":arrows_counterclockwise: Mississippi trie harvest started. Universe {universe:,}.")
    if PLAN.exists():
        data=json.loads(PLAN.read_text()); buckets=data["buckets"]
        log(f"  reusing plan: {len(buckets)} buckets")
    else:
        buckets,_=plan()
        notify(f":clipboard: Mississippi plan built: {len(buckets)} buckets covering "
               f"{sum(buckets.values()):,} of {universe:,}.")
    seen,stats=harvest(buckets)
    el=time.time()-t0
    pct = len(seen)/universe*100 if universe else 0
    log(f"DONE {len(seen):,} distinct of {universe:,} ({pct:.2f}%) in {el/60:.0f}m; "
        f"short buckets={stats['short']}")
    notify(f":checkered_flag: Mississippi harvest finished: *{len(seen):,} of {universe:,}* "
           f"({pct:.2f}%) in {el/60:.0f}m. Buckets returning fewer than planned: {stats['short']}.")
