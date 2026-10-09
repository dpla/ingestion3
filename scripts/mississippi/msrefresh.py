#!/usr/bin/env python3
"""
Per-id refresh AND coverage verification for Mississippi.

This is the measurement, not just a data pull. The union of the March and
September id lists (138,970) exceeds the live universe (~138,818), but set SIZE
proves nothing about set MEMBERSHIP: the union carries March ids that may since
have been withdrawn, so an excess and a deficit can coexist and cancel out. Only
resolving every id tells us which.

  resolved ~= universe  -> coverage is genuinely complete
  resolved <  universe  -> the difference is a real deficit still to discover

Retrieval uses `any,contains,<bare MMS number>` -- rid,exact returns 0 on this
view even for records the API just handed back, and the `alma` prefix breaks the
keyword match. Verified 20/20 exact single matches, 8.8/s at 6 workers.
"""
import json, os, queue, re, threading, time, urllib.parse, urllib.request
from pathlib import Path

KEY="l8xx87aeef957145450eaf117a6c1f0d8c71"
API="https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
VIEW={"vid":"01USM_INST:MDL","tab":"MDL","scope":"MDL","lang":"eng"}
WORKERS=6; REST=0.4; RETRIES=4

D=Path("/home/ec2-user/mississippi-2026")
SEED=D/"union-ids.txt"; OUT=D/"refresh.jsonl"; DONE=D/"refresh.done"
DEAD=D/"refresh-dead.txt"; LOG=D/"refresh.log"

def log(m):
    line=f"[{time.strftime('%H:%M:%S')}] {m}"
    print(line, flush=True)
    with LOG.open("a") as fh: fh.write(line+"\n")

def notify(text):
    c=Path("/home/ec2-user/.getty-slack")
    if not c.exists(): return
    env=dict(l.split("=",1) for l in c.read_text().splitlines() if "=" in l and not l.startswith("#"))
    t,u=env.get("SLACK_BOT_TOKEN","").strip(), env.get("SLACK_ALERT_USER_ID","").strip()
    if not (t and u): return
    try:
        urllib.request.urlopen(urllib.request.Request(
            "https://slack.com/api/chat.postMessage",
            data=json.dumps({"channel":u,"text":text}).encode(),
            headers={"Authorization":"Bearer "+t,"Content-type":"application/json"}),timeout=20).read()
    except Exception: pass

def universe():
    p=dict(VIEW); p.update({"apikey":KEY,"q":"any,contains,**","limit":"1","offset":"0"})
    try:
        with urllib.request.urlopen(API+"?"+urllib.parse.urlencode(p),timeout=60) as r:
            return json.loads(r.read()).get("info",{}).get("total")
    except Exception: return None

def lookup(rid):
    """(doc, status) where status is ok | gone | error."""
    bare=rid[4:] if rid.startswith("alma") else rid
    p=dict(VIEW); p.update({"apikey":KEY,"q":f"any,contains,{bare}","limit":"2","offset":"0"})
    url=API+"?"+urllib.parse.urlencode(p)
    for a in range(RETRIES):
        if a: time.sleep(REST*8*a)
        try:
            with urllib.request.urlopen(url,timeout=60) as r: body=r.read()
            time.sleep(REST)
            d=json.loads(body)
            for doc in (d.get("docs") or []):
                got=(((doc.get("pnx") or {}).get("control") or {}).get("recordid") or [""])[0]
                if got==rid: return doc,"ok"
            return None,"gone"
        except Exception:
            pass
    return None,"error"

def main():
    uni=universe()
    ids=[l.strip() for l in SEED.read_text().splitlines() if l.strip()]
    done=set(DONE.read_text().split()) if DONE.exists() else set()
    todo=[i for i in ids if i not in done]
    log(f"seed={len(ids):,} done={len(done):,} todo={len(todo):,} universe={uni}")
    notify(f":mag: Mississippi per-id verification started: {len(todo):,} ids to resolve "
           f"against a live universe of {uni:,}. This is the test of whether coverage is real.")
    work=queue.Queue()
    for i in todo: work.put(i)
    st={"ok":0,"gone":0,"error":0}
    lk=threading.Lock(); t0=time.time()
    of=OUT.open("a"); df=DONE.open("a"); xf=DEAD.open("a")
    def w():
        while True:
            try: rid=work.get_nowait()
            except queue.Empty: return
            doc,status=lookup(rid)
            with lk:
                st[status]+=1
                if status=="ok": of.write(json.dumps(doc)+"\n"); df.write(rid+"\n")
                elif status=="gone": xf.write(rid+"\n"); df.write(rid+"\n")
                n=sum(st.values())
                if n%2000==0:
                    of.flush(); df.flush(); xf.flush()
                    rate=n/max(1e-9,time.time()-t0)
                    log(f"  {n:,}/{len(todo):,} ok={st['ok']:,} gone={st['gone']:,} "
                        f"err={st['error']} {rate:.1f}/s eta={(len(todo)-n)/max(rate,1e-9)/3600:.1f}h")
    ts=[threading.Thread(target=w,daemon=True) for _ in range(WORKERS)]
    [t.start() for t in ts]; [t.join() for t in ts]
    of.close(); df.close(); xf.close()
    resolved=st["ok"]+len(done)
    el=(time.time()-t0)/3600
    log(f"DONE resolved={st['ok']:,} gone={st['gone']:,} errors={st['error']} in {el:.1f}h")
    uni2=universe()
    verdict=("COMPLETE" if uni2 and resolved>=uni2 else
             f"DEFICIT {uni2-resolved:,}" if uni2 else "universe unknown")
    log(f"VERDICT: resolved {resolved:,} vs universe {uni2:,} -> {verdict}")
    notify(f":checkered_flag: Mississippi verification done.\n"
           f"Resolved *{resolved:,}* live records; *{st['gone']:,}* seed ids no longer exist; "
           f"{st['error']} errors.\nLive universe {uni2:,} -> *{verdict}*")

if __name__=="__main__": main()
