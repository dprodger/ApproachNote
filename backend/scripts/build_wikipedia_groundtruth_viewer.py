#!/usr/bin/env python3
"""
Build a self-contained HTML verification UI from a wikipedia_queue_*.json.

Input is the queue produced by build_wikipedia_groundtruth_queue.py: performers
with Commons imagery but no Wikipedia link, each with evidence thumbnails and
candidate Wikipedia articles (category-derived, with name-search fallbacks).

The page lets you, per performer:
  - look at the Commons photos we already hold (the evidence),
  - pick the candidate Wikipedia article that matches them (or paste a custom
    URL, or mark "no match"),
  - and export the confirmed set as a GROUND-TRUTH JSON — human-verified links,
    stamped with manual provenance, suitable for re-ingest or for diffing
    against automated crawlers.

Decisions persist in the browser (localStorage, keyed by the queue filename),
so you can close and resume. Nothing is written back to the database; the
exported JSON is the deliverable.

Usage:
    python scripts/build_wikipedia_groundtruth_viewer.py data/ground_truth/wikipedia_queue_<ts>.json
    python scripts/build_wikipedia_groundtruth_viewer.py queue.json -o verify.html
"""

import argparse
import json
from pathlib import Path


_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --bg:#11141a; --panel:#1b212c; --line:#2b3442; --txt:#e6edf3;
          --muted:#8b97a7; --ok:#3fb950; --no:#e5484d; --accent:#388bfd; --warn:#d29922; }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--bg); color:var(--txt); }
  header { position:sticky; top:0; z-index:10; background:var(--panel); border-bottom:1px solid var(--line);
           padding:10px 14px; display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
  header h1 { font-size:15px; margin:0 8px 0 0; font-weight:600; }
  .stat { color:var(--muted); font-size:12px; } .stat b { color:var(--txt); }
  input[type=search], select, input[type=text] { background:var(--bg); color:var(--txt); border:1px solid var(--line);
           border-radius:6px; padding:5px 8px; font-size:13px; }
  button { background:var(--bg); color:var(--txt); border:1px solid var(--line); border-radius:6px;
           padding:5px 10px; font-size:12px; cursor:pointer; }
  button:hover { border-color:var(--accent); }
  button.primary { background:#1f6feb22; border-color:var(--accent); color:#cfe0ff; }
  .spacer { flex:1; }
  main { padding:14px; max-width:1100px; margin:0 auto; }
  .perf { border:1px solid var(--line); border-radius:10px; margin-bottom:14px; background:var(--panel); }
  .perf.done { border-color:#27412c; }
  .perf.nomatch { border-color:#4a2426; }
  .phead { display:flex; gap:10px; align-items:center; padding:10px 12px; border-bottom:1px solid var(--line);
           position:sticky; top:52px; background:var(--panel); }
  .pname { font-weight:600; font-size:15px; }
  .pill { font-size:11px; padding:2px 8px; border-radius:10px; border:1px solid var(--line); color:var(--muted); }
  .pill.ok { color:var(--ok); border-color:#27412c; } .pill.no { color:var(--no); border-color:#4a2426; }
  .body { display:grid; grid-template-columns:300px 1fr; gap:14px; padding:12px; }
  @media (max-width:760px){ .body { grid-template-columns:1fr; } }
  .evidence h3, .cands h3 { margin:0 0 7px; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); }
  .egrid { display:grid; grid-template-columns:repeat(auto-fill,minmax(80px,1fr)); gap:6px; }
  .egrid a { display:block; } .egrid img { width:100%; height:80px; object-fit:cover; border-radius:6px; background:#0c0f14; display:block; }
  .cand { display:flex; gap:10px; padding:8px; border:1px solid var(--line); border-radius:8px; margin-bottom:7px; cursor:pointer; align-items:flex-start; }
  .cand:hover { border-color:var(--accent); }
  .cand.sel { border-color:var(--ok); background:#10391c33; }
  .cand img { width:64px; height:64px; object-fit:cover; border-radius:6px; background:#0c0f14; flex:none; }
  .cand .noimg { width:64px; height:64px; border-radius:6px; background:#0c0f14; display:flex; align-items:center; justify-content:center; color:var(--muted); font-size:10px; flex:none; }
  .cand .ct { font-weight:600; } .cand .cd { color:var(--muted); font-size:12.5px; margin:2px 0; }
  .cand a.ext { color:var(--accent); text-decoration:none; font-size:12px; }
  .tag { font-size:10px; padding:1px 6px; border-radius:8px; border:1px solid var(--line); color:var(--muted); margin-right:5px; }
  .tag.cat { color:#8ad; border-color:#24405a; } .tag.name { color:var(--warn); border-color:#4a3d1c; }
  .tag.human { color:var(--ok); border-color:#27412c; } .tag.nonhuman { color:var(--no); border-color:#4a2426; }
  .alt { display:flex; gap:10px; align-items:center; padding:7px 8px; border:1px dashed var(--line); border-radius:8px; margin-top:7px; flex-wrap:wrap; }
  .alt label { display:flex; gap:6px; align-items:center; cursor:pointer; color:var(--muted); font-size:13px; }
  .alt.selno { border-color:var(--no); } .alt.selcustom { border-color:var(--ok); }
  .nocand { color:var(--warn); font-size:13px; margin-bottom:7px; }
  .sysctx { margin-top:12px; padding:10px; border:1px solid var(--line); border-radius:8px; background:#0f1620; }
  .sysctx h3 { margin:0 0 6px; }
  .sysctx .chips { display:flex; flex-wrap:wrap; gap:5px; margin-bottom:6px; }
  .sysctx .chip { font-size:11px; padding:2px 8px; border-radius:10px; border:1px solid #24405a; color:#8ad; }
  .sysctx .line { font-size:12.5px; color:var(--txt); margin:3px 0; }
  .sysctx .muted { color:var(--muted); }
  .sysctx ul { margin:5px 0 0; padding-left:16px; }
  .sysctx li { font-size:12px; color:var(--muted); }
  .sysctx .bio { font-size:12px; color:var(--muted); margin-top:6px; font-style:italic; }
  .hidden { display:none !important; }
  a.searchlink { color:var(--accent); font-size:12px; text-decoration:none; }
</style>
</head>
<body>
<header>
  <h1>Wikipedia ground-truth</h1>
  <span class="stat">decided <b id="sDone">0</b>/<b id="sTot">0</b> · verified <b id="sVer" style="color:var(--ok)">0</b> · no-match <b id="sNo" style="color:var(--no)">0</b></span>
  <span class="spacer"></span>
  <input type="search" id="q" placeholder="filter by name…">
  <select id="ff">
    <option value="">all</option>
    <option value="undecided">undecided</option>
    <option value="verified">verified</option>
    <option value="no_match">no-match</option>
    <option value="hascand">has candidate</option>
    <option value="nocand">no candidate</option>
  </select>
  <button id="exp" class="primary">Export ground truth JSON</button>
</header>
<main id="main"></main>
<script>
const Q = /*DATA*/;
const KEY = "__STORAGE_KEY__";
const SRC = "__SOURCE__";
const load = (k,d)=>{ try{ return JSON.parse(localStorage.getItem(KEY+":"+k)) ?? d; }catch(e){ return d; } };
const save = (k,v)=> localStorage.setItem(KEY+":"+k, JSON.stringify(v));

// decisions: performer_id -> {choice, url, qid, method, category, title, at}
let decisions = load("decisions", {});
const persist = ()=>{ save("decisions", decisions); stats(); };
const esc = s => (s||"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));

const main = document.getElementById("main");

function sysContextHTML(c){
  if(!c) return "";
  const parts = [];
  if((c.instruments||[]).length)
    parts.push(`<div class="chips">${c.instruments.map(i=>`<span class="chip">${esc(i)}</span>`).join("")}</div>`);
  if(c.recording_count){
    let era = "";
    if(c.year_min && c.year_max) era = c.year_min===c.year_max ? ` · ${c.year_min}` : ` · ${c.year_min}–${c.year_max}`;
    parts.push(`<div class="line"><b>${c.recording_count}</b> recording(s) in our DB<span class="muted">${era}</span></div>`);
  }
  const idbits = [];
  if(c.artist_type) idbits.push(esc(c.artist_type));
  if(c.disambiguation) idbits.push(esc(c.disambiguation));
  const dates = (c.birth_date||c.death_date) ? `${c.birth_date||"?"} – ${c.death_date||""}`.trim() : "";
  if(idbits.length) parts.push(`<div class="line muted">${idbits.join(" · ")}</div>`);
  if(dates) parts.push(`<div class="line muted">${esc(dates)}</div>`);
  const seenT = new Set();
  const samples = (c.sample_recordings||[]).filter(s=>{ const k=(s.title||"").toLowerCase(); if(seenT.has(k)) return false; seenT.add(k); return true; });
  if(samples.length)
    parts.push(`<ul>${samples.map(s=>`<li>${s.year?esc(String(s.year))+" — ":""}${esc(s.title||"")}</li>`).join("")}</ul>`);
  if(c.biography) parts.push(`<div class="bio">${esc(c.biography)}${c.biography.length>=280?"…":""}</div>`);
  if(!parts.length) parts.push(`<div class="line muted">No recording/instrument data in our DB.</div>`);
  return `<div class="sysctx"><h3>What our DB knows</h3>${parts.join("")}</div>`;
}

function decide(pid, d){ if(d) decisions[pid]=Object.assign({at:new Date().toISOString()},d); else delete decisions[pid]; persist(); render(); }

function statusOf(pid){ const d=decisions[pid]; if(!d) return "undecided"; return d.choice==="no_match"?"no_match":"verified"; }

function render(){
  const q=document.getElementById("q").value.trim().toLowerCase();
  const ff=document.getElementById("ff").value;
  main.innerHTML=""; let shown=0;
  for(const r of Q.records){
    if(q && !r.name.toLowerCase().includes(q)) continue;
    const st=statusOf(r.performer_id);
    if(ff==="undecided" && st!=="undecided") continue;
    if(ff==="verified" && st!=="verified") continue;
    if(ff==="no_match" && st!=="no_match") continue;
    if(ff==="hascand" && !r.candidates.length) continue;
    if(ff==="nocand" && r.candidates.length) continue;
    shown++;
    const d=decisions[r.performer_id];
    const sec=document.createElement("section");
    sec.className="perf"+(st==="verified"?" done":st==="no_match"?" nomatch":"");
    const statusPill = st==="verified" ? '<span class="pill ok">✓ verified</span>'
                     : st==="no_match" ? '<span class="pill no">✗ no match</span>'
                     : '<span class="pill">undecided</span>';
    sec.innerHTML = `
      <div class="phead">
        <span class="pname">${esc(r.name)}</span>
        <span class="pill">${r.evidence_images.length} photo(s)</span>
        ${statusPill}
      </div>
      <div class="body">
        <div class="evidence"><h3>Our Commons photos</h3><div class="egrid"></div><div class="sysbox"></div></div>
        <div class="cands"><h3>Candidate Wikipedia article</h3><div class="clist"></div></div>
      </div>`;
    const eg=sec.querySelector(".egrid");
    for(const img of r.evidence_images){
      const a=document.createElement("a"); a.href=img.page||img.thumb; a.target="_blank"; a.rel="noopener";
      a.innerHTML=`<img loading="lazy" src="${esc(img.thumb)}" alt="">`;
      a.querySelector("img").addEventListener("error",e=>e.target.style.opacity=.15);
      eg.appendChild(a);
    }
    sec.querySelector(".sysbox").innerHTML = sysContextHTML(r.system_context);
    const cl=sec.querySelector(".clist");
    if(!r.candidates.length){
      const sr=`https://en.wikipedia.org/w/index.php?search=${encodeURIComponent(r.name)}`;
      cl.insertAdjacentHTML("beforeend",
        `<div class="nocand">No automatic candidate. <a class="searchlink" href="${sr}" target="_blank" rel="noopener">Search Wikipedia ↗</a></div>`);
    }
    r.candidates.forEach((c,i)=>{
      const selected = d && d.choice==="cand" && d.qid===c.wikidata_qid;
      const div=document.createElement("div");
      div.className="cand"+(selected?" sel":"");
      const img = c.thumb ? `<img loading="lazy" src="${esc(c.thumb)}" alt="">` : `<div class="noimg">no photo</div>`;
      div.innerHTML=`${img}
        <div>
          <div class="ct">${esc(c.title)}</div>
          <div>
            <span class="tag ${c.method==='category'?'cat':'name'}">${c.method==='category'?'category-derived':'name search'}</span>
            <span class="tag ${c.is_human?'human':'nonhuman'}">${c.is_human?'human':'not human'}</span>
            <span class="tag">${esc(c.wikidata_qid)}</span>
          </div>
          <div class="cd">${esc(c.description||"")}</div>
          <a class="ext" href="${esc(c.wikipedia_url)}" target="_blank" rel="noopener">${esc(c.wikipedia_url)} ↗</a>
        </div>`;
      div.addEventListener("click",ev=>{
        if(ev.target.tagName==="A") return;
        decide(r.performer_id, {choice:"cand", url:c.wikipedia_url, qid:c.wikidata_qid,
                                method:c.method, category:c.commons_category, title:c.title});
      });
      cl.appendChild(div);
    });
    // alt row: no-match + custom URL
    const isNo = d && d.choice==="no_match";
    const isCustom = d && d.choice==="custom";
    const alt=document.createElement("div");
    alt.className="alt"+(isNo?" selno":isCustom?" selcustom":"");
    alt.innerHTML=`
      <label><input type="radio" name="alt-${r.performer_id}" ${isNo?"checked":""} data-no> ✗ No match</label>
      <label><input type="radio" name="alt-${r.performer_id}" ${isCustom?"checked":""} data-custom> ✎ Custom URL:</label>
      <input type="text" style="flex:1;min-width:180px" placeholder="https://en.wikipedia.org/wiki/…"
             value="${isCustom?esc(d.url||""):""}" data-customurl>
      ${d ? '<button data-clear>clear</button>' : ''}`;
    alt.querySelector("[data-no]").addEventListener("change",()=>decide(r.performer_id,{choice:"no_match",url:null,qid:null,method:null,category:null}));
    const cu=alt.querySelector("[data-customurl]");
    const commitCustom=()=>{ const u=cu.value.trim(); if(u) decide(r.performer_id,{choice:"custom",url:u,qid:null,method:"custom",category:null}); };
    alt.querySelector("[data-custom]").addEventListener("change",()=>{ if(cu.value.trim()) commitCustom(); else cu.focus(); });
    cu.addEventListener("change",commitCustom);
    const clr=alt.querySelector("[data-clear]"); if(clr) clr.addEventListener("click",()=>decide(r.performer_id,null));
    sec.querySelector(".cands").appendChild(alt);
    main.appendChild(sec);
  }
  if(!shown) main.innerHTML='<div style="color:var(--muted);padding:30px;text-align:center">No performers match this filter.</div>';
}

function stats(){
  const ids=Q.records.map(r=>r.performer_id);
  let ver=0,no=0;
  for(const id of ids){ const s=statusOf(id); if(s==="verified")ver++; else if(s==="no_match")no++; }
  document.getElementById("sTot").textContent=ids.length;
  document.getElementById("sDone").textContent=ver+no;
  document.getElementById("sVer").textContent=ver;
  document.getElementById("sNo").textContent=no;
}

document.getElementById("exp").onclick=()=>{
  const records={};
  for(const r of Q.records){
    const d=decisions[r.performer_id]; if(!d) continue;  // export only decided
    const verified = d.choice!=="no_match";
    records[r.performer_id]={
      name:r.name,
      status: verified?"verified":"no_match",
      wikipedia_url: verified ? d.url : null,
      wikidata_qid: verified ? (d.qid||null) : null,
      method:"manual",
      candidate_method: verified ? d.method : null,
      evidence: { commons_category: d.category||null },
      verified_at: d.at
    };
  }
  const n=Object.keys(records).length;
  if(!n) return alert("No decisions yet to export.");
  const out={ schema:"performer_wikipedia_groundtruth/v1", exported_at:new Date().toISOString(),
              source_queue:SRC, record_count:n, records };
  const blob=new Blob([JSON.stringify(out,null,2)],{type:"application/json"});
  const a=document.createElement("a"); a.href=URL.createObjectURL(blob);
  a.download="performer_wikipedia_groundtruth.json"; a.click(); URL.revokeObjectURL(a.href);
};

document.getElementById("q").addEventListener("input",render);
document.getElementById("ff").addEventListener("change",render);
stats(); render();
</script>
</body>
</html>
"""


def main() -> None:
    p = argparse.ArgumentParser(
        description="Build the Wikipedia ground-truth verification HTML from a queue JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    p.add_argument("queue", help="wikipedia_queue_<ts>.json path")
    p.add_argument("-o", "--output", default=None, help="Output HTML (default: <queue>.html)")
    args = p.parse_args()

    qpath = Path(args.queue)
    if not qpath.exists():
        raise SystemExit(f"No such file: {qpath}")
    data = json.loads(qpath.read_text(encoding="utf-8"))

    html = (_HTML
            .replace("__TITLE__", qpath.name)
            .replace("__STORAGE_KEY__", qpath.stem)
            .replace("__SOURCE__", qpath.name)
            .replace("/*DATA*/", json.dumps(data, ensure_ascii=False)))

    out = Path(args.output) if args.output else qpath.with_suffix(".html")
    out.write_text(html, encoding="utf-8")
    recs = data.get("records", [])
    withc = sum(1 for r in recs if r.get("candidates"))
    print(f"Wrote {out} — {len(recs)} performer(s), {withc} with >=1 candidate")
    print(f"Open it with:  open {out}")


if __name__ == "__main__":
    main()
