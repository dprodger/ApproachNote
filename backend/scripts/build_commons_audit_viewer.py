#!/usr/bin/env python3
"""
Build a self-contained HTML viewer for a commons_imagery_audit CSV.

The audit CSV (output of audit_commons_imagery.py) lists performer Commons
images flagged for review. NO_CATEGORY means the resolver no longer finds a
Wikipedia-anchored category for that performer — which is a *mix* of genuinely
wrong imagery (a same-named person's photos) and genuinely correct imagery from
performers who simply lack a Wikipedia URL. You can't bulk-delete it; you have
to look. This viewer makes looking fast.

Output is a single HTML file with the data embedded — no server, no network
except Commons thumbnails. Open it with `open <file>.html`. It lets you:

  - browse images grouped by performer, as lazy-loaded thumbnails
  - click an image (or use per-performer bulk actions) to mark it for deletion
  - mark a performer "reviewed" and hide reviewed ones to track progress
  - export the marked rows as a delete-list CSV, or copy a ready DELETE
    statement to run in psql

Marks and review state persist in the browser's localStorage (keyed by the CSV
filename), so you can close and resume. Nothing is written back to the DB.

Usage:
    python scripts/build_commons_audit_viewer.py commons_imagery_audit_<ts>.csv
    python scripts/build_commons_audit_viewer.py audit.csv -o viewer.html
"""

import argparse
import csv
import json
from pathlib import Path


def _thumb_url(row: dict, width: int = 180) -> str:
    """A small Commons thumbnail via Special:FilePath (avoids full-res fetch)."""
    spu = row.get("source_page_url") or ""
    marker = "/wiki/File:"
    if marker in spu:
        fname = spu.split(marker, 1)[1]  # already percent-encoded in the CSV
        return f"https://commons.wikimedia.org/wiki/Special:FilePath/{fname}?width={width}"
    return row.get("image_url") or ""


def _load_records(csv_path: Path) -> list[dict]:
    records = []
    with open(csv_path, newline="") as fh:
        for row in csv.DictReader(fh):
            records.append({
                "performer_id": row["performer_id"],
                "performer_name": row["performer_name"],
                "verdict": row["verdict"],
                "resolved_category": row.get("resolved_category") or "",
                "image_id": row["image_id"],
                "image_url": row["image_url"],
                "source_page_url": row.get("source_page_url") or "",
                "is_primary": str(row.get("is_primary")).lower() == "true",
                "thumb": _thumb_url(row),
            })
    return records


_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --bg:#11141a; --panel:#1b212c; --line:#2b3442; --txt:#e6edf3;
          --muted:#8b97a7; --del:#e5484d; --ok:#3fb950; --accent:#388bfd; }
  * { box-sizing: border-box; }
  body { margin:0; font:14px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
         background:var(--bg); color:var(--txt); }
  header { position:sticky; top:0; z-index:10; background:var(--panel);
           border-bottom:1px solid var(--line); padding:10px 14px;
           display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
  header h1 { font-size:15px; margin:0 12px 0 0; font-weight:600; }
  .stat { color:var(--muted); font-size:12px; }
  .stat b { color:var(--txt); }
  input[type=search], select { background:var(--bg); color:var(--txt);
           border:1px solid var(--line); border-radius:6px; padding:5px 8px; font-size:13px; }
  input[type=search] { min-width:200px; }
  button { background:var(--bg); color:var(--txt); border:1px solid var(--line);
           border-radius:6px; padding:5px 10px; font-size:12px; cursor:pointer; }
  button:hover { border-color:var(--accent); }
  button.danger:hover { border-color:var(--del); color:var(--del); }
  label.chk { color:var(--muted); font-size:12px; display:inline-flex; gap:5px; align-items:center; cursor:pointer; }
  .spacer { flex:1; }
  main { padding:14px; }
  .perf { border:1px solid var(--line); border-radius:8px; margin-bottom:14px; background:var(--panel); }
  .perf.reviewed { opacity:.55; }
  .perf > .phead { display:flex; flex-wrap:wrap; gap:10px; align-items:center;
                   padding:9px 12px; border-bottom:1px solid var(--line); position:sticky; top:52px; background:var(--panel); }
  .pname { font-weight:600; }
  .badge { font-size:11px; padding:2px 7px; border-radius:10px; border:1px solid var(--line); color:var(--muted); }
  .badge.cat { color:var(--ok); border-color:#244a2e; }
  .badge.nocat { color:var(--del); border-color:#4a2426; }
  .pmeta { color:var(--muted); font-size:12px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(150px,1fr)); gap:10px; padding:12px; }
  .card { border:2px solid transparent; border-radius:8px; overflow:hidden; background:var(--bg);
          cursor:pointer; position:relative; }
  .card.del { border-color:var(--del); }
  .card.del .thumb { opacity:.4; }
  .card .thumb { width:100%; height:150px; object-fit:cover; display:block; background:#0c0f14; }
  .card .meta { padding:5px 7px; font-size:11px; }
  .card .fn { color:var(--txt); word-break:break-word; max-height:32px; overflow:hidden; display:block; }
  .card a { color:var(--accent); text-decoration:none; font-size:11px; }
  .card .mark { position:absolute; top:6px; left:6px; background:rgba(229,72,77,.92);
                color:#fff; font-size:10px; padding:1px 6px; border-radius:4px; display:none; }
  .card.del .mark { display:block; }
  .card .star { position:absolute; top:6px; right:6px; font-size:13px; color:#ffd33d; display:none; }
  .card.primary .star { display:block; }
  .hidden { display:none !important; }
  .empty { color:var(--muted); padding:30px; text-align:center; }
</style>
</head>
<body>
<header>
  <h1>Commons imagery audit</h1>
  <span class="stat"><b id="sPerf">0</b> performers · <b id="sImg">0</b> images ·
        <b id="sDel" style="color:var(--del)">0</b> marked · reviewed <b id="sRev">0</b>/<b id="sPerfTot">0</b></span>
  <span class="spacer"></span>
  <input type="search" id="q" placeholder="filter by performer name…">
  <select id="vf">
    <option value="">all verdicts</option>
    <option value="NO_CATEGORY">NO_CATEGORY</option>
    <option value="NOT_IN_RESOLVED_CATEGORY">NOT_IN_RESOLVED_CATEGORY</option>
  </select>
  <label class="chk"><input type="checkbox" id="hideRev"> hide reviewed</label>
  <button id="expCsv">Export delete CSV</button>
  <button id="expSql">Copy DELETE SQL</button>
  <button id="clear" class="danger">Clear marks</button>
</header>
<main id="main"></main>
<script>
const DATA = /*DATA*/;
const KEY = "__STORAGE_KEY__";
const load = (k, d) => { try { return JSON.parse(localStorage.getItem(KEY+":"+k)) ?? d; } catch(e){ return d; } };
const save = (k, v) => localStorage.setItem(KEY+":"+k, JSON.stringify(v));

let marks = new Set(load("marks", []));        // image_id -> delete
let reviewed = new Set(load("reviewed", []));  // performer_id -> reviewed
const persist = () => { save("marks",[...marks]); save("reviewed",[...reviewed]); refreshStats(); };

// group rows by performer, preserving CSV order
const groups = [];
const byId = new Map();
for (const r of DATA) {
  let g = byId.get(r.performer_id);
  if (!g) { g = {id:r.performer_id, name:r.performer_name, cat:r.resolved_category, verdicts:new Set(), rows:[]};
            byId.set(r.performer_id, g); groups.push(g); }
  g.rows.push(r); g.verdicts.add(r.verdict);
}
groups.sort((a,b)=> a.name.toLowerCase().localeCompare(b.name.toLowerCase()));

const main = document.getElementById("main");
const esc = s => (s||"").replace(/[&<>"]/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));

function render() {
  const q = document.getElementById("q").value.trim().toLowerCase();
  const vf = document.getElementById("vf").value;
  const hideRev = document.getElementById("hideRev").checked;
  main.innerHTML = "";
  let shown = 0;
  for (const g of groups) {
    if (q && !g.name.toLowerCase().includes(q)) continue;
    if (vf && !g.verdicts.has(vf)) continue;
    if (hideRev && reviewed.has(g.id)) continue;
    shown++;
    const rev = reviewed.has(g.id);
    const sec = document.createElement("section");
    sec.className = "perf" + (rev ? " reviewed" : "");
    const catBadge = g.cat
      ? `<span class="badge cat">${esc(g.cat)}</span>`
      : `<span class="badge nocat">no Wikipedia category</span>`;
    sec.innerHTML = `
      <div class="phead">
        <span class="pname">${esc(g.name)}</span>
        <span class="pmeta">${g.rows.length} image(s)</span>
        ${catBadge}
        <span class="spacer"></span>
        <button data-act="delall">Mark all delete</button>
        <button data-act="keepall">Keep all</button>
        <label class="chk"><input type="checkbox" data-act="rev" ${rev?"checked":""}> reviewed</label>
      </div>
      <div class="grid"></div>`;
    const grid = sec.querySelector(".grid");
    for (const r of g.rows) {
      const card = document.createElement("div");
      card.className = "card" + (marks.has(r.image_id)?" del":"") + (r.is_primary?" primary":"");
      card.dataset.img = r.image_id;
      const fn = decodeURIComponent((r.source_page_url.split("/wiki/File:")[1])||r.image_id);
      card.innerHTML = `
        <span class="mark">DELETE</span><span class="star" title="primary">★</span>
        <img class="thumb" loading="lazy" src="${esc(r.thumb)}" alt="">
        <div class="meta">
          <span class="fn">${esc(fn)}</span>
          <a href="${esc(r.source_page_url||r.image_url)}" target="_blank" rel="noopener">Commons ↗</a>
        </div>`;
      card.querySelector(".thumb").addEventListener("error", e => { e.target.style.opacity=.15; });
      card.addEventListener("click", ev => {
        if (ev.target.tagName === "A") return;
        toggle(r.image_id, card);
      });
      grid.appendChild(card);
    }
    sec.querySelector('[data-act=delall]').onclick = () => {
      g.rows.forEach(r => marks.add(r.image_id)); persist(); render();
    };
    sec.querySelector('[data-act=keepall]').onclick = () => {
      g.rows.forEach(r => marks.delete(r.image_id)); persist(); render();
    };
    sec.querySelector('[data-act=rev]').onchange = e => {
      if (e.target.checked) reviewed.add(g.id); else reviewed.delete(g.id);
      persist();
      if (document.getElementById("hideRev").checked) render();
      else sec.classList.toggle("reviewed", e.target.checked);
    };
    main.appendChild(sec);
  }
  if (!shown) main.innerHTML = '<div class="empty">No performers match the current filter.</div>';
}

function toggle(imgId, card) {
  if (marks.has(imgId)) marks.delete(imgId); else marks.add(imgId);
  card.classList.toggle("del", marks.has(imgId));
  persist();
}

function refreshStats() {
  document.getElementById("sPerf").textContent = groups.length;
  document.getElementById("sPerfTot").textContent = groups.length;
  document.getElementById("sImg").textContent = DATA.length;
  document.getElementById("sDel").textContent = marks.size;
  document.getElementById("sRev").textContent = reviewed.size;
}

function markedRows() { return DATA.filter(r => marks.has(r.image_id)); }

function download(name, text, type) {
  const blob = new Blob([text], {type});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob); a.download = name; a.click();
  URL.revokeObjectURL(a.href);
}

document.getElementById("expCsv").onclick = () => {
  const rows = markedRows();
  if (!rows.length) return alert("Nothing marked for deletion.");
  const csv = "performer_id,image_id\n" + rows.map(r=>`${r.performer_id},${r.image_id}`).join("\n") + "\n";
  download("commons_delete_list.csv", csv, "text/csv");
};

document.getElementById("expSql").onclick = async () => {
  const rows = markedRows();
  if (!rows.length) return alert("Nothing marked for deletion.");
  const vals = rows.map(r=>`  ('${r.performer_id}'::uuid,'${r.image_id}'::uuid)`).join(",\n");
  const sql =
`-- ${rows.length} artist_images link(s) marked for deletion
DELETE FROM artist_images ai
USING (VALUES
${vals}
) AS f(performer_id, image_id)
WHERE ai.performer_id = f.performer_id AND ai.image_id = f.image_id;`;
  try { await navigator.clipboard.writeText(sql); alert(`Copied DELETE for ${rows.length} link(s) to clipboard.`); }
  catch(e) { download("commons_delete.sql", sql, "text/plain"); }
};

document.getElementById("clear").onclick = () => {
  if (!marks.size || !confirm(`Clear all ${marks.size} deletion marks?`)) return;
  marks.clear(); persist(); render();
};

document.getElementById("q").addEventListener("input", render);
document.getElementById("vf").addEventListener("change", render);
document.getElementById("hideRev").addEventListener("change", render);

refreshStats();
render();
</script>
</body>
</html>
"""


def build_html(records: list[dict], title: str, storage_key: str) -> str:
    data_json = json.dumps(records, ensure_ascii=False)
    return (_HTML_TEMPLATE
            .replace("__TITLE__", title)
            .replace("__STORAGE_KEY__", storage_key)
            .replace("/*DATA*/", data_json))


def main() -> None:
    p = argparse.ArgumentParser(
        description="Build a self-contained HTML viewer for an audit CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    p.add_argument("csv", help="commons_imagery_audit_<ts>.csv path")
    p.add_argument("-o", "--output", default=None,
                   help="Output HTML path (default: <csv>.html)")
    args = p.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"No such file: {csv_path}")

    records = _load_records(csv_path)
    out_path = Path(args.output) if args.output else csv_path.with_suffix(".html")
    html = build_html(records, title=csv_path.name, storage_key=csv_path.stem)
    out_path.write_text(html, encoding="utf-8")

    performers = len({r["performer_id"] for r in records})
    print(f"Wrote {out_path} — {len(records)} image(s) across {performers} performer(s)")
    print(f"Open it with:  open {out_path}")


if __name__ == "__main__":
    main()
