from pathlib import Path
import base64
import html
import re

ROOT = Path(__file__).parent
SOURCE = ROOT / "source.md"
OUT = ROOT.parent / "Synthetic_Banking_Mock_Data_Design_and_Delivery_Guide.html"
WORKFLOW = Path("/var/folders/y1/5s9yw3vx22dc7v8kc1cr_dxr0000gn/T/codex-clipboard-c26ec2bd-a81d-4f7b-bb8a-ad850fa2aaff.png")
RELATIONSHIP = Path("/var/folders/y1/5s9yw3vx22dc7v8kc1cr_dxr0000gn/T/codex-clipboard-73c69eef-d3b3-4d0f-846a-a382387d0bc5.png")

def image_uri(path):
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode("ascii")

def inline(text):
    text = html.escape(text.replace("\\", ""))
    text = text.replace("&lt;br&gt;", "<br>").replace("&lt;br/&gt;", "<br>")
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\[([^]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)
    return text

def parse_table(lines):
    rows=[]
    for line in lines:
        if re.match(r"^\|\s*[-:]+", line):
            continue
        rows.append([cell.strip() for cell in line.strip().strip("|").split("|")])
    return rows

def render_table(rows):
    header = "".join(f"<th>{inline(c)}</th>" for c in rows[0])
    body = []
    for row in rows[1:]:
        body.append("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in row) + "</tr>")
    return f'<div class="table-wrap"><table><thead><tr>{header}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'

def diagram(src, alt):
    return f'<figure class="diagram"><img src="{src}" alt="{alt}"></figure>'

def main():
    workflow_uri = image_uri(WORKFLOW)
    relationship_uri = image_uri(RELATIONSHIP)
    parts=[]; lines=SOURCE.read_text().splitlines(); i=0; mermaid=0; list_kind=None
    while i < len(lines):
        line=lines[i]
        if line.startswith("```"):
            if list_kind: parts.append(f"</{list_kind}>"); list_kind=None
            language=line[3:].strip() or "text"; i+=1; block=[]
            while i < len(lines) and not lines[i].startswith("```"):
                block.append(lines[i]); i+=1
            if language == "mermaid":
                mermaid += 1
                if mermaid in (1, 4): parts.append(diagram(workflow_uri, "SCD2 and CDC processing workflow"))
                elif mermaid == 2: parts.append(diagram(relationship_uri, "Synthetic banking relationship model"))
                else: parts.append(f'<section class="code-panel"><div class="code-label">Mermaid</div><pre><code>{html.escape(chr(10).join(block))}</code></pre></section>')
            else:
                parts.append(f'<section class="code-panel"><div class="code-label">{html.escape(language)}</div><pre><code>{html.escape(chr(10).join(block))}</code></pre></section>')
            i+=1; continue
        if line.startswith("|"):
            if list_kind: parts.append(f"</{list_kind}>"); list_kind=None
            block=[]
            while i < len(lines) and lines[i].startswith("|"):
                block.append(lines[i]); i+=1
            parts.append(render_table(parse_table(block))); continue
        h=re.match(r"^(#{1,4})\s+(.*)", line)
        if h:
            if list_kind: parts.append(f"</{list_kind}>"); list_kind=None
            level=len(h.group(1)); title=h.group(2)
            if level == 1:
                parts.append('<header class="page-header"><h1>Synthetic Banking Mock Data Design and Delivery Guide</h1><p>Confluence-ready reference for the synthetic Bronze/source environment</p></header>')
            else:
                parts.append(f"<h{level}>{inline(title)}</h{level}>")
            i+=1; continue
        bullet=re.match(r"^[-*]\s+(.*)", line)
        ordered=re.match(r"^\d+\.\s+(.*)", line)
        if bullet or ordered:
            kind="ul" if bullet else "ol"; content=(bullet or ordered).group(1)
            if list_kind != kind:
                if list_kind: parts.append(f"</{list_kind}>")
                parts.append(f"<{kind}>"); list_kind=kind
            parts.append(f"<li>{inline(content)}</li>"); i+=1; continue
        if list_kind: parts.append(f"</{list_kind}>"); list_kind=None
        if line.startswith("> "):
            parts.append(f'<aside class="note">{inline(line[2:])}</aside>')
        elif line.strip():
            parts.append(f"<p>{inline(line.strip())}</p>")
        i+=1
    if list_kind: parts.append(f"</{list_kind}>")
    document = f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Synthetic Banking Mock Data Design and Delivery Guide</title>
<style>
:root {{ --ink:#172b4d; --heading:#0c66e4; --border:#dfe1e6; --fill:#f4f5f7; --code:#f7f8f9; --note:#fff4ce; }}
body {{ margin:0; background:#fff; color:#172b4d; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif; font-size:15px; line-height:1.5; }}
main {{ max-width:1160px; margin:0 auto; padding:48px 56px 72px; }}
.page-header {{ border-bottom:1px solid var(--border); margin-bottom:30px; padding-bottom:20px; }}
h1,h2,h3,h4 {{ color:var(--ink); line-height:1.25; }} h1 {{ font-size:34px; margin:0 0 6px; }} h2 {{ font-size:26px; border-bottom:2px solid #dbeafe; padding-bottom:8px; margin:42px 0 14px; }} h3 {{ font-size:21px; margin:30px 0 10px; }} h4 {{ font-size:17px; margin:24px 0 8px; }}
.page-header p {{ color:#5e6c84; margin:0; font-size:16px; }} p {{ margin:10px 0; }} a {{ color:#0052cc; }} code {{ background:#f1f2f4; border-radius:3px; padding:1px 4px; font-family:ui-monospace,SFMono-Regular,Consolas,monospace; font-size:.92em; }}
ul,ol {{ padding-left:25px; }} li {{ margin:4px 0; }}
.table-wrap {{ overflow-x:auto; margin:16px 0 26px; border:1px solid var(--border); border-radius:3px; }} table {{ width:100%; border-collapse:collapse; font-size:13px; }} th {{ background:#e9f2ff; color:#172b4d; font-weight:700; }} th,td {{ text-align:left; vertical-align:top; border:1px solid var(--border); padding:9px 10px; }} tbody tr:nth-child(even) td {{ background:#fafbfc; }}
.code-panel {{ margin:16px 0 24px; border:1px solid var(--border); border-radius:3px; overflow:hidden; }} .code-label {{ padding:6px 12px; background:#e9f2ff; color:#172b4d; font-size:12px; font-weight:700; text-transform:uppercase; }} pre {{ margin:0; overflow:auto; padding:14px; background:var(--code); }} pre code {{ padding:0; background:none; font-size:12px; }}
.diagram {{ margin:18px 0 26px; padding:12px; border:1px solid var(--border); border-radius:3px; background:#fff; }} .diagram img {{ display:block; width:100%; height:auto; }}
.note {{ margin:16px 0; padding:14px 16px; border-left:4px solid #ffab00; background:var(--note); }}
@media (max-width:700px) {{ main {{ padding:28px 18px; }} h1 {{ font-size:28px; }} table {{ font-size:12px; }} th,td {{ padding:7px; }} }}
</style></head><body><main>{''.join(parts)}</main></body></html>'''
    OUT.write_text(document)
    print(OUT)

if __name__ == '__main__': main()
