from pathlib import Path
import re
import sys

ROOT = Path(__file__).parent
SOURCE = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "source.md"
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT.parent / "Synthetic_Banking_Mock_Data_Design_and_Delivery_Guide.confluence"

def inline(text):
    text = text.replace("\\", "")
    text = text.replace("<br>", "\\\\").replace("<br/>", "\\\\")
    text = re.sub(r"\*\*(.+?)\*\*", r"*\1*", text)
    text = re.sub(r"`([^`]+)`", r"{{\1}}", text)
    text = re.sub(r"\[([^]]+)\]\(([^)]+)\)", r"[\1|\2]", text)
    return text

def table_row(line, heading=False):
    cells = [inline(c.strip()).replace("|", "\\|") for c in line.strip().strip("|").split("|")]
    marker = "||" if heading else "|"
    return marker + marker.join(cells) + marker

def main():
    lines = SOURCE.read_text().splitlines(); out=[]; i=0; mermaid=0
    while i < len(lines):
        line = lines[i]
        if line.startswith("```"):
            lang = line[3:].strip() or "text"; i += 1; block=[]
            while i < len(lines) and not lines[i].startswith("```"):
                block.append(lines[i]); i += 1
            if lang == "mermaid":
                mermaid += 1
                if mermaid in (1, 4): out.append("!scd2-cdc-workflow.png|alt=SCD2 and CDC processing workflow!")
                elif mermaid == 2: out.append("!relationship-model.png|alt=Synthetic banking relationship model!")
                else: out.extend(["{code:language=mermaid}", *block, "{code}"])
            else:
                out.extend([f"{{code:language={lang}}}", *block, "{code}"])
            out.append(""); i += 1; continue
        if line.startswith("|"):
            block=[]
            while i < len(lines) and lines[i].startswith("|"):
                block.append(lines[i]); i += 1
            for index, row in enumerate(block):
                if index == 1 and re.match(r"^\|\s*[-:]+", row): continue
                out.append(table_row(row, heading=index == 0))
            out.append(""); continue
        heading = re.match(r"^(#{1,4})\s+(.*)", line)
        if heading:
            level = len(heading.group(1)); title = inline(heading.group(2))
            out.append(f"h{level}. {title}"); out.append(""); i += 1; continue
        bullet = re.match(r"^[-*]\s+(.*)", line)
        ordered = re.match(r"^\d+\.\s+(.*)", line)
        if bullet: out.append(f"* {inline(bullet.group(1))}")
        elif ordered: out.append(f"# {inline(ordered.group(1))}")
        elif line.startswith("> "):
            out.extend(["{panel:title=Important|borderStyle=solid|borderColor=#FFAB00|bgColor=#FFF4CE}", inline(line[2:]), "{panel}", ""])
        elif line.strip(): out.append(inline(line.strip()))
        else:
            out.append("")
        i += 1
    OUT.write_text("\n".join(out).rstrip() + "\n")
    print(OUT)

if __name__ == "__main__": main()
