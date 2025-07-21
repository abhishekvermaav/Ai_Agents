def generate_markdown(proc_name, tables, summary, diagram_path=None):
    md = f"""# 📘 Stored Procedure Documentation

## Procedure: `{proc_name}`

---

### 🧩 Tables Involved
{', '.join(tables) if tables else 'None detected'}

---

### 📄 Summary
{summary}
"""
    if diagram_path:
        md += f"\n\n### 🔗 Table Relationship Diagram\n![Diagram]({diagram_path})\n"
    return md
