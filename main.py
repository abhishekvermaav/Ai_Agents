import os
import sys

from utils import load_procedure, extract_tables, extract_proc_name, detect_object_type
from templates import generate_markdown
from ollama_client import generate_doc_from_local_model
from relationship_diagram import extract_relationships, draw_static_diagram, draw_interactive_diagram

def generate_doc(input_path):
    if not os.path.exists(input_path):
        print(f"❌ File not found: {input_path}")
        return None, None

    # Step 1: Load SQL code
    sql_code = load_procedure(input_path)

    # Step 2: Extract metadata
    object_name = extract_proc_name(sql_code)
    object_type = detect_object_type(sql_code)
    tables = extract_tables(sql_code)

    # Step 3: Extract relationships
    relationships = extract_relationships(sql_code)
    diagram_path = None
    html_path = None
    if relationships:
        diagram_path = f"{object_name}_diagram.png"
        html_path = f"{object_name}_diagram.html"
        draw_static_diagram(relationships, diagram_path)          # PNG
        draw_interactive_diagram(relationships, html_path)        # HTML

    # Step 4: Generate prompt for LLM
    prompt = f"""
You are an assistant that documents SQL {object_type}s.

Explain the {object_type} below in clear terms. Include:
- What it does
- List all columns selected (no summarizing)
- Tables it reads from or writes to
- Any conditions or joins used

SQL Code:
{sql_code}
    """

    # Step 5: Generate summary via LLM
    summary = generate_doc_from_local_model(prompt)

    # Step 6: Create markdown content
    md_doc = generate_markdown(object_name, tables, summary, diagram_path)

    # Step 7: Save markdown file
    output_path = f"{object_name}_doc.md"
    with open(output_path, "w") as f:
        f.write(md_doc)

    # Logging
    print(f"✅ Documentation written to {output_path}")
    if diagram_path:
        print(f"📊 Static diagram saved as {diagram_path}")
    if html_path:
        print(f"🌐 Interactive diagram saved as {html_path}")

    return output_path, html_path


# CLI usage support
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Please provide the input SQL file path. Example: python main.py myproc.sql")
    else:
        generate_doc(sys.argv[1])
