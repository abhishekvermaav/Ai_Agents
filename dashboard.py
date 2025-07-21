import streamlit as st
import streamlit.components.v1 as components
import os
import zipfile
from main import generate_doc

st.set_page_config(page_title="📄 SQL Documentation Generator", layout="wide")
st.title("📄 SQL Documentation Generator with Diagrams")

st.markdown("""
Upload one or more `.sql` files **or a `.zip`** archive containing SQL procedures.
The tool will generate Markdown documentation and an interactive relationship diagram.
""")

uploaded_file = st.file_uploader("📁 Upload SQL File or ZIP", type=["sql", "zip"])

def handle_upload(uploaded_file):
    input_paths = []

    if uploaded_file.name.endswith(".zip"):
        zip_dir = "unzipped_sql"
        os.makedirs(zip_dir, exist_ok=True)

        with zipfile.ZipFile(uploaded_file, "r") as zip_ref:
            zip_ref.extractall(zip_dir)

        for root, _, files in os.walk(zip_dir):
            for f in files:
                if f.endswith(".sql"):
                    input_paths.append(os.path.join(root, f))
    else:
        input_path = uploaded_file.name
        with open(input_path, "wb") as f:
            f.write(uploaded_file.read())
        input_paths.append(input_path)

    return input_paths

if uploaded_file:
    sql_paths = handle_upload(uploaded_file)

    for input_path in sql_paths:
        st.markdown("---")
        st.subheader(f"📋 Processing: `{os.path.basename(input_path)}`")

        with st.spinner("Generating documentation and diagrams..."):
            doc_path, diagram_path = generate_doc(input_path)

        base_name = os.path.splitext(os.path.basename(input_path))[0]

        if doc_path and os.path.exists(doc_path):
            with open(doc_path, "r") as f:
                st.markdown(f"#### 📄 Markdown Documentation for `{base_name}`")
                st.markdown(f.read())

            with open(doc_path, "rb") as f:
                st.download_button(
                    label="⬇️ Download Markdown",
                    data=f,
                    file_name=os.path.basename(doc_path),
                    mime="text/markdown"
                )
        else:
            st.warning("⚠️ Documentation not generated.")

        st.markdown(f"#### 📊 Diagram for `{base_name}`")

        if diagram_path and os.path.exists(diagram_path):
            if diagram_path.endswith(".html"):
                with open(diagram_path, "r", encoding="utf-8") as f:
                    html_content = f.read()
                components.html(html_content, height=700, scrolling=True)

                with open(diagram_path, "rb") as f:
                    st.download_button(
                        label="⬇️ Download Interactive HTML Diagram",
                        data=f,
                        file_name=os.path.basename(diagram_path),
                        mime="text/html"
                    )
            elif diagram_path.endswith(".png"):
                st.image(diagram_path)

                with open(diagram_path, "rb") as f:
                    st.download_button(
                        label="⬇️ Download PNG Diagram",
                        data=f,
                        file_name=os.path.basename(diagram_path),
                        mime="image/png"
                    )
            else:
                st.warning("⚠️ Diagram format not recognized.")
        else:
            st.warning("⚠️ No diagram found.")

    st.success("✅ All files processed.")
