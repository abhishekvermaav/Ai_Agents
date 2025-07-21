import re
import matplotlib.pyplot as plt
import networkx as nx
from pyvis.network import Network

# Extract relationships from SQL
def extract_relationships(sql_code):
    relationships = []
    tables = set()

    # Read operations: FROM, JOIN
    from_tables = re.findall(r'\bFROM\s+([^\s,;\n]+)', sql_code, re.IGNORECASE)
    join_tables = re.findall(r'\bJOIN\s+([^\s,;\n]+)', sql_code, re.IGNORECASE)
    read_tables = set(from_tables + join_tables)

    for table in read_tables:
        tables.add(table)
        relationships.append(('PROC', table, 'read'))

    # Write operations: INSERT INTO, UPDATE, DELETE FROM
    write_patterns = [
        r'\bINSERT\s+INTO\s+([^\s\(]+)',
        r'\bUPDATE\s+([^\s]+)',
        r'\bDELETE\s+FROM\s+([^\s]+)'
    ]

    for pattern in write_patterns:
        matches = re.findall(pattern, sql_code, re.IGNORECASE)
        for table in matches:
            tables.add(table)
            relationships.append(('PROC', table, 'write'))

    return relationships

# Draw static diagram with color coding
def draw_static_diagram(relationships, filename="diagram.png"):
    G = nx.DiGraph()
    edge_colors = []

    for source, target, rel_type in relationships:
        G.add_edge(source, target)
        edge_colors.append('green' if rel_type == 'read' else 'red')

    plt.figure(figsize=(10, 8))
    pos = nx.spring_layout(G, seed=42)
    nx.draw(
        G,
        pos,
        with_labels=True,
        node_color="lightblue",
        node_size=3000,
        font_size=10,
        font_weight="bold",
        edge_color=edge_colors,
        arrows=True
    )

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='green', edgecolor='black', label='Reads'),
        Patch(facecolor='red', edgecolor='black', label='Writes')
    ]
    plt.legend(handles=legend_elements, loc='upper left')
    plt.title("Table Relationship Diagram")
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

# Draw interactive diagram using Pyvis
def draw_interactive_diagram(relationships, filename="diagram.html"):
    net = Network(height="700px", width="100%", directed=True)
    net.barnes_hut()

    added_nodes = set()
    for source, target, rel_type in relationships:
        for node in [source, target]:
            if node not in added_nodes:
                net.add_node(node, label=node, title=node)
                added_nodes.add(node)

        color = "green" if rel_type == "read" else "red"
        title = "Reads from" if rel_type == "read" else "Writes to"
        net.add_edge(source, target, color=color, title=title, arrows="to")

    net.show_buttons(filter_=['physics'])
    net.write_html(filename)  # ← use write_html instead of show()
