import matplotlib.pyplot as plt
import networkx as nx

# Create a directed graph for the ETL process
G = nx.DiGraph()

# Nodes and labels
nodes = [
    "Data Source", 
    "Raw Transactions", 
    "Duplicate Records Detected", 
    "ETL De-Duplication", 
    "Clean Transactions", 
    "Reconciled Report"
]
labels = {
    "Data Source": "Data Source\n(e.g., ATM, Online Banking)",
    "Raw Transactions": "Raw Transactions",
    "Duplicate Records Detected": "Duplicate Records Detected\n(e.g., John Smith, J. Smith)",
    "ETL De-Duplication": "ETL Process:\nDe-Duplication",
    "Clean Transactions": "Clean Transactions",
    "Reconciled Report": "Reconciled Report\n(Accurate and Verified)"
}

# Add nodes
G.add_nodes_from(nodes)

# Add edges
edges = [
    ("Data Source", "Raw Transactions"),
    ("Raw Transactions", "Duplicate Records Detected"),
    ("Duplicate Records Detected", "ETL De-Duplication"),
    ("ETL De-Duplication", "Clean Transactions"),
    ("Clean Transactions", "Reconciled Report")
]
G.add_edges_from(edges)

# Plotting
plt.figure(figsize=(10, 6))
pos = nx.spring_layout(G, seed=42)
nx.draw(
    G, pos, with_labels=True, node_color="lightblue", edge_color="gray", node_size=3000, font_size=10
)
nx.draw_networkx_labels(G, pos, labels, font_size=10, font_color="black")
plt.title("ETL Process for De-Duplication in Banking Transactions")
plt.show()

