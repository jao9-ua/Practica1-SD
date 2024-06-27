import networkx as nx

def heuristic(a, b):
    return abs(a[0] - b[0]) + abs(a[1] - b[1])

# Crear un grafo de ejemplo
G = nx.grid_2d_graph(20, 20)

# Añadir obstáculos
obstacles = [(1, 0), (1, 1), (0, 1)]
for obstacle in obstacles:
    G.remove_node(obstacle)

# Definir el nodo inicial y final
start = (0, 0)
goal = (4, 4)

# Encontrar el camino más corto usando A*
try:
    path = nx.astar_path(G, start, goal, heuristic=heuristic)
    print("Camino encontrado:", path)
except nx.NetworkXNoPath:
    print("No se encontró un camino.")
