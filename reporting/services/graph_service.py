import pickle
import networkx as nx

class GraphService:
    def __init__(self, graph_path: str = "relationship_graph_llm.pkl"):
        # 네트워크 그래프 로딩 (pickle 방식)
        with open(graph_path, "rb") as f:
            self.G = pickle.load(f)

    def get_related_stocks(self, ticker: str, max_depth: int = 2):
        target_node = None
        for node, data in self.G.nodes(data=True):
            if data.get("stock_code") == ticker:
                target_node = node
                break

        if not target_node:
            return []

        # 2) shortest path 탐색
        distances = nx.single_source_shortest_path_length(
            self.G,
            target_node,
            cutoff=max_depth
        )

        related = []
        for name, dist in distances.items():
            if name == target_node:
                continue

            data = self.G.nodes[name]

            related.append({
                "name": name,
                "distance": dist,
                "corp_code": data.get("corp_code"),
                "stock_code": data.get("stock_code"),
                "business": data.get("business"),
                "region": data.get("region")
            })

        return sorted(related, key=lambda x: x["distance"])
