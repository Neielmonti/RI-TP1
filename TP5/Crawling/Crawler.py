from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict
import requests
from pyvis.network import Network
import webbrowser
import sys
import networkx as nx
import matplotlib.pyplot as plt
import tldextract

"""
1: push(todo_list.initial_set_of_urls)
2: while todo_list[0] = Null do
    3: page <-- fetch_page(todo_list[0])

    4: if page downloaded then

        5: links <-- parse(page)

        6: for all in links do

            7: if l in done list then
                8: push(todo_list[0].outlinks,done_list[l].id)

            9: else if l in todo list then
                10: push(todo_list[0].outlinks,todo_list[l].id)

            11: else if l pass our filter then
                12: push(todo list,l)
                13: todo_list[l].id=no. of url's 
                14: push(todo_list[0].outlinks,todo_list[l].id)
"""


def get_page(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()

        # Verificar que el contenido es HTML
        content_type = response.headers.get("Content-Type", "")
        if "html" not in content_type:
            return None

        return response.text
    except requests.RequestException:
        return None


def get_links(base_url, html):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        full_url = urljoin(base_url, a["href"])
        links.append(full_url)
    return links


def get_physical_depth(url):
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    return len(path.split("/")) if path else 0


def normalize_domain(url):
    extracted = tldextract.extract(url)
    return f"{extracted.domain}.{extracted.suffix}"


def passes_filter(url, max_physical_depth):
    return url.startswith("http") and get_physical_depth(url) <= max_physical_depth


def free_crawling(
    seed_urls,
    max_pages_per_site=20,
    max_logical_depth=3,
    max_physical_depth=3,
    max_total_pages=500,
):
    todo_list = []
    done_list = {}
    url_to_id = {}
    pages_per_site = defaultdict(int)

    for url in seed_urls:
        domain = normalize_domain(url)
        todo_list.append(
            {
                "url": url,
                "id": len(url_to_id),
                "site_domain": domain,
                "logical_depth": 0,
                "outlinks": [],
            }
        )
        url_to_id[url] = len(url_to_id)

    i = 0
    while i < len(todo_list):
        if len(done_list) >= max_total_pages:
            print(f"\nSe alcanzó el máximo global de {max_total_pages} páginas.")
            break

        current = todo_list[i]
        url = current["url"]
        i += 1

        domain = current["site_domain"]
        logical_depth = current["logical_depth"]

        print(f"\n [i: {i}] Visiting {url}")

        if pages_per_site[domain] >= max_pages_per_site:
            continue

        html = get_page(url)
        if html is None:
            continue

        if url in done_list:
            continue

        pages_per_site[domain] += 1
        done_list[url] = current
        links = get_links(url, html)

        for l in links:
            if not passes_filter(l, max_physical_depth):
                continue

            if l in done_list:
                link_id = done_list[l]["id"]
                if link_id not in current["outlinks"]:
                    current["outlinks"].append(link_id)

            elif l in url_to_id:
                link_id = url_to_id[l]
                if link_id not in current["outlinks"]:
                    current["outlinks"].append(link_id)

            elif logical_depth + 1 <= max_logical_depth:
                domain_l = normalize_domain(l)
                if (
                    pages_per_site[domain_l] < max_pages_per_site
                    and len(url_to_id) < max_total_pages
                ):
                    new_id = len(url_to_id)
                    url_to_id[l] = new_id
                    todo_list.append(
                        {
                            "url": l,
                            "id": new_id,
                            "site_domain": domain_l,
                            "logical_depth": logical_depth + 1,
                            "outlinks": [],
                        }
                    )
                    current["outlinks"].append(new_id)

        current["outlinks"].sort()

    # Mostrar resumen final
    print("\n\nResumen de crawling:")
    for entry in todo_list:
        print(
            f"[{entry['id']}] {entry['url']} (Profundidad lógica: {entry['logical_depth']})"
        )
        print(f"   -> outlinks: {entry['outlinks']}")

    dominios_unicos = {entry["site_domain"] for entry in todo_list}
    print(f"\nDominios visitados: {dominios_unicos}")

    # Grafo con pyvis
    print("\nGenerando grafo de navegación con pyvis :)")
    net = Network(
        height="750px",
        width="100%",
        bgcolor="#222222",
        font_color="white",
        directed=True,
    )
    net.repulsion()

    for entry in todo_list:
        url = entry["url"]
        outlinks = entry["outlinks"]
        is_dynamic = "?" in url
        is_seed = entry["logical_depth"] == 0
        is_leaf = len(outlinks) == 0

        if is_seed:
            color = "red"
        elif is_dynamic:
            color = "deepskyblue"
        elif is_leaf:
            color = "limegreen"
        else:
            color = "yellow"

        net.add_node(entry["id"], label=f"{entry['id']}", title=url, color=color)

    for entry in todo_list:
        for out_id in entry["outlinks"]:
            net.add_edge(entry["id"], out_id)

    net.write_html("grafo_crawler.html")
    print("Grafo guardado en 'grafo_crawler.html'")
    webbrowser.open("grafo_crawler.html")

    analizar_distribuciones(todo_list)


def analizar_distribuciones(todo_list):
    # Inicializamos las estructuras vacias.
    total_static = 0
    total_dynamic = 0
    logical_dist = defaultdict(int)
    physical_dist = defaultdict(int)

    # Recorremos cada pagina, contabilizando sus datos.
    for entry in todo_list:
        url = entry["url"]
        logical = entry["logical_depth"]
        physical = get_physical_depth(url)
        logical_dist[logical] += 1
        physical_dist[physical] += 1

        if "?" in url:
            total_dynamic += 1
        else:
            total_static += 1

    # Mostrar resultados
    print("\nAnálisis de URLs:")

    print(f"\n - Cantidad de páginas estáticas: {total_static}")
    print(f" - Páginas dinámicas: {total_dynamic}")

    print("\n - Distribución por profundidad lógica:")
    for depth in sorted(logical_dist):
        print(f"  ---   Profundidad lógica {depth}: {logical_dist[depth]} páginas")

    print("\n - Distribución por profundidad física:")
    for depth in sorted(physical_dist):
        print(f"  ---   Profundidad física {depth}: {physical_dist[depth]} páginas")


def crawl_only(seed_urls, max_pages=4, max_logical_depth=2, max_physical_depth=2):
    # Inicializamos las estructuras vacías.
    todo_list = []
    done_list = {}
    url_to_id = {}
    pages_per_site = defaultdict(int)

    # Agregamos las URLs semilla
    for url in seed_urls:
        domain = normalize_domain(url)
        todo_list.append(
            {
                "url": url,
                "id": len(url_to_id),
                "site_domain": domain,
                "logical_depth": 0,
                "outlinks": [],
            }
        )
        url_to_id[url] = len(url_to_id)

    i = 0
    while i < len(todo_list) and len(done_list) < max_pages:
        current = todo_list[i]
        url = current["url"]
        i += 1

        domain = current["site_domain"]
        logical_depth = current["logical_depth"]

        print(f"\n [i = {i}] Visiting: {url}")

        if pages_per_site[domain] >= 50:
            continue

        html = get_page(url)
        if html is None:
            continue

        if url in done_list:
            continue

        pages_per_site[domain] += 1
        done_list[url] = current
        links = get_links(url, html)

        for l in links:
            if not passes_filter(l, max_physical_depth):
                continue

            if l in done_list:
                link_id = done_list[l]["id"]
                if link_id not in current["outlinks"]:
                    current["outlinks"].append(link_id)

            elif l in url_to_id:
                link_id = url_to_id[l]
                if link_id not in current["outlinks"]:
                    current["outlinks"].append(link_id)

            elif (
                logical_depth + 1 <= max_logical_depth
                and len(url_to_id) < max_pages
            ):
                domain_l = normalize_domain(l)
                if pages_per_site[domain_l] < 50:
                    new_id = len(url_to_id)
                    url_to_id[l] = new_id
                    todo_list.append(
                        {
                            "url": l,
                            "id": new_id,
                            "site_domain": domain_l,
                            "logical_depth": logical_depth + 1,
                            "outlinks": [],
                        }
                    )
                    current["outlinks"].append(new_id)

        current["outlinks"].sort()

    print(f"\nTotal de páginas procesadas: {len(done_list)}")
    print(f"Total de URLs en todo_list: {len(todo_list)}")
    return list(done_list.values())


def nx_graph(todo_list):
    # Construir grafo dirigido
    G = nx.DiGraph()
    for entry in todo_list:
        G.add_node(entry["id"], url=entry["url"])
        for out in entry["outlinks"]:
            G.add_edge(entry["id"], out)

    # Calcular PageRank y HITS
    pagerank = nx.pagerank(G, alpha=0.85)
    hits_auth, _ = nx.hits(G, max_iter=1000)

    # Ordenamientos
    orden_pr = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)
    orden_auth = sorted(hits_auth.items(), key=lambda x: x[1], reverse=True)

    # Calcular overlap
    overlap_percent = []
    ks = list(range(10, min(501, len(G.nodes)), 10))
    for k in ks:
        top_pr = set([node for node, _ in orden_pr[:k]])
        top_auth = set([node for node, _ in orden_auth[:k]])
        inter = top_pr & top_auth
        overlap = len(inter) / k
        overlap_percent.append(overlap)

    # Graficar
    plt.figure(figsize=(10, 6))
    plt.plot(ks, overlap_percent, marker="o", label="Overlap PR vs Auth")
    plt.xlabel("Cantidad de páginas recolectadas")
    plt.ylabel("Porcentaje de overlap")
    plt.title("Overlap entre orden por PageRank y por Authority (HITS)")
    plt.grid(True)
    plt.legend()
    plt.savefig("overlap_pagerank_hits.png")
    plt.show()

    print("\n\nOverlap graficado y guardado en el archivo.")


if __name__ == "__main__":

    seedEj2 = [
        "https://www.google.com",
        "https://www.youtube.com",
        "https://mail.google.com",
        "https://outlook.office.com",
        "https://www.facebook.com",
        "https://docs.google.com",
        "https://chatgpt.com",
        "https://login.microsoftonline.com",
        "https://www.linkedin.com",
        "https://accounts.google.com",
        "https://x.com",
        "https://www.bing.com",
        "https://www.instagram.com",
        "https://drive.google.com",
        "https://campus-1001.ammon.cloud",
        "https://github.com",
        "https://duckduckgo.com",
        "https://web.whatsapp.com",
        "https://www.reddit.com",
        "https://calendar.google.com",
    ]

    seedEj3 = ["https://www.wikipedia.org"]

    mode = sys.argv[1] if len(sys.argv) > 1 else "a"

    if mode == "ej2":
        free_crawling(
            seed_urls=seedEj2,
            max_pages_per_site=20,
            max_logical_depth=3,
            max_physical_depth=3,
            max_total_pages=1000,
        )

    if mode == "ej3":
        free_crawling(
            seed_urls=seedEj3,
            max_pages_per_site=10,
            max_logical_depth=3,
            max_physical_depth=3,
        )

    elif mode == "ej5":
        todo_list = crawl_only(seedEj2, max_pages=500)
        print(f"Se recolectaron {len(todo_list)} páginas.")
        nx_graph(todo_list)
