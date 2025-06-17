from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict
import requests
from pyvis.network import Network
import webbrowser
import sys
import networkx as nx
import matplotlib.pyplot as plt


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
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    try:
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None

def get_links(base_url, html):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        full_url = urljoin(base_url, a['href'])
        links.append(full_url)
    return links

def get_physical_depth(url):
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    return len(path.split('/')) if path else 0

def passes_filter(url, max_physical_depth):
    return url.startswith("http") and get_physical_depth(url) <= max_physical_depth

def main(seed_urls, max_pages_per_site=20, max_logical_depth=3, max_physical_depth=3):
    # Inicializamos las listas vacias
    todo_list = []
    done_list = {}
    url_to_id = {}
    pages_per_site = defaultdict(int)

    # Para las semillas, tomamos su url, le asignamos un ID (y lo agregamos al diccionario de id's), 
    # guardamos su dominio, su profundidad logica, y le asignamos una lista vacia en outlinks.
    for url in seed_urls:
        domain = urlparse(url).netloc
        todo_list.append({
            'url': url,
            'id': len(url_to_id),
            'site_domain': domain,
            'logical_depth': 0,
            'outlinks': []
        })
        url_to_id[url] = len(url_to_id)

    i = 0
    while i < len(todo_list):
        # Tomamos la pagina actual.
        current = todo_list[i]
        url = current['url']
        print(f"Visiting {url}")
        i += 1

        # Tomamos el sitio web / dominio actual.
        domain = current['site_domain']
        logical_depth = current['logical_depth']

        # Si la cantidad de paginas visitadas de un dado dominio, excede la cantidad 
        # limite de este dominio, se ignora la pagina.
        if pages_per_site[domain] >= max_pages_per_site:
            continue

        # Si la pagina no es html, se ignora la pagina.
        html = get_page(url)
        if html is None:
            continue

        # Si la pagina ya se visitó anteriormente, se ignora la pagina.
        if current['url'] in done_list:
            continue

        # Aumentamos el contador de paginas por sitio, agregamos la pagina a la 
        # lista de visitados, y obtenemos sus links salientes
        pages_per_site[domain] += 1
        done_list[url] = current
        links = get_links(url, html)

        
        for l in links:
            if not passes_filter(l, max_physical_depth):
                continue

            # Evitar duplicados en outlinks si ya está
            if l in done_list:
                link_id = done_list[l]['id']
                if link_id not in current['outlinks']:
                    current['outlinks'].append(link_id)

            elif l in url_to_id:
                link_id = url_to_id[l]
                if link_id not in current['outlinks']:
                    current['outlinks'].append(link_id)

            elif logical_depth + 1 <= max_logical_depth:
                if pages_per_site[domain] < max_pages_per_site:
                    new_id = len(url_to_id)
                    url_to_id[l] = new_id
                    todo_list.append({
                        'url': l,
                        'id': new_id,
                        'site_domain': domain,
                        'logical_depth': logical_depth + 1, # Tomamos la profundidad logica del link padre, y le sumamos 1
                        'outlinks': []
                    })
                    current['outlinks'].append(new_id)
                
        current['outlinks'].sort()

    # Mostrar resumen final
    print("\nResumen de crawling:")
    for entry in todo_list:
        print(f"[{entry['id']}] {entry['url']} (Profundidad lógica: {entry['logical_depth']})")
        print(f"   -> outlinks: {entry['outlinks']}, type: {type(entry['outlinks'])}")

    # Grafo con pyvis
    print("\nGenerando grafo de navegación con pyvis :)")

    net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", directed=True)
    net.repulsion()
    # NODOS
    for entry in todo_list:
        net.add_node(
            entry['id'],
            label=f"{entry['id']}",
            title=entry['url'],
            color=f"hsl({(entry['logical_depth'] * 70) % 360}, 70%, 50%)"
        )

    # ARISTAS
    for entry in todo_list:
        for out_id in entry['outlinks']:
            net.add_edge(entry['id'], out_id)

    # MOSTRAR
    net.write_html("grafo_crawler.html")
    print("Grafo guardado en 'grafo_crawler.html'")
    webbrowser.open("grafo_crawler.html")

    analizar_distribuciones(todo_list)



def analizar_distribuciones(todo_list):
    from collections import defaultdict

    total_static = 0
    total_dynamic = 0
    logical_dist = defaultdict(int)
    physical_dist = defaultdict(int)

    for entry in todo_list:

        url = entry['url']
        logical = entry['logical_depth']
        physical = get_physical_depth(url)

        logical_dist[logical] += 1
        physical_dist[physical] += 1

        if '?' in url:
            total_dynamic += 1
        else:
            total_static += 1

    # Mostrar resultados
    print("\nAnálisis de URLs:")

    print(f"\nPáginas estáticas: {total_static}")
    print(f"Páginas dinámicas: {total_dynamic}")

    print("\nDistribución por profundidad lógica:")
    for depth in sorted(logical_dist):
        print(f"  ---   Profundidad lógica {depth}: {logical_dist[depth]} páginas")

    print("\n Distribución por profundidad física:")
    for depth in sorted(physical_dist):
        print(f"  ---   Profundidad física {depth}: {physical_dist[depth]} páginas")


def crawl_only(seed_urls, max_pages=500, max_logical_depth=10, max_physical_depth=10):
    todo_list = []
    done_list = {}
    url_to_id = {}
    pages_per_site = defaultdict(int)

    for url in seed_urls:
        domain = urlparse(url).netloc
        todo_list.append({
            'url': url,
            'id': len(url_to_id),
            'site_domain': domain,
            'logical_depth': 0,
            'outlinks': []
        })
        url_to_id[url] = len(url_to_id)

    i = 0
    while i < len(todo_list) and len(done_list) < max_pages:
        current = todo_list[i]
        url = current['url']
        i += 1

        domain = current['site_domain']
        logical_depth = current['logical_depth']

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
                link_id = done_list[l]['id']
                if link_id not in current['outlinks']:
                    current['outlinks'].append(link_id)

            elif l in url_to_id:
                link_id = url_to_id[l]
                if link_id not in current['outlinks']:
                    current['outlinks'].append(link_id)

            elif logical_depth + 1 <= max_logical_depth:
                if pages_per_site[domain] < 50:
                    new_id = len(url_to_id)
                    url_to_id[l] = new_id
                    todo_list.append({
                        'url': l,
                        'id': new_id,
                        'site_domain': domain,
                        'logical_depth': logical_depth + 1,
                        'outlinks': []
                    })
                    current['outlinks'].append(new_id)

        current['outlinks'].sort()

    return list(done_list.values())

if __name__ == "__main__":
    seed = [
        "https://mercadolibre.com/"
    ]

    mode = sys.argv[1] if len(sys.argv) > 1 else "a"

    if mode == "a":
        main(seed_urls=seed, max_pages_per_site=100, max_logical_depth=50, max_physical_depth=50)

    elif mode == "b":
        todo_list = crawl_only(seed, max_pages=500)
        print(f"Se recolectaron {len(todo_list)} páginas.")

        # Construir grafo dirigido
        G = nx.DiGraph()
        for entry in todo_list:
            G.add_node(entry['id'], url=entry['url'])
            for out in entry['outlinks']:
                G.add_edge(entry['id'], out)

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
        plt.plot(ks, overlap_percent, marker='o', label="Overlap PR vs Auth")
        plt.xlabel("Cantidad de páginas recolectadas")
        plt.ylabel("Porcentaje de overlap")
        plt.title("Overlap entre orden por PageRank y por Authority (HITS)")
        plt.grid(True)
        plt.legend()
        plt.savefig("overlap_pagerank_hits.png")
        plt.show()

        print("✅ Overlap graficado y guardado como 'overlap_pagerank_hits.png'.")
        print("\n📌 Explicación:")
        print("PageRank y Authority no son lo mismo. PR favorece nodos apuntados por muchos, mientras que Authority depende de la calidad de los hubs que apuntan. Por eso el overlap no es perfecto.")




# NETWORK X : random_internet_as_graph 

"""
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
        "https://calendar.google.com"
"""