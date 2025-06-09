from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict
import requests
from pyvis.network import Network
import webbrowser


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
    try:
        response = requests.get(url, timeout=5)
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

        # Si la cantidad de paginas visitadas de un dado dominio, excede el la cantidad 
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
                        'logical_depth': logical_depth + 1,
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
    print("\nGenerando grafo de navegación con PyVis...")

    net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", directed=True)

    # Agregar nodos
    for entry in todo_list:
        net.add_node(
            entry['id'],
            label=f"{entry['id']}",
            title=entry['url'],
            color=f"hsl({(entry['logical_depth'] * 70) % 360}, 70%, 50%)"
        )

    # Agregar aristas
    for entry in todo_list:
        for out_id in entry['outlinks']:
            net.add_edge(entry['id'], out_id)

    # Guardar y mostrar
    net.write_html("grafo_crawler.html")
    print("Grafo guardado en 'grafo_crawler.html'")
    webbrowser.open("grafo_crawler.html")




if __name__ == "__main__":
    # Conjunto semilla, 20 primeros sitios del ranking de Netcraft
    seed = [
        "https://www.labredes.unlu.edu.ar/ri2025",
    ]
    main(seed_urls=seed, max_pages_per_site=20, max_logical_depth=3, max_physical_depth=3)


"""
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