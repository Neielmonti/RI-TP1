import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def extraer_enlaces(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        sopa = BeautifulSoup(response.text, "html.parser")

        links = []
        for a in sopa.find_all("a", href=True):
            enlace_absoluto = urljoin(url, a["href"])  # Canoniza el enlace
            links.append(enlace_absoluto)

        return links

    except requests.RequestException as e:
        print(f"Error al recuperar la página: {e}")
        return []


def main():
    if len(sys.argv) != 2:
        print("Uso: python<version> urlDownloader.py <URL>")
        return

    url = sys.argv[1]
    enlaces = extraer_enlaces(url)

    print("\n\nEnlaces encontrados:")
    for enlace in enlaces:
        print(f" --- {enlace}")


if __name__ == "__main__":
    main()
