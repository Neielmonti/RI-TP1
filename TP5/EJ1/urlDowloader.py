import sys
import requests
from bs4 import BeautifulSoup

def extraer_enlaces(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        sopa = BeautifulSoup(response.text, 'html.parser')

        links = []
        for a in sopa.find_all('a', href=True):
            links.append(a['href'])

        return links

    except requests.RequestException as e:
        print(f"Error al descargar la página: {e}")
        return []

def main():
    if len(sys.argv) != 2:
        print("Uso: python<version> EJ1.py <URL>")
        return

    url = sys.argv[1]
    enlaces = extraer_enlaces(url)

    print("\nEnlaces encontrados:")
    for enlace in enlaces:
        print(enlace)

if __name__ == "__main__":
    main()