from bs4 import BeautifulSoup
from pathlib import *
import pyterrier as pt
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

if not pt.started():
    pt.init()

file_index = 0
docs = []

def directory_dfs(path: Path) -> None:
    global file_index

    for x in path.iterdir():

        # Busca recursivamente en las subcarpetas encontradas en este directorio
        if x.is_dir():
            directory_dfs(x)

        # Lee datos de los archivos encontrados en este directorio
        else:
            print(f"Procesando archivo: {file_index}")
            file_index += 1

            # Abrimos el archivo local
            with open(x, "r", encoding="utf-8") as f:
                html = f.read()

            soup = BeautifulSoup(html, features="html.parser")

            # Eliminar elementos script y style
            for script in soup(["script", "style"]):
                script.extract()

            # Obtener el texto visible
            text = soup.get_text()

            # Limpiar líneas vacías y espacios
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            cleaned_text = '\n'.join(chunk for chunk in chunks if chunk)

            docs.append({
                "docno": x.name,
                "text": cleaned_text
            })

            file_index += 1

directory_dfs(Path("/home/kmonti/Desktop/RI-TP1/TP3/EJ4/en"))

df = pd.DataFrame(docs)

indexer = pt.IterDictIndexer("./indice")
indexref = indexer.index(df.to_dict(orient="records"))

index = pt.IndexFactory.of(indexref)
bm25 = pt.BatchRetrieve(index, wmodel="BM25")

results = bm25.search("Isaac Newtone")
print(results[["docno", "score", "rank"]])

indexer = pt.IterDictIndexer("./indice", fields=["text"], meta=["docno"])


