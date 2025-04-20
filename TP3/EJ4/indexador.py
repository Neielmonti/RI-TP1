# indexador.py
from bs4 import BeautifulSoup
from pathlib import Path
import pyterrier as pt
import pandas as pd
import shutil

def indexar_y_buscar(input_dir: str, query: str) -> pd.DataFrame:
    if not pt.started():
        pt.init()

    file_index = 0
    docs = []

    def directory_dfs(path: Path) -> None:
        nonlocal file_index
        for x in path.iterdir():
            if x.is_dir():
                directory_dfs(x)
            else:
                with open(x, "r", encoding="utf-8") as f:
                    html = f.read()
                soup = BeautifulSoup(html, features="html.parser")
                for script in soup(["script", "style"]):
                    script.extract()
                text = soup.get_text()
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                cleaned_text = '\n'.join(chunk for chunk in chunks if chunk)
                docs.append({
                    "docno": x.name,
                    "docid": str(file_index),
                    "text": cleaned_text
                })
                file_index += 1

    directory_dfs(Path(input_dir))
    df = pd.DataFrame(docs)

    index_dir = Path("indice").resolve()
    if index_dir.exists():
        shutil.rmtree(index_dir)

    indexer = pt.IterDictIndexer(str(index_dir), fields=["text"], meta=["docno", "docid"])
    indexref = indexer.index(df.to_dict(orient="records"))

    index = pt.IndexFactory.of(indexref)
    bm25 = pt.BatchRetrieve(index, wmodel="BM25")

    results = bm25.search(query)
    return results[["docid", "docno", "score", "rank"]]

