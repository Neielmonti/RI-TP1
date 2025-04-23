# indexador.py
from bs4 import BeautifulSoup
from pathlib import Path
import pyterrier as pt
import pandas as pd
import argparse
import shutil
from scipy.stats import kendalltau, spearmanr

def limpiar_html(input_dir: Path) -> pd.DataFrame:
    docs = []
    file_index = 0

    print("Leyendo y limpiando documentos...")

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
                if file_index % 100 == 0:
                    print(f"  > Documentos procesados: {file_index}")

    directory_dfs(input_dir)
    print(f"Total de documentos procesados: {file_index}")
    return pd.DataFrame(docs)

def indexar_documentos(df: pd.DataFrame):
    index_dir = Path("indice").resolve()
    if index_dir.exists():
        shutil.rmtree(index_dir)

    indexer = pt.IterDictIndexer(str(index_dir), fields=["text"], meta=["docno", "docid"])
    indexref = indexer.index(df.to_dict(orient="records"))
    return pt.IndexFactory.of(indexref)

def comparar_modelos(index, queries: list[str]):
    tfidf = pt.BatchRetrieve(index, wmodel="TF_IDF")
    bm25 = pt.BatchRetrieve(index, wmodel="BM25")

    correlaciones = []

    print("Comparando modelos para cada query...")
    
    for i, query in enumerate(queries):
        print(f"  > Procesando query {i + 1}/{len(queries)}: {query}")
        tfidf_results = tfidf.search(query).reset_index(drop=True)
        bm25_results = bm25.search(query).reset_index(drop=True)

        merged = pd.merge(tfidf_results, bm25_results, on="docno", suffixes=("_tfidf", "_bm25"))

        for k in [10, 25, 50]:
            top_k = merged.head(k)
            if len(top_k) < k:
                continue  # saltar si no hay suficientes documentos

            # Rankings: cuanto más arriba en la lista, menor es el valor de "rank"
            ranks_tfidf = top_k["rank_tfidf"]
            ranks_bm25 = top_k["rank_bm25"]

            spearman = spearmanr(ranks_tfidf, ranks_bm25).correlation
            kendall = kendalltau(ranks_tfidf, ranks_bm25).correlation
            correlaciones.append({
                "query": query,
                "top_k": k,
                "spearman": spearman,
                "kendall": kendall
            })

    return pd.DataFrame(correlaciones)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Indexa y compara modelos TF-IDF y BM25 con PyTerrier")
    parser.add_argument("input_dir", type=str, help="Ruta al directorio wiki-small")
    parser.add_argument("compare_models", type=str, help="Indica si se quiere comparar modelos o no (values: [y,n])")
    args = parser.parse_args()

    if not pt.java.started():
        pt.java.init()

    input_path = Path(args.input_dir).resolve()
    df_docs = limpiar_html(input_path)
    index = indexar_documentos(df_docs)

    # 5 consultas manuales derivadas de necesidades de información
    queries = [
        "historia de la segunda guerra mundial",
        "procesos de fotosíntesis en plantas",
        "biografía de Albert Einstein",
        "impacto del cambio climático en los océanos",
        "estructura del sistema solar"
    ]

    if (args.compare_models == "y"):
        correlaciones_df = comparar_modelos(index, queries)
        print(correlaciones_df)
    elif (args.compare_models == "n"):
        tfidf = pt.BatchRetrieve(index, wmodel="TF_IDF")
        for query in queries:
            tfidf_results = tfidf.search(query).reset_index(drop=True)
            print(tfidf_results.head(11))
    else:
        print("valor invalido en compare_models")


