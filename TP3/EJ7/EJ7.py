from pathlib import Path
import pyterrier as pt
import pandas as pd
import shutil
import re
import argparse
from collections import defaultdict
import matplotlib.pyplot as plt
import pyterrier.measures as pt_measures
from pyterrier.terrier import Retriever
import numpy as np


def indexar_y_buscar(input_dir: str, queries_df) -> pd.DataFrame:
    if not pt.java.started():
        pt.java.init()

    docs = []

    def directory_dfs(path: Path) -> None:
        pattern = r"<DOCNO>\s*([0-9]+)\s*</DOCNO>"

        with open(path, encoding="utf-8") as trec_file:
            text = trec_file.readline().strip()
            while text:
                if text == "<DOC>":
                    content_lines = []
                    docno = None

                    text = trec_file.readline().strip()
                    while text and text != "</DOC>":
                        if "<DOCNO>" in text:
                            docno_match = re.search(pattern, text)
                            if docno_match:
                                docno = docno_match.group(1)
                        else:
                            content_lines.append(text)
                        text = trec_file.readline().strip()

                    if docno and content_lines:
                        lines = (line.strip() for line in content_lines)
                        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                        cleaned_text = '\n'.join(chunk for chunk in chunks if chunk)

                        if cleaned_text.strip():
                            docs.append({
                                "docno": str(docno),
                                "text": cleaned_text
                            })

                text = trec_file.readline().strip()

    directory_dfs(Path(input_dir))
    df = pd.DataFrame(docs)

    index_dir = Path("indice").resolve()
    if index_dir.exists():
        shutil.rmtree(index_dir)

    indexer = pt.IterDictIndexer(str(index_dir), fields=["text"], meta=["docno"])
    indexref = indexer.index(df.to_dict(orient="records"))

    index = pt.IndexFactory.of(indexref)
    tfidf = pt.terrier.Retriever(index, wmodel="TF_IDF")

    results = tfidf.transform(queries_df)

    queries_df['qid'] = queries_df['qid'].astype(int)
    results['docno'] = results['docno'].astype(str)

    results['relevant'] = results.apply(
        lambda row: 1 if str(row['docno']) in row['relevant_docs'] else 0, axis=1
    )

    return results[["qid", "docno", "score", "rank", "relevant"]]


def cargar_relevantes(path_txt):
    relevantes = defaultdict(list)
    with open(path_txt, "r", encoding="utf-8") as f:
        for linea in f:
            partes = linea.strip().split()
            if len(partes) >= 3:
                query_id = int(partes[0])
                doc_id = partes[2]
                relevantes[query_id].append(doc_id)
    return dict(relevantes)


def parse_trec_titles(trec_file_path):
    with open(trec_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    tops = re.findall(r'<top>(.*?)</top>', content, re.DOTALL)
    qid_to_query = {}
    for top in tops:
        num_match = re.search(r'<num>\s*(\d+)\s*</num>', top)
        title_match = re.search(r'<title>\s*(.*?)</title>', top, re.DOTALL)
        if num_match and title_match:
            qid = int(num_match.group(1).strip())
            title = title_match.group(1).strip().replace('\n', ' ')
            qid_to_query[qid] = title
    return qid_to_query


def construir_estructura(qid_to_docs, qid_to_query):
    estructura = {}
    for qid in qid_to_docs:
        estructura[qid] = {
            'query': qid_to_query.get(qid, ''),
            'relevant_docs': qid_to_docs[qid]
        }
    return estructura


def precision_at_k(results, k):
    return results.head(k)['relevant'].sum() / k


def average_precision(results):
    relevant_docs = 0
    ap = 0
    for rank, row in results.iterrows():
        if row['relevant'] == 1:
            relevant_docs += 1
            ap += relevant_docs / (rank + 1)
    return ap / relevant_docs if relevant_docs > 0 else 0


def ndcg_at_k(results, k):
    dcg = 0
    idcg = 0
    for rank, row in results.head(k).iterrows():
        if row['relevant'] == 1:
            dcg += 1 / np.log2(rank + 2)
        idcg += 1 / np.log2(rank + 2)
    return dcg / idcg if idcg > 0 else 0


def plot_precision_recall_interpolado(results):
    # Agrupamos por query y acumulamos los resultados
    qids = results['qid'].unique()
    all_precisions = {r: [] for r in np.linspace(0.0, 1.0, 11)}

    for qid in qids:
        query_results = results[results['qid'] == qid].sort_values(by="rank")
        total_relevant = query_results['relevant'].sum()
        if total_relevant == 0:
            continue

        precisiones = []
        recalls = []
        relevant_retrieved = 0

        for i, (_, row) in enumerate(query_results.iterrows(), start=1):
            if row['relevant'] == 1:
                relevant_retrieved += 1
            prec = relevant_retrieved / i
            rec = relevant_retrieved / total_relevant
            precisiones.append(prec)
            recalls.append(rec)

        # Interpolación: para cada punto de recall estándar, tomamos la mayor precisión alcanzada
        for recall_level in all_precisions:
            precisiones_a_incluir = [p for p, r in zip(precisiones, recalls) if r >= recall_level]
            max_prec = max(precisiones_a_incluir) if precisiones_a_incluir else 0
            all_precisions[recall_level].append(max_prec)

    # Calculamos el promedio por cada nivel de recall
    avg_precisions = [np.mean(all_precisions[r]) for r in sorted(all_precisions)]

    # Graficamos
    plt.figure(figsize=(8, 6))
    plt.plot(np.linspace(0.0, 1.0, 11), avg_precisions, marker='o')
    plt.xlabel('Recall')
    plt.ylabel('Precisión interpolada')
    plt.title('Curva de Precisión Interpolada (11 puntos estándar)')
    plt.grid(True)
    plt.ylim(0, 1.05)
    plt.show()


def calcular_metricas(results):
    global_metrics = {
        'P@10': precision_at_k(results, 10),
        'AP': average_precision(results),
        'NDCG@10': ndcg_at_k(results, 10)
    }
    return global_metrics


def analizar_metrica_por_query(results):
    individual_metrics = {}
    for qid, group in results.groupby('qid'):
        individual_metrics[qid] = {
            'P@10': precision_at_k(group, 10),
            'AP': average_precision(group),
            'NDCG@10': ndcg_at_k(group, 10)
        }
    return individual_metrics

def mostrar_metricas_en_tabla(resultados_por_query):
    df = pd.DataFrame.from_dict(resultados_por_query, orient='index')
    df.index.name = "Query"
    df = df[["P@10", "AP", "NDCG@10"]]  # Asegura el orden de las columnas
    print("\nMétricas por query (formato tabular):\n")
    print(df.round(4))  # Podés ajustar los decimales si querés

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Indexador con PyTerrier.")
    parser.add_argument("base_dir", type=str, help="(directorio vaswani) directorio raíz que contiene qrels, query-text.trec y corpus/doc-text.trec")
    args = parser.parse_args()

    base_path = Path(args.base_dir).resolve()

    qid_path = base_path / "qrels"
    trec_path = base_path / "query-text.trec"
    corpus_path = base_path / "corpus" / "doc-text.trec"

    # Al listado de queries, le agregamos una lista de documentos relevantes para cada una
    qid_to_docs = cargar_relevantes(qid_path)
    qid_to_query = parse_trec_titles(trec_path)
    estructura_final = construir_estructura(qid_to_docs, qid_to_query)

    # Creamos un dataframe de queries con la lista de documentos relevantes
    queries_df = pd.DataFrame([
        {"qid": qid, "query": info["query"], "relevant_docs": info["relevant_docs"], "cant_RD": len(info["relevant_docs"])}
        for qid, info in estructura_final.items()
    ])

    # Tomamos las primeras 11 queries, para probar el modelo
    primeros_11 = queries_df.head(11)

    # Realizamos la búsqueda
    results = indexar_y_buscar(str(corpus_path), primeros_11)

    # Análisis global
    global_metrics = calcular_metricas(results)
    print("Métricas Globales:")
    for metric, value in global_metrics.items():
        print(f"{metric}: {value}")

    # Análisis individual por query
    individual_metrics = analizar_metrica_por_query(results)
    mostrar_metricas_en_tabla(individual_metrics)

    # Graficamos Precisión-Recall
    plot_precision_recall_interpolado(results)