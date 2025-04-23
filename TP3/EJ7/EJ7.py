from pathlib import Path
import pyterrier as pt
import pandas as pd
import shutil
import re
import argparse
from collections import defaultdict
import matplotlib.pyplot as plt
import pyterrier.measures as pt_measures

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

    # Crea el directorio con el indice
    index_dir = Path("indice").resolve()
    if index_dir.exists():
        shutil.rmtree(index_dir)
    indexer = pt.IterDictIndexer(str(index_dir), fields=["text"], meta=["docno"])
    indexref = indexer.index(df.to_dict(orient="records"))
    index = pt.IndexFactory.of(indexref)

    # Creamos el modelo de recuperacion (retriever)
    tfidf = pt.terrier.Retriever(index, wmodel="TF_IDF")

    # Y recuperamos los resultados de estas queries
    results = tfidf.transform(queries_df)

    # Esto se hace por compatibilidad de datos.
    queries_df['qid'] = queries_df['qid'].astype(int)
    results['docno'] = results['docno'].astype(str)

    # Agrego un campo de relevancia al ranking, para ver si el documento devuelto es realmente relevante
    results['relevant'] = results.apply(
        lambda row: 1 if str(row['docno']) in row['relevant_docs'] else 0, axis=1
    )
    return results[["qid", "docno", "score", "rank", "relevant"]]


# Función de carga de los documentos relevantes
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


# Función para parsear los títulos de las queries en el archivo TREC
def parse_trec_titles(trec_file_path):
    with open(trec_file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    tops = re.findall(r'<top>(.*?)</top>', content, re.DOTALL)
    qid_to_query = {}

    for top in tops:
        num_match = re.search(r'<num>\s*(\d+)\s*</num>', top)
        title_match = re.search(r'<title>\s*(.*?)</title>', top, re.DOTALL)

        if num_match and title_match:
            qid = int(num_match.group(1).strip())  # Convertimos a int para que coincida con los qid del txt ^.^
            title = title_match.group(1).strip().replace('\n', ' ')
            qid_to_query[qid] = title

    return qid_to_query


# Construir la estructura final con la query y los documentos relevantes
def construir_estructura(qid_to_docs, qid_to_query):
    estructura = {}

    for qid in qid_to_docs:
        estructura[qid] = {
            'query': qid_to_query.get(qid, ''),  # Query asociada
            'relevant_docs': qid_to_docs[qid]  # Documentos relevantes para esa query
        }

    return estructura


def analizar_resultados(results_df):
    print("\n\nAnálisis de resultados")

    # Evaluador de métricas
    evaluator = pt.Utils.evaluate

    # Diccionario con las métricas a evaluar
    metricas = {
        "P@10": "P.10",
        "AP": "AP",
        "NDCG@10": "nDCG@10"
    }

    # Evaluación global (promedio)
    print("\n--- Métricas globales ---")
    global_scores = evaluator(results_df, metrics=list(metricas.values()))
    for nombre, codigo in metricas.items():
        print(f"{nombre}: {global_scores[codigo]:.4f}")

    # Evaluación individual por query
    print("\n--- Métricas individuales por query ---")
    individual_scores = evaluator(results_df, metrics=list(metricas.values()), perquery=True)
    print(individual_scores)

    # Gráfica de Precisión-Recall en 11 puntos
    print("\n--- Curva Precisión-Recall (11 puntos estándar) ---")
    pr_scores = evaluator(results_df, metrics=["Rprec", "recall_11pt"], perquery=True)

    # Promediamos los 11 puntos para la curva PR
    mean_recalls = [sum(x) / len(x) for x in zip(*pr_scores["recall_11pt"])]
    recall_levels = [round(i / 10, 1) for i in range(11)]

    plt.figure(figsize=(8, 5))
    plt.plot(recall_levels, mean_recalls, marker='o', linestyle='-', color='blue')
    plt.title('Curva Precisión-Recall (11 puntos estándar)')
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.grid(True)
    plt.xticks(recall_levels)
    plt.ylim(0, 1.05)
    plt.show()


if __name__ == "__main__":
    # Argumento único: directorio base
    parser = argparse.ArgumentParser(description="Indexador con PyTerrier.")
    parser.add_argument("base_dir", type=str, help="(directorio vaswani) directorio raíz que contiene qrels, query-text.trec y corpus/doc-text.trec")
    args = parser.parse_args()

    base_path = Path(args.base_dir).resolve()

    qid_path = base_path / "qrels"
    trec_path = base_path / "query-text.trec"
    corpus_path = base_path / "corpus" / "doc-text.trec"

    # Al listado de queries, le agregamos una lista de documentos relevantes para cada una
    # Primero cargamos los documentos relevantes por cada qID
    # Luego, cargamos el cuerpo de la query para cada qID
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
    with pd.option_context('display.max_colwidth', None):
        print(primeros_11[['qid', 'query', 'cant_RD']])

    # Ahora, realizamos la búsqueda (que en realidad crea el index, hace las consultas, 
    # y le agrega al ranking el campo de relevancia).
    results = indexar_y_buscar(str(corpus_path), primeros_11)

    # Por ultimo, mostramos los resultados
    for qid, group in results.groupby("qid"):
        print(f"\nResultados para la query {qid}:")
        print(group[["docno", "score", "rank", "relevant"]].head(11).to_string(index=False))

    analizar_resultados(results)
