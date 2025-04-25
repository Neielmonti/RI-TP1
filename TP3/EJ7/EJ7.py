from pathlib import Path
import pyterrier as pt
import pandas as pd
import shutil
import re
import argparse
from collections import defaultdict


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
                        chunks = (
                            phrase.strip()
                            for line in lines
                            for phrase in line.split("  ")
                            )
                        cleaned_text = '\n'.join(
                            chunk for chunk in chunks if chunk
                            )

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

    indexer = pt.IterDictIndexer(
        str(index_dir),
        fields=["text"],
        meta=["docno"]
    )
    indexref = indexer.index(df.to_dict(orient="records"))

    index = pt.IndexFactory.of(indexref)
    tfidf = pt.terrier.Retriever(index, wmodel="TF_IDF")

    results = tfidf.transform(queries_df)

    queries_df['qid'] = queries_df['qid'].astype(int)
    results['docno'] = results['docno'].astype(str)

    results['relevant'] = results.apply(
        lambda row: 1
        if str(row['docno']) in row['relevant_docs']
        else 0, axis=1
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


def construir_df_de_queries(base_dir: Path) -> pd.DataFrame:
    qrels_path = base_dir / "qrels"
    trec_path = base_dir / "query-text.trec"

    qid_to_docs = cargar_relevantes(qrels_path)
    qid_to_query = parse_trec_titles(trec_path)

    data = []
    for qid in sorted(qid_to_docs):
        data.append({
            "qid": qid,
            "query": qid_to_query.get(qid, ''),
            "relevant_docs": qid_to_docs[qid],
            "cant_RD": len(qid_to_docs[qid])
        })

    return pd.DataFrame(data)


def construir_qrels_df(qrels_path: Path) -> pd.DataFrame:
    data = []
    with open(qrels_path, "r", encoding="utf-8") as f:
        for line in f:
            partes = line.strip().split()
            if len(partes) >= 3:
                qid = int(partes[0])
                docno = str(partes[2])
                data.append({"qid": qid, "docno": docno, "label": 1})
    return pd.DataFrame(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Indexador con PyTerrier.")
    parser.add_argument(
        "base_dir",
        type=str,
        help="""(directorio vaswani) directorio raíz que contiene qrels,
        query-text.trec y corpus/doc-text.trec"""
    )
    args = parser.parse_args()

    base_path = Path(args.base_dir).resolve()
    corpus_path = base_path / "corpus" / "doc-text.trec"

    # Creamos el dataframe de queries con sus documentos relevantes
    queries_df = construir_df_de_queries(base_path)

    # Usamos las primeras 11 queries como ejemplo
    primeros_11 = queries_df.head(11)

    print(primeros_11)

    # Ejecutamos la búsqueda con PyTerrier
    results = indexar_y_buscar(str(corpus_path), primeros_11)

    # Y mostramos resultados por query
    for qid, group in results.groupby("qid"):
        print(f"\nResultados para la query {qid}:")
        print(group[[
            "docno",
            "score",
            "rank",
            "relevant"
        ]].head(11).to_string(index=False))
