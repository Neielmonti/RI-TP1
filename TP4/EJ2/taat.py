from TP4.EJ1.indexer import Indexer
from TP4.EJ2.queryProcessor import QueryProcessor
from pathlib import Path
import argparse
import math
import boolean

class TaatRetriever:
    def __init__(self, path: Path, nDocsToDisc: int, loadIndexFromDisk: bool = False):
        self.indexer = Indexer()
        self.queryProcessor = QueryProcessor()

        if not loadIndexFromDisk:
            self.indexer.index_directory(path, nDocsToDisc)
        self.indexer.load_index()

        self.algebra = boolean.BooleanAlgebra()

    def searchTerm(self, term: str) -> list:
        return self.indexer.search(term)

    def searchQuery(self, query: str) -> list:
        query_terms = self.queryProcessor.process_query(query)
        scores = {}
        term_results = []

        for q_term, q_freq in query_terms:
            postings_list = self.indexer.search(q_term)
            term_results.append((q_term, q_freq, postings_list))

        for term, q_freq, postings_list in term_results:
            df = len(postings_list)
            n_docs = self.indexer.doc_count
            if df == 0:
                continue
            idf = n_docs / df

            for docID, d_freq in postings_list:
                if d_freq <= 0 or q_freq <= 0 or idf <= 1:
                    continue
                weighted_freq = ((1 + math.log(d_freq, 2)) / math.log(idf, 2)) * (1 + math.log(q_freq, 2))
                scores[docID] = scores.get(docID, 0) + weighted_freq

        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_scores

    def getQueryRanking(self, query: str) -> None:
        expression = self.algebra.parse(query, simplify=False)
        docs = self.analyzeBooleanExpression(expression)
        print(f"Retrieved documents for '{query}':")
        print(docs)
        return docs

    def analyzeBooleanExpression(self, expression):
        if isinstance(expression, boolean.boolean.Symbol):
            term_postings = self.indexer.search(str(expression))
            return set([doc_id for doc_id, _ in term_postings])

        elif isinstance(expression, boolean.boolean.NOT):
            all_docs = set(self.indexer.getAllDocsID())
            all_doc_ids = {docID for docID, _, _ in all_docs}
            term_doc = self.analyzeBooleanExpression(expression.args[0])
            
            return all_doc_ids - term_doc

        elif isinstance(expression, boolean.boolean.AND):
            return set.intersection(*[self.analyzeBooleanExpression(arg) for arg in expression.args])

        elif isinstance(expression, boolean.boolean.OR):
            return set.union(*[self.analyzeBooleanExpression(arg) for arg in expression.args])

        raise ValueError("Operador desconocido en expresión booleana.")

    def termIsInVocab(self, term: str) -> bool:
        return self.indexer.term_in_vocab(term)

def main():
    parser = argparse.ArgumentParser(description="Indexador y buscador booleano y ponderado")
    parser.add_argument("path", type=str, help="Ruta al directorio con archivos HTML")
    parser.add_argument("docs", type=int, default=250, help="Cantidad de documentos a procesar antes de volcar a disco")
    parser.add_argument("--load", action="store_true", help="Cargar índice desde disco en lugar de indexar de nuevo")
    args = parser.parse_args()

    taat = TaatRetriever(Path(args.path), args.docs, loadIndexFromDisk=args.load)

    while True:
        query = input("Ingrese una query (booleana o normal): ")
        if any(op in query.upper() for op in ["AND", "OR", "NOT"]):
            taat.getQueryRanking(query)
        else:
            results = taat.searchQuery(query)
            print(f"Top resultados para '{query}':")
            for doc_id, score in results[:10]:
                print(f"Doc: {doc_id} - Score: {score:.4f}")

if __name__ == "__main__":
    main()