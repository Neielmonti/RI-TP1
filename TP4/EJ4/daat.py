from TP4.EJ1.indexer import Indexer
from TP4.EJ2.queryProcessor import QueryProcessor
from pathlib import Path
import argparse
import math
import boolean

class DaatRetriever:
    def __init__(self, path: Path, nDocsToDisc: int, loadIndexFromDisk: bool = False, indexer = Indexer(True)):
        self.indexer = indexer
        self.queryProcessor = QueryProcessor()

        if not loadIndexFromDisk:
            self.indexer.index_directory(path, nDocsToDisc)
            self.indexer.build_vocabulary()
        self.indexer.load_index()

        self.algebra = boolean.BooleanAlgebra()

    def searchTerm(self, term: str) -> list:
        return self.indexer.search(term)

    def get_by_docID(self, postingList, x):
        for item in postingList:
            if item[1] == x:
                return item
        return None
    
    def searchQuery(self, query: str) -> list:
        query_terms = self.queryProcessor.process_query(query)

        scores = {}
        term_results = []

        for q_term, q_freq in query_terms:
            postings_list = self.indexer.search(q_term)
            term_results.append((q_term, q_freq, postings_list))

        aux = self.indexer.getAllDocsID()
        all_docs = [doc_id for _, doc_id in aux]

        docNorms = self.indexer.getDocNorms()
        n_docs = self.indexer.doc_count
        query_norm, idf_terms = self.compute_query_norm(term_results, n_docs)

        for docID in all_docs:
            for term, q_freq, postingList in term_results:
                df = len(postingList)
                if df == 0:
                    continue

                # Obtengo la tupla que tiene el docID actual en esta posting list (si es que existe)
                docActual = self.get_by_docID(postingList, docID)

                if docActual:
                    docName, _, d_freq = docActual
                    idf = idf_terms[term]
                    
                    if d_freq <= 0 or q_freq <= 0 or idf <= 0:
                        continue

                    tfidf_d = (1 + math.log(d_freq, 2)) * idf
                    tfidf_q = (1 + math.log(q_freq, 2)) * idf
                    
                    if docID in scores:
                        scores[docID] = (scores[docID][0] + tfidf_d * tfidf_q, docName)
                    else:
                        scores[docID] = (tfidf_d * tfidf_q, docName)

        if query_norm > 0:
            for docID in scores:
                doc_norm = docNorms[docID]

                if doc_norm > 0:
                    scores[docID] = (scores[docID][0] / (query_norm * doc_norm) , scores[docID][1])

        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        return [(docName, docID, score) for docID, (score, docName) in sorted_scores]
    
    def compute_query_norm(self, term_results: list, total_docs: int) -> tuple[float, dict]:
        query_norm = 0.0
        idf_terms = {}

        for term, q_freq, postings_list in term_results:
            df = len(postings_list)
            if df == 0:
                continue
            idf = math.log(total_docs / df, 2)
            idf_terms[term] = idf
            tfidf_q = (1 + math.log(q_freq, 2)) * idf
            query_norm += tfidf_q ** 2

        return math.sqrt(query_norm), idf_terms

    def getQueryRanking(self, query: str, top: int = 10) -> None:

        if any(op in query.upper() for op in ["AND", "OR", "NOT"]):
            expression = self.algebra.parse(query, simplify=False)
            docs = self.analyzeBooleanExpression(expression)
            print(f"\nDocumentos recuperados para '{query}':")
            for docname, docID, _ in docs:
                print(f"-- {docname} : {docID}")
    
        else:
            docs = self.searchQuery(query)
            print(f"\nTop[{top}] resultados para '{query}':")
            for docname, docID, score in docs[:top]:
                print(f"-- {docname} : {docID} : {score:.4f}")
        
        if not docs:
            print("-- NO DOCUMENTS FOUND")

        print("\n\n")
        return docs

    def analyzeBooleanExpression(self, expression):
        if isinstance(expression, boolean.boolean.Symbol):
            # Devuelve lista de tuplas (docname, docid)
            return set(self.indexer.search(str(expression)))

        elif isinstance(expression, boolean.boolean.NOT):
            # all_docs y term_docs son sets de tuplas (docname, docid)
            all_docs = set(self.indexer.getAllDocsID())
            term_docs = self.analyzeBooleanExpression(expression.args[0])
            return all_docs - term_docs

        elif isinstance(expression, boolean.boolean.AND):
            sets = [self.analyzeBooleanExpression(arg) for arg in expression.args]
            if not sets:
                return set()
            # Intersección basada en tuplas (docname, docid)
            return set.intersection(*sets)

        elif isinstance(expression, boolean.boolean.OR):
            sets = [self.analyzeBooleanExpression(arg) for arg in expression.args]
            return set.union(*sets)

        raise ValueError("Operador desconocido en expresión booleana.")

    def termIsInVocab(self, term: str) -> bool:
        return self.indexer.isTermInVocab(term)

def main():
    parser = argparse.ArgumentParser(description="Indexador y buscador booleano y ponderado")
    parser.add_argument("path", type=str, help="Ruta al directorio con archivos HTML")
    parser.add_argument("docs", type=int, default=250, help="Cantidad de documentos a procesar antes de volcar a disco")
    parser.add_argument("--load", action="store_true", help="Cargar índice desde disco en lugar de indexar de nuevo")
    args = parser.parse_args()

    daat = DaatRetriever(Path(args.path), args.docs, loadIndexFromDisk=args.load, indexer=Indexer(True))

    while True:
        query = input("Ingrese una query: ")
        daat.getQueryRanking(query)

if __name__ == "__main__":
    main()