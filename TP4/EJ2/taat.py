from TP4.EJ1.indexer import Indexer
from TP4.EJ2.queryProcessor import QueryProcessor
from pathlib import Path
import argparse
import math
import boolean

class TaatRetriever:
    def __init__(self, path: Path, nDocsToDisc: int, loadIndexFromDisk: bool = False, indexer = Indexer()):
        self.indexer = indexer
        self.queryProcessor = QueryProcessor()

        if not loadIndexFromDisk:
            self.indexer.index_directory(path, nDocsToDisc)
            self.indexer.build_vocabulary()
        self.indexer.load_index()

        self.algebra = boolean.BooleanAlgebra()

    def searchTerm(self, term: str) -> list:
        return self.indexer.search(term)

    def searchQuery(self, query: str) -> list:
        query_terms = self.queryProcessor.process_query(query)
        scores = {}
        term_results = []

        for q_term, q_freq in query_terms:
            postings_list = self.indexer.search(q_term)  # cada elemento: (docName, docID, freq)
            term_results.append((q_term, q_freq, postings_list))

        for term, q_freq, postings_list in term_results:
            df = len(postings_list)
            n_docs = self.indexer.doc_count
            if df == 0:
                continue
            idf = n_docs / df

            for docName, docID, d_freq in postings_list:
                if d_freq <= 0 or q_freq <= 0 or idf <= 0:
                    continue
                weighted_freq = ((1 + math.log(d_freq, 2)) * math.log(idf, 2)) * (1 + math.log(q_freq, 2))

                if docID in scores:
                    scores[docID] = (scores[docID][0] + weighted_freq, docName)
                else:
                    scores[docID] = (weighted_freq, docName)

        # Ordenar por score descendente
        sorted_scores = sorted(scores.items(), key=lambda x: x[1][0], reverse=True)

        # Armar salida como lista de tuplas: (docID, score, docName)
        return [(docName, docID, score) for docID, (score, docName) in sorted_scores]
    

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

    taat = TaatRetriever(Path(args.path), args.docs, loadIndexFromDisk=args.load)

    while True:
        query = input("Ingrese una query: ")
        taat.getQueryRanking(query)
        """
        query = input("Ingrese una query (booleana o normal): ")
        if any(op in query.upper() for op in ["AND", "OR", "NOT"]):
            taat.getQueryRanking(query)
        else:
            results = taat.searchQuery(query)
            print(f"Top resultados para '{query}':")
            for doc_id, score in results[:10]:
                print(f"Doc: {doc_id} - Score: {score:.4f}")
        """

if __name__ == "__main__":
    main()