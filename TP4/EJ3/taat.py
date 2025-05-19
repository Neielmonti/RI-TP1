from TP4.EJ1.indexer import Indexer 
from TP4.EJ2.queryProcessor import QueryProcessor
from pathlib import Path
import argparse
import math

class TaatRetriever:
    def __init__(self, path: Path, nDocsToDisc: int):
        self.indexer = Indexer()
        self.queryProcessor = QueryProcessor()
        self.indexer.index_files(path,nDocsToDisc)
        self.indexer.cargar_indice()

    def searchTerm(self, term: str) -> list:
        return self.indexer.searchTerm(term)
    
    def searchQuery(self, query: str) -> list:
        query_terms = self.queryProcessor.process_query(query)
        scores = {}
        term_results = []
        for q_term, q_freq in query_terms:
            postings_list = self.indexer.searchTerm(q_term)
            term_results.append((q_term, q_freq, postings_list))

        for term, q_freq, postings_list in term_results:

            df = len(postings_list)
            n_docs = self.indexer.file_index
            idf = n_docs / df

            for docID, d_freq in postings_list:
                weighted_freq = ((1 + math.log(d_freq, 2)) / math.log(idf, 2)) * (1 + math.log(q_freq, 2))
                if docID in scores:
                    scores[docID] += weighted_freq
                else:
                    scores[docID] = weighted_freq
        
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_scores
    

def main():
    parser = argparse.ArgumentParser(description="Indexador de documentos")
    parser.add_argument("path", type=str, help="Ruta al directorio con archivos HTML")
    parser.add_argument("docs", type=int, default=250, help="Cantidad de documentos a procesar antes de descargar a disco")
    args = parser.parse_args()

    taat = TaatRetriever(Path(args.path),int(args.docs))


if __name__ == "__main__":
    main()