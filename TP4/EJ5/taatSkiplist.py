from TP4.EJ5.indexerSkipList import IndexerSkiplist
from TP4.EJ2.queryProcessor import QueryProcessor
from pathlib import Path
import argparse
import math
import time
import boolean


class TaatSkiplistRetriever:
    def __init__(self, path: Path, nDocsToDisc: int, loadIndexFromDisk: bool = False):
        self.indexer = IndexerSkiplist()
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
            postings_list = self.indexer.search(q_term)
            term_results.append((q_term, q_freq, postings_list))

        for term, q_freq, postings_list in term_results:
            df = len(postings_list)
            n_docs = self.indexer.doc_count
            if df == 0:
                continue
            idf = n_docs / df

            for docName, docID, d_freq, skip in postings_list:
                if d_freq <= 0 or q_freq <= 0 or idf <= 1:
                    continue
                weighted_freq = ((1 + math.log(d_freq, 2)) / math.log(idf, 2)) * (
                    1 + math.log(q_freq, 2)
                )
                scores[docID] = scores.get(docID, 0) + weighted_freq

        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_scores

    def getQueryRanking(self, query: str, useSkipLists=False) -> None:
        expression = self.algebra.parse(query, simplify=False)
        docs = self.analyzeBooleanExpression(expression, useSkipLists)
        if useSkipLists:
            stringOut = "CON"
        else:
            stringOut = "SIN"
        print(f"Retrieved documents for '{query}' ({stringOut} SKIPLISTS):")
        self.printResults(docs)
        return docs

    def printResults(self, postingList):
        jump = 1
        print("doc")
        for posting in postingList:
            if posting[2] != 0:
                print(
                    f" --- {posting[0]} : {posting[1]} : {posting[2]} <------------ (salto {jump})"
                )
                jump += 1
            else:
                print(f" --- {posting[0]} : {posting[1]} : {posting[2]}")

    def getTermSkipList(self, term):
        skipList = self.indexer.getSkipList(term)
        for posting in skipList:
            print(f" --- {posting[0]} : {posting[1]}")

    def intersect_with_skips(self, list1, list2):
        result = []
        i1 = i2 = 0

        while i1 < len(list1) and i2 < len(list2):
            docName1, doc1, skip1 = list1[i1]
            docName2, doc2, skip2 = list2[i2]
            if doc1 == doc2:
                result.append((doc1, 1, 0))
                i1 += 1
                i2 += 1
            elif doc1 < doc2:
                if skip1 != 0 and list1[skip1][1] <= doc2:
                    i1 = skip1
                else:
                    i1 += 1
            else:  # doc2 < doc1
                if skip2 != 0 and list2[skip2][1] <= doc1:
                    i2 = skip2
                else:
                    i2 += 1

        return result

    def intersect(self, list1, list2):
        result = []
        i1 = i2 = 0

        while i1 < len(list1) and i2 < len(list2):
            doc1 = list1[i1][1]
            doc2 = list2[i2][1]
            if doc1 == doc2:
                result.append((doc1, 1, 0))
                i1 += 1
                i2 += 1
            elif doc1 < doc2:
                i1 += 1
            else:  # doc2 < doc1
                i2 += 1

        return result

    def analyzeBooleanExpression(self, expression, useSkipLists=False):
        if isinstance(expression, boolean.boolean.Symbol):
            raw_postings = self.indexer.search(
                str(expression)
            )  # [(docID, freq, skip), ...]
            return [
                (docName, docID, skip) for docName, docID, _, skip in raw_postings
            ]  # conservamos docID y skip

        elif isinstance(expression, boolean.boolean.NOT):
            all_docs = set(self.indexer.get_all_doc_ids())  # {docID, ...}
            term_docs = self.analyzeBooleanExpression(expression.args[0], useSkipLists)
            term_doc_ids = {docID for docID, _ in term_docs}

            result_ids = all_docs - term_doc_ids
            return [(docID, 0) for docID in result_ids]

        elif isinstance(expression, boolean.boolean.AND):
            left = expression.args[0]
            right = expression.args[1]

            left_result = self.analyzeBooleanExpression(left, useSkipLists)
            right_result = self.analyzeBooleanExpression(right, useSkipLists)

            if useSkipLists:
                return self.intersect_with_skips(left_result, right_result)
            else:
                return self.intersect(left_result, right_result)

        elif isinstance(expression, boolean.boolean.OR):
            seen = set()
            result = []

            for arg in expression.args:
                for docID, _ in self.analyzeBooleanExpression(arg, useSkipLists):
                    if docID not in seen:
                        seen.add(docID)
                        result.append((docID, 0))

            return result

        raise ValueError(f"Operador desconocido en expresión booleana [{expression}]")

    def termIsInVocab(self, term: str) -> bool:
        return self.indexer.term_in_vocab(term)

    def queriesTest(self, path: Path):
        time_accum_con = 0
        time_accum_sin = 0
        queries = 0

        with open(path, "r") as f:
            query = f.readline().replace("\n", "").strip()
            while query:
                queries += 1

                start1 = time.time()
                # Queries and CON Skiplists
                self.getQueryRanking(query, True)
                time1 = time.time() - start1

                time_accum_con += time1

                start2 = time.time()
                # Queries and SIN Skiplists
                self.getQueryRanking(query, False)
                time2 = time.time() - start2

                time_accum_sin += time2

                print(f"\nTiempo de ejecucion (s) para [{query}]:")
                print(f" -- Con skips: {time1};\n -- Sin skip: {time2}")
                if time1 < time2:
                    print(" ---- Es mejor con skips\n")
                elif time1 > time2:
                    print(" ---- Es mejor sin skips\n")
                else:
                    print("Son iguales\n")
                print("\n\n")
                query = f.readline().replace("\n", "").strip()

        print(f"[ RESUMEN FINAL ]")
        print(
            f"Tiempo promedio de busqueda CON SKIPS: {time_accum_con / queries} \nTiempo promedio de busqueda SIN SKIPS: {time_accum_sin / queries}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Indexador y buscador booleano y ponderado"
    )
    parser.add_argument(
        "path_corpus", type=str, help="Ruta al directorio con archivos HTML"
    )
    parser.add_argument(
        "docs",
        type=int,
        default=250,
        help="Cantidad de documentos a procesar antes de volcar a disco",
    )
    parser.add_argument("path_queries", type=str, help="Ruta al archivo de queries")
    parser.add_argument(
        "--load",
        action="store_true",
        help="Cargar índice desde disco en lugar de indexar de nuevo",
    )
    args = parser.parse_args()

    taat = TaatSkiplistRetriever(
        Path(args.path_corpus), args.docs, loadIndexFromDisk=args.load
    )
    taat.queriesTest(Path(args.path_queries))

    while True:
        print("\n\n")
        query = input("Ingrese un termino para recuperar su skiplist: ")
        taat.getTermSkipList(query)


if __name__ == "__main__":
    main()
