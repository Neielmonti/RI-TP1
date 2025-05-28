from TP4.EJ2.taat import TaatRetriever
from TP4.EJ2.queryProcessor import QueryProcessor
from pathlib import Path
import argparse
import os
import time


class EJ3:
    def __init__(self, corpus_path: Path, queries_path: Path):
        files_n = len(
            [
                f
                for f in os.listdir(corpus_path)
                if os.path.isfile(os.path.join(corpus_path, f))
            ]
        )
        percent = round(files_n * 0.15)

        self.taatRetriever = TaatRetriever(corpus_path, percent, True)
        self.queryProcessor = QueryProcessor()
        self.queries_path = queries_path

        self.q2_count = 0
        self.q2_accum = 0
        self.q3_count = 0
        self.q3_accum = 0

    def processQueries(self):
        with open(self.queries_path, "r") as q_file:
            line = q_file.readline()
            while line:
                query_text = line.split(":")[1]
                terms = [t[0] for t in self.queryProcessor.process_query(query_text)]
                if len(terms) == 2 or len(terms) == 3:
                    self.searchBooleanPattern(terms)
                line = q_file.readline()

        if self.q2_count > 0:
            print(
                f"Tiempo promedio de ejecucion de queries [ 2 TERMS ]: {self.q2_accum / self.q2_count} segundos"
            )
        if self.q3_count > 0:
            print(
                f"Tiempo promedio de ejecucion de queries [ 3 TERMS ]: {self.q3_accum / self.q3_count} segundos"
            )

    def process2Terms(self, terms: list):
        print("\n-[ 2 TERMS ]-----------------------------------------")
        self.q2_count += 1
        start = time.time()
        self.taatRetriever.getQueryRanking(f"{terms[0]} AND {terms[1]}")
        self.taatRetriever.getQueryRanking(f"{terms[0]} OR {terms[1]}")
        self.taatRetriever.getQueryRanking(f"{terms[0]} AND NOT {terms[1]}")
        end = time.time()
        self.q2_accum += end - start
        print(f"\nTiempo de ejecución: {end - start:.4f} segundos\n")

    def process3Terms(self, terms: list):
        print("\n-[ 3 TERMS ]-----------------------------------------")
        self.q3_count += 1
        start = time.time()
        self.taatRetriever.getQueryRanking(f"{terms[0]} AND {terms[1]} AND {terms[2]}")
        self.taatRetriever.getQueryRanking(f"({terms[0]} OR {terms[1]}) AND {terms[2]}")
        self.taatRetriever.getQueryRanking(f"({terms[0]} AND {terms[1]}) OR {terms[2]}")
        end = time.time()
        self.q3_accum += end - start
        print(f"\nTiempo de ejecución: {end - start:.4f} segundos\n")

    def searchBooleanPattern(self, terms: list):
        for term in terms:
            if not self.taatRetriever.termIsInVocab(term):
                return

        if (len(terms)) == 2:
            self.process2Terms(terms)
        elif (len(terms)) == 3:
            self.process3Terms(terms)


def main():
    parser = argparse.ArgumentParser(description="Indexador de documentos")
    parser.add_argument(
        "corpus_path", type=str, help="Ruta al directorio con archivos HTML"
    )
    parser.add_argument("queries_path", type=str, help="Ruta al archivo de queries")
    args = parser.parse_args()

    ej3 = EJ3(Path(args.corpus_path), Path(args.queries_path))
    ej3.processQueries()


if __name__ == "__main__":
    main()
