import random
import time
import csv
from TP4.EJ6.indexerEJ6 import IndexerEJ6
from TP4.EJ2.taat import TaatRetriever
from TP4.EJ4.daat import DaatRetriever
from pathlib import Path
import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Indexador y buscador booleano y ponderado"
    )
    parser.add_argument("corpus_path", type=str, help="Ruta al archivo corpus")
    parser.add_argument("queries_path", type=str, help="Ruta al archivo de queries")
    parser.add_argument(
        "--load",
        action="store_true",
        help="Cargar índice desde disco en lugar de indexar de nuevo",
    )
    args = parser.parse_args()

    indexer = IndexerEJ6(True)
    print("\nIniciando TAAT Retriever...")
    taat = TaatRetriever(Path(args.corpus_path), 0, False, indexer=indexer)
    print("\nIniciando DAAT Retriever...")
    daat = DaatRetriever(Path(args.corpus_path), 0, True, indexer=indexer)

    print(
        "\n\n\nInicializacion completa. Realizando pruebas con queries aleatorias...\n\n"
    )
    taat_accum = 0
    daat_accum = 0

    queries = []
    with open(Path(args.queries_path), "r") as f:
        query = f.readline()
        while query:
            queries.append(query)
            query = f.readline()

    cycles = 20
    data = []

    for i in range(cycles):
        query = queries[random.randint(0, len(queries) - 1)]

        t_start = time.time()
        t_results = taat.searchQuery(query)
        t_time = time.time() - t_start

        d_start = time.time()
        d_results = daat.searchQuery(query)
        d_time = time.time() - d_start

        lenght = round((len(t_results) + len(d_results)) / 2)

        print(
            f"\n\n\n[ QUERY {query.strip()}, Q_COUNT: {i+1} ]\nQuery lenght: {len(query)}, Postings lenght: {lenght}\nTAAT_time: {t_time}, DAAT_time: {d_time}"
        )
        data.append((len(query), lenght, t_time, d_time))
        taat_accum += t_time
        daat_accum += d_time

    print(
        f"[RESULTADOS FINALES] TAAT_mean_time: {taat_accum/cycles}, DAAT_mean_time: {daat_accum/cycles}"
    )

    min_ql = min(data, key=lambda x: x[0])[0]
    max_ql = max(data, key=lambda x: x[0])[0]
    min_pa = min(data, key=lambda x: x[1])[1]
    max_pa = max(data, key=lambda x: x[1])[1]

    new_data = []
    for row in data:
        n0 = (row[0] - min_ql) / (max_ql - min_ql)
        n1 = (row[1] - min_pa) / (max_pa - min_pa)
        prod = n0 * n1
        new_data.append((n0, n1, prod, row[2], row[3]))

    new_data.sort(key=lambda x: x[2])

    with open("data.csv", "w", newline="\n") as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=" ", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        for row in new_data:
            spamwriter.writerow(row)


if __name__ == "__main__":
    main()
