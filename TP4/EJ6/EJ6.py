
import time
from TP4.EJ6.indexerEJ6 import IndexerEJ6
from TP4.EJ2.taat import TaatRetriever
from TP4.EJ4.daat import DaatRetriever
from pathlib import Path
import argparse

def main():
    parser = argparse.ArgumentParser(description="Indexador y buscador booleano y ponderado")
    parser.add_argument("corpus_path", type=str, help="Ruta al archivo corpus")
    parser.add_argument("queries_path", type=str, help="Ruta al archivo de queries")
    parser.add_argument("--load", action="store_true", help="Cargar índice desde disco en lugar de indexar de nuevo")
    args = parser.parse_args()

    indexer = IndexerEJ6()
    taat = TaatRetriever(Path(args.corpus_path), 0, True, indexer=indexer)
    daat = DaatRetriever(Path(args.corpus_path), 0, True, indexer=indexer)

    taat_accum = 0
    daat_accum = 0
    count = 0

    with open(Path(args.queries_path), 'r') as f:
        count = 0
        query = f.readline()
        while query:
            count += 1
            t_start = time.time()
            t_results = taat.searchQuery(query)
            t_time = time.time() - t_start

            d_start = time.time()
            d_results = daat.searchQuery(query)
            d_time = time.time() - d_start

            print(f"[ QUERY 1 ] TAAT_time: {t_start}, DAAT_time: {d_time}")
            taat_accum += t_start
            daat_accum += d_time
            query = f.readline()

        print(f"[RESULTADOS FINALES] TAAT_mean_time: {taat_accum/count}, DAAT_mean_time: {daat_accum/count}")


if __name__ == "__main__":
    main()