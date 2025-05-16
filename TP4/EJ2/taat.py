from TP4.EJ1.indexer import Indexer 
from TP4.EJ2.queryProcessor import QueryProcessor
from pathlib import Path
import argparse

class TaatRetriever:
    def __init__(self, path: Path, nDocsToDisc: int):
        self.indexer = Indexer()
        self.queryProcessor = QueryProcessor()
        self.indexer.index_files(path,nDocsToDisc)
        self.indexer.cargar_indice()


    def searchTerm(self, term: str) -> list:
            return self.indexer.searchTerm(term)

def main():
    parser = argparse.ArgumentParser(description="Indexador de documentos")
    parser.add_argument("path", type=str, help="Ruta al directorio con archivos HTML")
    parser.add_argument("docs", type=int, default=250, help="Cantidad de documentos a procesar antes de descargar a disco")
    args = parser.parse_args()

    taat = TaatRetriever(Path(args.path),int(args.docs))


if __name__ == "__main__":
    main()