from bs4 import BeautifulSoup
from pathlib import Path
from tokenicer import TextProcessor
import os
import argparse
import pprint

class Indexer:
    def __init__(self):
            self.n_iterations = 0
            self.file_index = 0
            self.textProcessor = TextProcessor() 

    def directory_dfs(self, path: Path, textProcessor: TextProcessor, N_docs_to_disc: int) -> None:
        for x in path.iterdir():
            if x.is_dir():
                self.directory_dfs(x, textProcessor, N_docs_to_disc)
            else:
                if (self.file_index % 250) == 0:
                    print(f"Procesando archivo: {self.file_index}")
                with open(x, "r", encoding="utf-8") as f:
                    html = f.read()
                soup = BeautifulSoup(html, features="html.parser")
                for script in soup(["script", "style"]):
                    script.extract()
                text = soup.get_text()

                textProcessor.process_text(text, str(self.file_index))
                self.file_index += 1
                self.n_iterations += 1

                if self.n_iterations >= N_docs_to_disc:
                    textProcessor.serializar()
                    self.n_iterations = 0

        if self.n_iterations > 0:
            textProcessor.serializar()
            self.n_iterations = 0

    
    def index_files(self, path: Path, N_docs_to_disc=1) -> None:
        #cantidad_archivos = sum([len(files) for _, _, files in os.walk(path)])
        self.directory_dfs(path, self.textProcessor, N_docs_to_disc)
        self.textProcessor.serializar()

    def cargar_indice(self):
        self.textProcessor.cargar_indice()


def main():
    parser = argparse.ArgumentParser(description="Indexador de documentos")
    parser.add_argument("path", type=str, help="Ruta al directorio con archivos HTML")
    parser.add_argument("docs", type=int, default=250, help="Cantidad de documentos a procesar antes de descargar a disco")
    args = parser.parse_args()

    indexer = Indexer()
    indexer.index_files(Path(args.path))
    indexer.cargar_indice()


if __name__ == "__main__":
    main()