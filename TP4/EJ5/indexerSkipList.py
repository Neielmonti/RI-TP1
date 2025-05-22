from bs4 import BeautifulSoup
from pathlib import Path
from collections import defaultdict
import struct
import pickle
import os
import math
from TP4.EJ1.tokenicer import TextProcessor

# Agregue skip lists a su índice del ejercicio 1 y ejecute un conjunto de consultas AND sobre el índice original y 
# luego usando los punteros. Compare los tiempos de ejecución con los del ejercicio 3. 
# 5.1) Agregue un script que permita recuperar las skips list para un término dado. En este caso la salida 
# deberá ser una lista de docName:docID ordenada por docName.

class Indexer:
    def __init__(self):
        self.text_processor = TextProcessor()
        self.terms = []
        self.term_ids = {}
        self.json_data = defaultdict(lambda: {"postings": {}})
        self.file_index = 0
        self.n_iterations = 0
        self.epoch = 0
        self.PATH_CHUNKS = Path("chunks") / "chunk"
        self.PATH_VOCAB = "vocabulary.bin"
        self.PATH_POSTINGS = "postings.bin"
        self.PATH_SKIPLISTS = "skiplists.bin"
        self.index = {}
        self.doc_count = 0

    def _add_document(self, doc_id, text):
        term_freqs = self.text_processor.process(text)
        for term, freq in term_freqs.items():
            if term not in self.term_ids:
                term_id = len(self.terms)
                self.term_ids[term] = term_id
                self.terms.append(term)
            else:
                term_id = self.term_ids[term]
            self.json_data[term_id]["postings"][doc_id] = freq

    def _serialize_chunk(self):
        chunk_file = self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{self.epoch}.bin"
        chunk_file.parent.mkdir(parents=True, exist_ok=True)

        with open(chunk_file, "wb") as f:
            for term_id, data in sorted(self.json_data.items()):
                for doc_id, freq in data["postings"].items():
                    f.write(struct.pack("III", term_id, int(doc_id), freq))

        self.json_data.clear()
        self.epoch += 1

    def index_directory(self, path: Path, docs_per_chunk=250):
        doc_files = list(path.rglob("*.html"))
        self.doc_count = len(doc_files)

        for file in path.rglob("*"):
            if file.is_file():
                with open(file, encoding="utf-8") as f:
                    html = f.read()
                soup = BeautifulSoup(html, "html.parser")
                for tag in soup(["script", "style"]): tag.extract()
                text = soup.get_text()
                self._add_document(str(self.file_index), text)
                self.file_index += 1
                self.n_iterations += 1

                if self.n_iterations >= docs_per_chunk:
                    self._serialize_chunk()
                    self.n_iterations = 0

        if self.n_iterations > 0:
            self._serialize_chunk()

    def build_vocabulary(self):
        offset = 0
        offset_skiplist = 0
        vocab = {}

        with open(self.PATH_VOCAB, "wb") as v_file, open(self.PATH_POSTINGS, "wb") as p_file, open(self.PATH_SKIPLISTS, "wb") as s_file:
            for term_id, term in enumerate(self.terms):
                postings = []
                for epoch in range(self.epoch):
                    chunk_file = self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{epoch}.bin"
                    with open(chunk_file, "rb") as c_file:
                        while True:
                            data = c_file.read(12)
                            if not data:
                                break
                            t_id, doc_id, freq = struct.unpack("III", data)
                            if t_id == term_id:
                                postings.append((doc_id, freq))

                postings.sort()
                df = len(postings)

                x = int(math.sqrt(len(postings)))
                skip_list = postings[::x]

                # Formar la skiplist
                for i, (doc_id, freq) in enumerate(skip_list):
                    s_file.write(struct.pack("II", doc_id, i * x))
                offset_skiplist += len(skip_list) * 8

                # Formar las postings
                for doc_id, freq in postings:
                    p_file.write(struct.pack("II", doc_id, freq))
                vocab[term] = (offset, df, offset_skiplist)
                offset += df * 8

            pickle.dump(vocab, v_file)

    def load_index(self):
        if not os.path.exists(self.PATH_VOCAB):
            print("[ERROR]: Vocabulario no encontrado.")
            return
        with open(self.PATH_VOCAB, "rb") as v_file:
            self.index = pickle.load(v_file)


    def search(self, term):
        result = {"postings":[], "skip_list":[]}

        if term not in self.index:
            return result
        
        offset, df, offset_skiplist = self.index[term]

        with open(self.PATH_POSTINGS, "rb") as p_file, open(self.PATH_SKIPLISTS, "wb") as s_file:
            p_file.seek(offset)
            for _ in range(df):
                doc_id, freq = struct.unpack("II", p_file.read(8))
                result["postings"].append((doc_id, freq))

            s_file.seek(offset_skiplist)
            x = int(math.sqrt(df))
            skiplist_len = (df - 1) // x + 1
            for _ in range(skiplist_len):
                doc_id, skip_pointer = struct.unpack("II", s_file.read(8))
                result["skip_list"].append((doc_id, freq))
        return result


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, help="Directorio de documentos HTML")
    parser.add_argument("docs", type=int, help="Documentos por chunk (serialización)")
    args = parser.parse_args()

    indexer = Indexer()
    indexer.index_directory(Path(args.path), args.docs)
    indexer.build_vocabulary()
    indexer.load_index()

    while True:
        query = input("\nBuscar término (enter para salir): ").strip().lower()
        if not query:
            break
        postings = indexer.search(query)
        if postings:
            print(f"Documentos que contienen '{query}': {postings}")
        else:
            print(f"Término '{query}' no encontrado.")


if __name__ == "__main__":
    main()