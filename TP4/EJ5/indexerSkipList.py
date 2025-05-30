import time
from bs4 import BeautifulSoup
from pathlib import Path
from collections import defaultdict
import struct
import pickle
import os
from TP4.EJ1.tokenicer import TextProcessor


class IndexerSkiplist:
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
        self.PATH_DOCNAMES = "docnames.bin"
        self.docnames = {}
        self.index = {}
        self.doc_count = 0

    def getAllDocsID(self):
        result = []
        for docId, docname in enumerate(self.docnames):
            result.append((docname, docId))
        return result

    def _add_document(self, doc_id, text, docname):
        term_freqs = self.text_processor.process(text)
        for term, freq in term_freqs.items():
            if term not in self.term_ids:
                term_id = len(self.terms)
                self.term_ids[term] = term_id
                self.terms.append(term)
            else:
                term_id = self.term_ids[term]
            self.json_data[term_id]["postings"][doc_id] = freq

        self.docnames[doc_id] = docname

    def _serialize_chunk(self):
        chunk_file = (
            self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{self.epoch}.bin"
        )
        chunk_file.parent.mkdir(parents=True, exist_ok=True)

        print(f" ----- [Serializando chunk {self.epoch}]")

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
                for tag in soup(["script", "style"]):
                    tag.extract()
                text = soup.get_text()
                self._add_document(str(self.file_index), text, file.stem)
                self.file_index += 1
                self.n_iterations += 1

                if self.n_iterations >= docs_per_chunk:
                    self._serialize_chunk()
                    self.n_iterations = 0

                if (self.file_index % 250) == 0:
                    print(f" --- {self.file_index} documentos analizados.")

        if self.n_iterations > 0:
            self._serialize_chunk()

    def build_vocabulary(self):
        print(f"\nConstruyendo índice a partir de los chunks.\n")

        offset = 0
        vocab = {}
        postings_dict = defaultdict(list)

        start_time = time.time()
        for epoch in range(self.epoch):
            chunk_file = self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{epoch}.bin"
            with open(chunk_file, "rb") as c_file:
                while True:
                    data = c_file.read(12)
                    if not data:
                        break
                    term_id, doc_id, freq = struct.unpack("III", data)
                    postings_dict[term_id].append((doc_id, freq))

        print(f" Tiempo de merge: {time.time() - start_time:.2f} s.")

        total_terms = len(self.terms)
        step = max(1, total_terms // 10)

        with open(self.PATH_VOCAB, "wb") as v_file, open(
            self.PATH_POSTINGS, "wb"
        ) as p_file:
            for term_id, term in enumerate(self.terms):
                postings = postings_dict.get(term_id, [])
                postings.sort()

                df = len(postings)
                skip_interval = int(df**0.5) if df > 0 else 0

                for i, (doc_id, freq) in enumerate(postings):
                    if (
                        skip_interval > 0
                        and i % skip_interval == 0
                        and i + skip_interval < df
                    ):
                        skip_to = i + skip_interval
                    else:
                        skip_to = 0
                    p_file.write(struct.pack("III", doc_id, freq, skip_to))

                vocab[term] = (offset, df)
                offset += df * 12  # 3 * 4 bytes (doc_id, freq, skip_to)

                if (term_id + 1) % step == 0:
                    percent = ((term_id + 1) * 100) // total_terms
                    print(f" --- {percent}% de los términos almacenados.")

            pickle.dump(vocab, v_file)
            print(f"[DEBUG] Guardado vocabulario con {len(vocab)} términos.")

        with open(self.PATH_DOCNAMES, "wb") as d_file:
            pickle.dump(self.docnames, d_file)

    def load_index(self):
        print(f"\nCargando indice desde el archivo a la memora.\n")
        if not os.path.exists(self.PATH_VOCAB):
            print("[ERROR]: Vocabulario no encontrado.")
            return
        with open(self.PATH_VOCAB, "rb") as v_file:
            self.index = pickle.load(v_file)
        if not os.path.exists(self.PATH_DOCNAMES):
            print("[ERROR]: DOCNAMES no encontrados.")
            return
        with open(self.PATH_DOCNAMES, "rb") as d_file:
            self.docnames = pickle.load(d_file)

    def search(self, term):
        if term not in self.index:
            return []
        offset, df = self.index[term]
        postings = []
        with open(self.PATH_POSTINGS, "rb") as p_file:
            p_file.seek(offset)
            for _ in range(df):
                doc_id, freq, skip_to = struct.unpack("III", p_file.read(12))
                postings.append((self.docnames[str(doc_id)], doc_id, freq, skip_to))
        return postings

    def getSkipList(self, term):
        postings = self.search(term)
        result = []
        for doc_name, doc_id, freq, skip_to in postings:
            if skip_to != 0:
                result.append((doc_name, doc_id, freq, skip_to))

        return result


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, help="Directorio de documentos HTML")
    parser.add_argument("docs", type=int, help="Documentos por chunk (serialización)")
    args = parser.parse_args()

    indexer = IndexerSkiplist()
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
