import os
from TP4.EJ1.indexer import Indexer
from collections import defaultdict
from pathlib import Path
import struct
import pickle


class IndexerEJ6(Indexer):
    def index_directory(self, path: Path, docs_per_chunk=250):

        docnames = {}

        postings_dict = defaultdict(list)

        doc_count = 0

        with open(path, encoding="utf-8") as f:
            i = 0
            for line in f:

                line = line.strip().rstrip(",")  # quita salto de línea y coma final
                if not line:
                    continue

                term, df_str, doc_ids_str = line.split(":", 2)

                doc_ids = list(map(int, filter(None, doc_ids_str.split(","))))

                postings_dict[term] = (int(df_str), doc_ids)
                self.terms.append(term)

                for doc_id in doc_ids:
                    docnames[doc_id] = doc_ids

                if (i % 500) == 0:
                    print(f" --- {i} documentos analizados.")
                i += 1

        self.doc_count = len(docnames)
        vocab = {}
        total_terms = len(self.terms)
        step = max(1, total_terms // 10)

        with open(self.PATH_VOCAB, "wb") as v_file, open(
            self.PATH_POSTINGS, "wb"
        ) as p_file:
            offset = 0
            for term_id, term in enumerate(self.terms):

                df, postings = postings_dict[term]

                postings.sort()  # Ordenar por doc_id

                for doc_id in postings:
                    p_file.write(struct.pack("II", doc_id, 1))  # Asumo freq = 1
                vocab[term] = (offset, df)
                offset += df * 8

                if (term_id + 1) % step == 0:
                    percent = ((term_id + 1) * 100) // total_terms
                    print(f" --- {percent}% de los términos almacenados.")

            pickle.dump(vocab, v_file)
            print(f"[DEBUG] Guardado vocabulario con {len(vocab)} términos.")

        self.docnames = docnames
        self.doc_count = len(docnames)
        if self.doc_count == 0:
            self.doc_count = 1

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
        if self.saveNorms:
            self.doc_count = len(self.docnames)
            self.saveDocNorms()

    def build_vocabulary(self):
        pass

    def search(self, term):
        if term not in self.index:
            return []
        offset, df = self.index[term]
        postings = []
        with open(self.PATH_POSTINGS, "rb") as p_file, open(
            self.PATH_DOCNAMES, "rb"
        ) as d_file:
            p_file.seek(offset)
            for _ in range(df):
                doc_id, freq = struct.unpack("II", p_file.read(8))
                postings.append((str(doc_id), doc_id, freq))
        return postings
