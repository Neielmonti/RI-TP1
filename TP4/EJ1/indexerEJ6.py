from TP4.EJ1.indexer import Indexer
from collections import defaultdict
from pathlib import Path
import struct
import pickle


class IndexerEJ6(Indexer):
    def index_directory(self, path: Path, docs_per_chunk=250):

        docnames = {}

        postings_dict = defaultdict(list)
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip().rstrip(",")  # quita salto de línea y coma final
                if not line:
                    continue
                term, df_str, doc_ids_str = line.split(":", 2)
                doc_ids = list(map(int, filter(None, doc_ids_str.split(","))))
                postings_dict[term] = (int(df_str), doc_ids)
                for doc_id in doc_ids:
                    docnames[doc_id] = doc_ids

        vocab = {}
        total_terms = len(self.terms)
        step = max(1, total_terms // 10)

        with open(self.PATH_VOCAB, "wb") as v_file, open(self.PATH_POSTINGS, "wb") as p_file:
            for term_id, term in enumerate(self.terms):
                postings = postings_dict.get(term_id, [])
                postings.sort()  # Ordenar por doc_id

                df = len(postings)
                for doc_id, freq in postings:
                    p_file.write(struct.pack("II", doc_id, freq))
                vocab[term] = (offset, df)
                offset += df * 8

                if (term_id + 1) % step == 0:
                    percent = ((term_id + 1) * 100) // total_terms
                    print(f" --- {percent}% de los términos almacenados.")

            pickle.dump(vocab, v_file)
            print(f"[DEBUG] Guardado vocabulario con {len(vocab)} términos.")
        
        with open(self.PATH_DOCNAMES, "wb") as d_file:
            self.docnames = dict(sorted(docnames.items(), key=lambda item: item[0], reverse=True))
            print(self.docnames)
            pickle.dump(self.docnames, d_file)