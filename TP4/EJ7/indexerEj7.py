from bs4 import BeautifulSoup
from pathlib import Path
from collections import defaultdict
import struct
import pickle
import time
import os
from bitarray import bitarray
from TP4.EJ1.tokenicer import TextProcessor


class Indexer:
    def __init__(self, useDgaps=False):
        self.text_processor = TextProcessor()
        self.terms = []
        self.term_ids = {}
        self.json_data = defaultdict(lambda: {"postings": {}})
        self.file_index = 0
        self.n_iterations = 0
        self.epoch = 0
        self.PATH_CHUNKS = Path("chunks") / "chunk"
        self.PATH_VOCAB = "vocabulary.bin"
        self.PATH_POSTINGS_DOCS = "postings_docs.bin"
        self.PATH_POSTINGS_FREQS = "postings_freqs.bin"
        self.PATH_DOCNAMES = "docnames.bin"
        self.docnames = {}
        self.index = {}
        self.doc_count = 0
        self.compression_time = 0
        self.useDgaps = useDgaps

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

        with open(chunk_file, "wb") as f:
            for term_id, data in sorted(self.json_data.items()):
                for doc_id, freq in data["postings"].items():
                    f.write(struct.pack("III", term_id, int(doc_id), freq))

        self.json_data.clear()
        self.epoch += 1

    def index_directory(self, path: Path, docs_per_chunk=0):
        print(f"\nRecorriendo y tokenizando documentos.\n")
        doc_files = list(path.rglob("*.html"))
        self.doc_count = len(doc_files)

        if docs_per_chunk == 0:
            docs_per_chunk = int(self.doc_count * 0.1)

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

        offset_docIds = 0
        offset_freqs = 0
        vocab = {}
        postings_dict = defaultdict(list)

        # Aca leo todos los chunks UNA SOLA VEZ y agrupo por term_id
        # Realmente, esto trae el indice entero a memoria, venciendo el sentido de los chunks,
        # pero por cuestiones de tiempo de ejecucion decidi hacerlo de esta manera.
        # (Caso contrario, hubiera recorrido cada chunk en busqueda de un term_id especifico, guardar su posting list,
        # y pasar al siguiente termino).
        for epoch in range(self.epoch):
            chunk_file = self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{epoch}.bin"
            with open(chunk_file, "rb") as c_file:
                while True:
                    data = c_file.read(12)
                    if not data:
                        break
                    term_id, doc_id, freq = struct.unpack("III", data)
                    postings_dict[term_id].append((doc_id, freq))

        total_terms = len(self.terms)
        step = max(1, total_terms // 10)

        # Escribir postings ordenados y armar vocabulario
        with open(self.PATH_VOCAB, "wb") as v_file, open(
            self.PATH_POSTINGS_DOCS, "wb"
        ) as pd_file, open(self.PATH_POSTINGS_FREQS, "wb") as pf_file:

            time_accum = 0

            for term_id, term in enumerate(self.terms):
                postings = postings_dict.get(term_id, [])
                postings.sort()  # Ordenar por doc_id

                if self.useDgaps:
                    save_list = self.getDGaps(postings)
                else:
                    save_list = postings

                df = len(postings)

                vl = 0
                gamma = 0

                for doc_id, freq in save_list:
                    vl_item, vl_time = self.saveAsVariableLength(doc_id, pd_file)
                    vl += vl_item
                    gamma_item, gamma_time = self.saveAsGamma(freq, pf_file)
                    gamma += gamma_item

                time_accum += vl_time + gamma_time

                vocab[term] = (df, offset_docIds, offset_freqs)

                offset_docIds += vl
                offset_freqs += gamma

                if (term_id + 1) % step == 0:
                    percent = ((term_id + 1) * 100) // total_terms
                    print(f" --- {percent}% de los términos almacenados.")

            self.compression_time = time_accum
            pickle.dump(vocab, v_file)
            print(f"[DEBUG] Guardado vocabulario con {len(vocab)} términos.")

        with open(self.PATH_DOCNAMES, "wb") as d_file:
            pickle.dump(self.docnames, d_file)

    def getDGaps(self, posting_list):
        dgaps = []
        previous = 0
        for docID, freq in posting_list:
            dgaps.append(((docID - previous), freq))
            previous = docID
        return dgaps

    def saveAsVariableLength(self, doc_id: int, file):
        start_time = time.time()
        parts = []

        # Cortamos el número en grupos de 7 bits, desde el final
        while True:
            # Tomar los últimos 7 bits
            part = doc_id & 0b01111111
            parts.insert(0, part)  # Lo agregamos al principio de la lista
            doc_id = (
                doc_id // 128
            )  # Dividimos por 128 (2^7) para pasar al siguiente bloque de 7 bits
            if doc_id == 0:
                break

        # Escribir los bytes en el archivo
        for i in range(len(parts)):
            byte = parts[i]
            # Si es el último byte, le ponemos un 1 adelante
            if i == len(parts) - 1:
                byte |= 0b10000000
            file.write(struct.pack("B", byte))

        return len(parts) * 8, time.time() - start_time

    def saveAsGamma(self, freq: int, file):
        start_time = time.time()

        bin_str = bin(freq)[2:]
        length = len(bin_str)
        unary = "1" * (length - 1) + "0"

        rmsb = bin_str[1:]  # quitar el bit más significativo (siempre 1)
        gamma_code = unary + rmsb

        while len(gamma_code) % 8 != 0:
            gamma_code += "0"

        # Escribir byte a byte usando struct
        for i in range(0, len(gamma_code), 8):
            byte_str = gamma_code[i : i + 8]
            byte = int(byte_str, 2)
            file.write(struct.pack("B", byte))

        return len(gamma_code), time.time() - start_time  # cantidad de bits codificados

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

    def read_varlength_code(self, file, start_bit_offset):
        start_time = time.time()

        bits = self.getBitsFromFile(start_bit_offset, file, 10)
        result = 0
        bit_index = 0

        while True:
            current_bits = bits[bit_index : bit_index + 8]
            bits_del_byte = current_bits.to01()
            valor_del_byte = int(bits_del_byte, 2)

            # Extraer el MSB
            msb = valor_del_byte & 0b10000000

            datos = valor_del_byte & 0b01111111
            result = result * 128 + datos
            bit_index += 8

            if msb != 0:
                break

        return result, bit_index, bits, time.time() - start_time

    def read_gamma_code(self, file, start_bit_offset):
        start_time = time.time()

        bits = self.getBitsFromFile(start_bit_offset, file, 10)
        unary_count = 0
        i = 0

        while bits[i]:
            unary_count += 1
            i += 1
        i += 1  # saltar el 0

        length = unary_count + 1
        binary_rest = bits[i : i + (length - 1)]
        i += length - 1

        # reconstruir el número
        bin_str = "1" + "".join("1" if b else "0" for b in binary_rest)
        value = int(bin_str, 2)

        return value, i, bits, time.time() - start_time

    def getBitsFromFile(self, bits_offset, file, safetyBytes):
        bytes_offset = bits_offset // 8
        aux_bits_offset = bits_offset % 8

        file.seek(bytes_offset)
        data = file.read(safetyBytes)  # leer suficientes bytes

        bits = bitarray()
        bits.frombytes(data)
        bits = bits[aux_bits_offset:]

        return bits

    def search(self, term):
        if term not in self.index:
            return []

        df, offset_docIds_bits, offset_freqs_bits = self.index[term]

        postings = []

        time_accum = 0

        with open(self.PATH_POSTINGS_DOCS, "rb") as pd_file, open(
            self.PATH_POSTINGS_FREQS, "rb"
        ) as pf_file, open(self.PATH_DOCNAMES, "rb") as d_file:
            bit_offset_doc = offset_docIds_bits
            bit_offset_freq = offset_freqs_bits

            bits_docs = bitarray()
            bits_freqs = bitarray()

            d_file.seek(0)
            docnames = pickle.load(d_file)

            for i in range(df):
                doc_gap, bits_read_doc, bits_doc, doc_time = self.read_varlength_code(
                    pd_file, bit_offset_doc
                )
                bit_offset_doc += bits_read_doc
                bits_docs.extend(bits_doc)

                freq, bits_read_freq, bits_freq, freq_time = self.read_gamma_code(
                    pf_file, bit_offset_freq
                )
                bit_offset_freq += bits_read_freq
                bits_freqs.extend(bits_freq)

                # Acumular docIDs a partir de gaps (si es que se usaron Dgaps)
                if self.useDgaps and postings:
                    doc_id = postings[i - 1][1] + doc_gap
                else:
                    doc_id = doc_gap

                # obtener nombre documento
                doc_name = docnames.get(str(doc_id), None)
                postings.append((doc_name, doc_id, freq))
                time_accum += doc_time + freq_time

        return postings, bits_docs, bits_freqs, time_accum / df

    def printTermPostingList(self, term):
        print("\n")
        if term not in self.index:
            print("El termino no forma parte del vocabulario")
            return
        print(f"Postings del termino '{term}'\n")
        postings, bits_docs, bits_freqs, _ = self.search(term)
        print(
            f"Bits de la lista de docs: \n{bits_docs.to01()} \n\nBits de la lista de frecuencias: \n{bits_freqs.to01()} \n\nPostings descomprimidas:\n"
        )
        for posting in postings:
            print(f" -- {posting[0]}:{posting[1]}:{posting[2]}")

    def isTermInVocab(self, term):
        return term in self.index

    def getIndexDecompressionTime(self):
        time_accum = 0
        for term in list(self.index.keys()):
            _, _, _, d_time = self.search(term)
            time_accum += d_time
        return time_accum

    def showCompressAndDecompressTimes(self):
        print(
            f"\nTiempo de compresion del indice: {self.compression_time} s.\nTiempo de descompresion del indice: {self.getIndexDecompressionTime()} s.\n\n"
        )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, help="Directorio de documentos HTML")
    parser.add_argument("docs", type=int, help="Documentos por chunk (serialización)")
    parser.add_argument("--dgaps", action="store_true", help="Usar D-Gaps")
    args = parser.parse_args()

    indexer = Indexer(args.dgaps)

    indexer.index_directory(Path(args.path), args.docs)
    indexer.build_vocabulary()
    indexer.load_index()

    indexer.showCompressAndDecompressTimes()

    while True:
        query = input("\nBuscar término (enter para salir): ").strip().lower()
        if not query:
            break
        indexer.printTermPostingList(query)


if __name__ == "__main__":
    main()
