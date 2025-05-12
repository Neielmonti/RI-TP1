import re
import platform
import heapq
import tempfile
import subprocess
from collections import defaultdict
import nltk
from nltk.corpus import stopwords
import struct
import pprint
import heapq
from collections import defaultdict

class TextProcessor:
    def __init__(self):
        self.token_count = 0
        self.doc_count = 0

        nltk.download("stopwords")
        self.stopwords = set(stopwords.words("english"))

        self.json_data = defaultdict(
            lambda: {"postings": {}}
            )
        self.epoch = 0
        self.epochs = []
        self.PATH_VOCAB = "vocabs/vocab"
        self.PATH_POSTINGS = "postings/postings"

        self.terms = []

        nltk.download("stopwords", quiet=True)
        self.stopwords = set(stopwords.words("spanish"))

    def process_text(self, text: str, docID: str):
        sorted_words = self.sort_words(text)

        i = 0
        while i < len(sorted_words):
            word = sorted_words[i]
            if word in self.stopwords or len(word) <= 3:
                i += 1
                continue
            term_count = 1
            while i + 1 < len(sorted_words) and sorted_words[i + 1] == word:
                term_count += 1
                i += 1
            if word not in self.stopwords:
                self.update_json_in_memory(word, docID, term_count)
            i += 1

        self.doc_count += 1

    def sort_words(self, text):
        if platform.system() == "Windows":
            return self.sort_words_windows(text)
        else:
            return self.sort_words_unix(text)

    def sort_words_unix(self, text):
        with tempfile.NamedTemporaryFile(
            mode="w+",
            delete=False,
            encoding="utf-8"
        ) as tmp:
            tmp.write(text)
            tmp.flush()
            command = (
                f"cat {tmp.name} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\n' "
                f"| sed 's/[^a-záéíóúñü]/ /g' | tr -s ' ' '\n' | sed '/^$/d' | sort"
            )
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True)
            words = result.stdout.strip().split('\n')
            self.token_count += len(words)
            return words

    def sort_words_windows(self, text):
        heap = []
        for line in text.splitlines():
            line = re.sub(r'[^a-záéíóúñü\s]', ' ', line.lower())
            for word in line.split():
                heapq.heappush(heap, word)
        words = []
        while heap:
            words.append(heapq.heappop(heap))
        self.token_count += len(words)
        return words

    def update_json_in_memory(self, term, docID, freq):
        try:
            id = self.terms.index(term)
        except ValueError:
            id = len(self.terms)
            self.terms.append(term)

        term_data = self.json_data[id]
        term_data["postings"][docID] = freq

    def serializar(self):
        offset = 0
        postings_file = self.PATH_POSTINGS + str(self.epoch) + ".bin"
        vocab_file = self.PATH_VOCAB + str(self.epoch) + ".bin"

        print(f"analizando epoca {self.epoch}: ")
        pprint.pprint(self.json_data)

        with open(postings_file, "wb") as p_file, open(vocab_file, "wb") as v_file:
            for term, data in sorted(self.json_data.items()):
                postings = data["postings"].items()
                df = len(data["postings"])
                v_file.write(f"{term} {df} {offset} {len(postings)}\n".encode("utf-8"))
                for docID, freq in postings:
                    p_file.write(struct.pack("II", int(docID), freq))
                offset += len(postings)
        
        self.json_data.clear()
        self.json_data = defaultdict(
            lambda: {"postings": {}}
            )
        self.epoch += 1

    def merge_postings(self):
        partial_results = self.get_partial_results()
        merged_index = defaultdict(lambda: {"df": 0, "postings": {}})
        min_heap = []

        # En este contexto, llamo rp a los resultados parciales (individualmente)
        for i, rp in enumerate(partial_results):
            if rp:  # Verificamos que el RP no esté vacío
                first_termID = min(rp.keys())
                postings_data = rp.pop(first_termID)["postings"]

                # Convertimos a lista para el heap
                postings_list = postings_data if isinstance(postings_data, list) else list(postings_data.items())
                heapq.heappush(min_heap, (first_termID, postings_list, i))

        # Mantenemos el heap hasta que no queden rp's por agregar
        while min_heap:
            current_termID, postings_list, index = heapq.heappop(min_heap)

            # Agregamos los postings al índice final
            for docID, freq in postings_list:
                merged_index[current_termID]["postings"][docID] = (
                    merged_index[current_termID]["postings"].get(docID, 0) + freq
                )

            # Actualizamos el df
            merged_index[current_termID]["df"] = len(merged_index[current_termID]["postings"])

            # Si el rp actual todavia tiene términos, agrego el siguiente al heap
            if partial_results[index]:
                next_termID = min(partial_results[index].keys())
                next_postings_data = partial_results[index].pop(next_termID)["postings"]
                next_postings_list = next_postings_data if isinstance(next_postings_data, list) else list(next_postings_data.items())
                heapq.heappush(min_heap, (next_termID, next_postings_list, index))

        return merged_index
    

    def get_partial_results(self):
        partial_results = []
        print(f"Epoch {self.epoch}")
        for i in range(self.epoch - 1):
            partial_result = {}
            postings_file = self.PATH_POSTINGS + str(i) + ".bin"
            vocab_file = self.PATH_VOCAB + str(i) + ".bin"
            
            with open(vocab_file, "r", encoding="utf-8") as v_file:
                with open(postings_file, "rb") as p_file:
                    for line in v_file:
                        termID, df, offset, length = line.strip().split()
                        termID = int(termID)
                        df = int(df)
                        offset = int(offset)
                        length = int(length)
                        p_file.seek(offset * 8)
                        postings = [struct.unpack("II", p_file.read(8)) for _ in range(length)]
                        partial_result[termID] = {"df": df, "postings": postings}
            
            partial_results.append(partial_result)
        return partial_results


    def cargar_indice(self):
        index = {}

        for i in range(0, self.epoch):
            postings_file = self.PATH_POSTINGS + str(self.epoch) + ".bin"
            vocab_file = self.PATH_VOCAB + str(self.epoch) + ".bin"
            
            with open(vocab_file, "r", encoding="utf-8") as v_file:
                with open(postings_file, "rb") as p_file:
                    for line in v_file:
                        term, df, offset, length = line.strip().split()
                        df = int(df)
                        offset = int(offset)
                        length = int(length)
                        p_file.seek(offset * 8)
                        postings = [struct.unpack("II", p_file.read(8)) for _ in range(length)]
                        index[term] = {"df": df, "postings": postings}

        print("LISTA DE TERMINOS")
        print(len(self.terms))
        return index