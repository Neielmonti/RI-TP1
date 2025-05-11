import re
import platform
import heapq
import tempfile
import subprocess
from collections import defaultdict
import nltk
from nltk.corpus import stopwords
import struct
import nltk
from nltk.corpus import stopwords


class TextProcessor:
    def __init__(self):
        self.token_count = 0
        self.doc_count = 0

        nltk.download("stopwords")
        self.stopwords = set(stopwords.words("english"))

        self.json_data = defaultdict(
            lambda: {"termID": "", "df": 0, "postings": {}}
            )
        self.epoch = 0
        self.epochs = []
        self.PATH_VOCAB = "vocab"
        self.PATH_POSTINGS = "postings"

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
        term_data["termId"] = id
        term_data["df"] += 1
        term_data["postings"][docID] = freq

    def serializar(self):
        offset = 0
        postings_file = self.PATH_POSTINGS + str(self.epoch) + ".bin"
        vocab_file = self.PATH_VOCAB + str(self.epoch) + ".bin"

        with open(postings_file, "wb") as p_file, open(vocab_file, "wb") as v_file:
            for term, data in sorted(self.json_data.items()):
                postings = data["postings"].items()
                df = data["df"]
                v_file.write(f"{term} {df} {offset} {len(postings)}\n".encode("utf-8"))
                for docID, freq in postings:
                    p_file.write(struct.pack("II", int(docID), freq))
                offset += len(postings)
        
        self.epoch += 1


    def cargar_indice(self, epoch):
        index = {}
        postings_file = self.PATH_POSTINGS + str(epoch) + ".bin"
        vocab_file = self.PATH_VOCAB + str(epoch) + ".bin"
        
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
        return index