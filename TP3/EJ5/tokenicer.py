import re
import platform
import heapq
import tempfile
import subprocess
from collections import defaultdict
import nltk
from nltk.corpus import stopwords


class TextProcessor:
    def __init__(self):
        self.token_count = 0
        self.doc_count = 0
        self.json_data = defaultdict(
            lambda: {"palabra": "", "df": 0, "apariciones": {}}
            )
        self.statistics = {"N": 0, "num_terms": 0, "num_tokens": 0}

        nltk.download("stopwords", quiet=True)
        self.stopwords = set(stopwords.words("spanish"))

    def process_text(self, text: str, docID: str):
        sorted_words = self.sort_words(text)
        doc_terms = set()

        i = 0
        while i < len(sorted_words):
            word = sorted_words[i]
            term_count = 1
            while i + 1 < len(sorted_words) and sorted_words[i + 1] == word:
                term_count += 1
                i += 1
            if word not in self.stopwords:
                self.update_json_in_memory(word, docID, term_count, doc_terms)
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

    def update_json_in_memory(self, term, docID, freq, doc_terms):
        term_data = self.json_data[term]
        term_data["palabra"] = term
        term_data["apariciones"][docID] = freq
        if term not in doc_terms:
            term_data["df"] += 1
            doc_terms.add(term)

    def finalizar(self):
        self.statistics["N"] = self.doc_count
        self.statistics["num_terms"] = len(self.json_data)
        self.statistics["num_tokens"] = self.token_count
