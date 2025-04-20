import os
import json
import re
import platform
import heapq
import subprocess
import tempfile
from collections import defaultdict
import orjson  # Reemplazamos json por orjson para mejor rendimiento
import nltk
from nltk.corpus import stopwords



class TextProcessor:
    def __init__(self):
        self.json_file = "palabras.json"
        self.sorting_file = "ordenado.txt"
        self.token_count = 0
        self.doc_count = 0
        self.json_data = defaultdict(lambda: {"palabra": "", "df": 0, "apariciones": {}})
        self.statistics = {"N": 0, "num_terms": 0, "num_tokens": 0}

        nltk.download("stopwords")  # solo la primera vez
        self.stopwords = set(stopwords.words("spanish"))

    def process_text(self, text: str, docID: str):
        self.sort_words(text, self.sorting_file)
        doc_terms = set()

        with open(self.sorting_file, "r", encoding="utf-8") as f:
            word1 = self.readlinePlus(f)
            while word1:
                term_count = 1
                word2 = self.readlinePlus(f)

                while word1 == word2 and word1:
                    term_count += 1
                    word2 = self.readlinePlus(f)

                if word1 not in self.stopwords:
                    self.update_json_in_memory(word1, docID, term_count, doc_terms)
                word1 = word2

        self.doc_count += 1

    def saveData(self) -> None:
        # Guardamos la información usando orjson para mayor velocidad
        self.save_json()
        self.save_json_statistics()

    def readlinePlus(self, file) -> str:
        aux = file.readline().strip()
        if aux != "":
            self.token_count += 1
        return aux

    def sort_words(self, text, output_file):
            if platform.system() == "Windows":
                self.sort_words_windows(text, output_file)
            else:
                self.sort_words_unix(text, output_file)
    """
    def sort_words_unix(self, text, output_file):
        words = re.sub(r'[^a-z\s]', ' ', text.lower()).split()
        words.sort()
        with open(output_file, "w", encoding="utf-8") as f:
            for word in words:
                f.write(f"{word}\n")
    """
    def sort_words_unix(self, text, output_file):
        # Escribimos el string temporalmente para poder usar comandos Unix sobre él
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8") as tmp:
            tmp.write(text)
            tmp.flush()  # Aseguramos que se guarde antes de leer

            command = (
                f"cat {tmp.name} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\n' "
                f"| sed 's/[^a-z]/ /g' | tr -s ' ' '\n' | sed '/^$/d' | sort > {output_file}"
            )
            subprocess.run(command, shell=True, check=True)
    def sort_words_windows(self, text, output_file):
        heap = []

        # Procesar directamente el texto
        for line in text.splitlines():
            line = re.sub(r'[^a-z\s]', ' ', line.lower())
            for word in line.split():
                heapq.heappush(heap, word)

        with open(output_file, 'w', encoding='utf-8') as output:
            while heap:
                output.write(heapq.heappop(heap) + '\n')

    def update_json_in_memory(self, term, docID, freq, doc_terms):
        term_data = self.json_data[term]
        term_data["palabra"] = term
        term_data["apariciones"][docID] = freq
        if term not in doc_terms:
            term_data["df"] += 1
            doc_terms.add(term)

    def save_json_statistics(self):
        # Esta funcion calcula las estadisticas (o mejor dicho las guarda)
        # en el JSON, a partir de atributos del objeto, (a excepcion del
        # term_count, ya que este lo extrae del len del JSON)
        self.statistics["N"] = self.doc_count
        self.statistics["num_terms"] = len(self.json_data)
        self.statistics["num_tokens"] = self.token_count

        self.save_json()

    def load_json(self):
        try:
            with open(self.json_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_json(self):
        # Guardamos los datos en formato JSON usando orjson para mejor rendimiento
        with open(self.json_file, "wb") as f:
            f.write(orjson.dumps({
                "data": dict(self.json_data),
                "statistics": self.statistics
            }))