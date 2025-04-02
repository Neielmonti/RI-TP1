import os
import json
import re
import subprocess
import argparse
from collections import Counter


class DocumentProcessor:
    def __init__(self, corpus_folder):
        self.token_count = 0
        self.doc_count = 0

        self.json_file = "palabras.json"

        self.shortest_doc = ""
        self.shortest_doc_size = float("inf")
        self.largest_doc = ""
        self.largest_doc_size = 0

        self.corpus_folder = corpus_folder
        self.pattern_url = (
            r"(?:https?|ftp):\/\/(?:www\.)?[\w.-]+(?:\.[a-zA-Z]{2,6})+"
            r"(?:\/[\w\/._-]*)*(?:\?[\w\&=\-;%\.\d]*)?(?:#[\w-]*)?"
        )
        self.pattern_email = (
            r"[a-zA-Z0-9_]+(?:[.][a-zA-Z0-9_]+)*@[a-zA-Z0-9_]+"
            r"(?:[.][a-zA-Z0-9_]+)*[.][a-zA-Z]{2,5}"
        )
        self.pattern_number = r"[0-9]+(?:-[0-9]+)*"
        self.pattern_abbr = r"(?:[A-Z][A-Z]?[a-z]*\.)+[A-Z]?"
        self.pattern_name = r"[A-Z][a-z]+(?: [A-Z][a-z]+)*"
        #self.pattern_word = r"\b\w+\b"

        self.patterns = [
            self.pattern_url,
            self.pattern_email,
            self.pattern_number,
            self.pattern_abbr,
            self.pattern_name,
        #    self.pattern_word,
        ]

    def loadStopWords(self):
        with open(self.stop_words_file, "r", encoding="utf-8") as file:
            for line in file:
                self.stopWords.extend(line.strip().lower().split())

    def readlinePlus(self, file) -> str:
        aux = re.sub("\n", "", file.readline().strip())
        return aux

    def isAValidToken(self, token: str) -> bool:
        if len(token) < self.min_len or len(token) > self.max_len:
            return False
        if self.check_for_stop_words and token in self.stopWords:
            return False
        return True

    def checkSizeDoc(self, docID: int, size: int):
        if size < self.shortest_doc_size:
            self.shortest_doc = docID
            self.shortest_doc_size = size
        elif size > self.largest_doc_size:
            self.largest_doc = docID
            self.largest_doc_size = size

    def process_files(self):
        json_data = self.load_json(self.json_file)
        json_data.setdefault("data", {})
        json_data.setdefault("statistics", {})

        files = sorted(
            f for f in os.listdir(self.corpus_folder) if f.endswith(".txt")
        )

        for file in files:
            docID = file

            with open(
                os.path.join(self.corpus_folder, file), "r", encoding="utf-8"
            ) as f:
                extracted_terms = []
                word1 = self.readlinePlus(f)
                cleaned_text = word1

                for pattern in self.patterns:
                    matches = re.findall(pattern, cleaned_text)
                    if matches:
                        extracted_terms.extend(
                            [m[0] if isinstance(m, tuple) else m for m in matches]
                        )
                        for term in matches:
                            self.token_count += 1
                            cleaned_text = re.sub(
                                r"(?<!\w)" + re.escape(term) + r"(?!\w)", "", cleaned_text, 1
                            )

                cleaned_text = re.sub(r"[^a-zA-ZáéíóúüñÁÉÍÓÚÜÑ]+", " ", cleaned_text).strip()
                extracted_terms_counts = Counter(extracted_terms)
                tokens = [token for token in cleaned_text.split() if token.strip()]
                sorted_tokens = sorted(tokens)
                token_counts = Counter(sorted_tokens)

                self.token_count += len(tokens)
                
                for term, freq in extracted_terms_counts.items():
                    self.updateJsonInMemory(json_data["data"], term, docID, freq)
                for token, freq in token_counts.items():
                    self.updateJsonInMemory(json_data["data"], token, docID , freq)

            self.checkSizeDoc(docID, len(tokens))

        self.save_json(self.json_file, json_data)
        self.save_json_statistics(json_data)

    def updateJsonInMemory(self, data, word, docID, freq):
        term = word.lower()
        if term not in data:
            data[term] = {"palabra": term, "df": 0, "apariciones": {}}

        if "cf" not in data[term]:
            data[term]["cf"] = 0

        data[term]["cf"] += freq

        if docID not in data[term]["apariciones"]:
            data[term]["df"] += 1

        data[term]["apariciones"][docID] = freq

    def save_json_statistics(self, json_data):
        # Esta funcion calcula las estadisticas (o mejor dicho las guarda)
        # en el JSON, a partir de atributos del objeto, (a excepcion del
        # term_count, ya que este lo extrae del len del JSON)
        term_count = len(json_data["data"])
        json_data["statistics"] = {
            "N": self.doc_count,
            "num_terms": term_count,
            "num_tokens": self.token_count,
        }
        self.save_json(self.json_file, json_data)
    
    def load_json(self, json_file):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_json(self, json_file, data):
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def run(self):
        self.process_files()
        self.save_json(self.json_file, self.load_json(self.json_file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parámetros del programa")
    parser.add_argument("corpus_folder", help="Ruta al directorio que contiene los documentos")
    args = parser.parse_args()
    processor = DocumentProcessor(args.corpus_folder)
    processor.run()


