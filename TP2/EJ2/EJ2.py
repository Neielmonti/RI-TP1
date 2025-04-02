import os
import json
import re
import platform
import heapq
import subprocess
import argparse


class DocumentProcessor:
    def __init__(self, corpus_folder, stop_words_folder=None):
        self.token_count = 0
        self.doc_count = 0
        self.min_len = 0
        self.max_len = 3000
        self.sum_terms_per_doc = 0

        self.json_file = "palabras.json"
        self.sorting_file = "ordenado.txt"
        self.terms_file = "terminos.txt"
        self.statistics_file = "estadisticas.txt"
        self.frequency_file = "frecuencias.txt"

        self.stop_words_file = stop_words_folder
        self.check_for_stop_words = False
        self.stopWords = []

        self.shortest_doc = ""
        self.shortest_doc_size = 99999999999999
        self.largest_doc = ""
        self.largest_doc_size = 0

        if stop_words_folder:
            self.check_for_stop_words = True
            self.loadStopWords()

        self.corpus_folder = corpus_folder

    def loadStopWords(self):
        with open(self.stop_words_file, "r", encoding="utf-8") as file:
            for line in file:
                self.stopWords.extend(line.strip().lower().split())

    def readlinePlus(self, file) -> str:
        aux = re.sub("\n", "", file.readline())
        if aux != "":
            self.token_count += 1
        return aux

    def isAValidToken(self, token: str) -> bool:
        if len(token) < self.min_len or len(token) > self.max_len:
            return False
        if self.check_for_stop_words:
            if token in self.stopWords:
                return False
        return True

    def checkSizeDoc(self, docID: int, size: int):
        if size < self.shortest_doc_size:
            self.shortest_doc = docID
            self.shortest_doc_size = size
        elif size > self.largest_doc_size:
            self.largest_doc = docID
            self.largest_doc_size = size

    def openFilesFromFolder(self):
        json_data = self.load_json(self.json_file)

        if "data" not in json_data:
            json_data["data"] = {}
        if "statistics" not in json_data:
            json_data["statistics"] = {}

        files = sorted(f for f in os.listdir(self.corpus_folder) if f.endswith('.txt'))

        for file in files:
            self.doc_count += 1

            docID = file

            doc_token_count = 0

            self.sort_words(os.path.join(self.corpus_folder, file), self.sorting_file)

            with open(self.sorting_file, "r", encoding="utf-8") as f:
                word1 = self.readlinePlus(f)

                while word1:
                    contador = 1
                    word2 = self.readlinePlus(f)

                    while word1 == word2 and word1:
                        contador += 1
                        word2 = self.readlinePlus(f)
                    if self.isAValidToken(word1):
                        self.updateJsonInMemory(json_data["data"], word1, docID, contador)
                        self.sum_terms_per_doc += 1
                    doc_token_count += contador
                    word1 = word2

            self.checkSizeDoc(docID, doc_token_count)

        self.save_json(self.json_file, json_data)
        self.save_terms_file(json_data)
        self.save_statistics_file(json_data)
        self.save_top_terms(self.json_file, top="max")
        self.save_top_terms(self.json_file, top="min")

    def sort_words(self, filename, output_file):
        if platform.system() == "Windows":
            self.sort_words_windows(filename, output_file)
        else:
            self.sort_words_unix(filename, output_file)

    def sort_words_unix(self, filename, output_file):
        # Creo un archivo nuevo, reemplazando Uppercase por Lowecase,
        # haciendo un split en los espacios, dejando cada palabra en
        # una linea del texto, y removiendo todos los caracteres que
        # no sean letras.
        command = (
            f"cat {filename} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\n' "
            f"| sed 's/[^a-z]/ /g' | tr -s ' ' '\n' | sed '/^$/d' | sort > {output_file}"
        )
        subprocess.run(command, shell=True, check=True)

    def sort_words_windows(self, filename, output_file):
        # Similar a la funcion de Unix, nos quedamos con un archivo con
        # el texto limpio y ordenado.
        heap = []

        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                line = re.sub(r'[^a-z\s]', ' ', line.lower())
                for word in line.split():
                    heapq.heappush(heap, word)

        with open(output_file, 'w', encoding='utf-8') as output:
            while heap:
                output.write(heapq.heappop(heap) + '\n')

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

    def load_json(self, json_file):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_json(self, json_file, data):
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def save_json_statistics(self, json_file):
        json_data = self.load_json(json_file)

        term_count = len(json_data["data"])
        json_data["statistics"] = {"N": self.doc_count, "num_terms": term_count, "num_tokens": self.token_count}

        self.save_json(json_file, json_data)

    def get_terms_average_len(self, json_data) -> int:
        terms = json_data["data"]
        term_count = len(terms)
        total_length = sum(len(term) for term in terms)
        return total_length // term_count

    def save_statistics_file(self, json_data):
        with open(self.statistics_file, "w", encoding="utf-8") as f:
            term_count = len(json_data["data"])
            term_average_len = self.get_terms_average_len(json_data)
            terms_with_freq1 = self.countTermsWithFreq1(json_data)

            f.write(f"{self.doc_count}\n")
            f.write(f"{self.token_count} {term_count}\n")
            f.write(f"{self.token_count/self.doc_count} {self.sum_terms_per_doc/self.doc_count}\n")
            f.write(f"{self.token_count} {term_count}\n")
            f.write(f"{term_average_len}\n")
            f.write(f"{self.shortest_doc_size} {self.largest_doc_size}\n")
            f.write(f"{terms_with_freq1}\n")

    def save_terms_file(self, json_data):
        with open(self.terms_file, "w", encoding="utf-8") as f:
            for term, data in json_data.get("data", {}).items():
                cf = sum(data["apariciones"].values()) 
                df = data["df"]
                f.write(f"{term} {cf} {df}\n")

    def save_top_terms(self, json_file, top="max"):
        with open(json_file, "r", encoding="utf-8") as file:
            json_data = json.load(file)

        terms = json_data.get("data", {}) 
        term_list = [(term, info["cf"]) for term, info in terms.items()]
        term_list.sort(key=lambda x: x[1], reverse=(top == "max"))
        top_terms = term_list[:10]

        with open(self.frequency_file, "a", encoding="utf-8") as file:
            for term, cf in top_terms:
                file.write(f"{term} {cf}\n")

    def countTermsWithFreq1(self, json_data):
        terms = json_data.get("data", {})
        term_list = [(term, info["cf"]) for term, info in terms.items()]
        terms_with_freq1 = [term for term, cf in term_list if cf == 1]
        return len(terms_with_freq1)

    def run(self):
        self.openFilesFromFolder()
        self.save_json_statistics(self.json_file)
        os.remove(self.sorting_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parámetros del programa")
    parser.add_argument("corpus_folder", help="Ruta al directorio que contiene los documentos")
    parser.add_argument(
        "stop_words_folder", nargs="?", default=None, help="Ruta al archivo que contiene las palabras vacías (opcional)"
    )
    args = parser.parse_args()

    processor = DocumentProcessor(args.corpus_folder, args.stop_words_folder)
    processor.run()

