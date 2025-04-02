import os
import json
import re
import subprocess
import platform
import heapq
import sys
import time
from nltk.stem import PorterStemmer, LancasterStemmer


class TextProcessor:
    def __init__(self, file: str, stemmer_type: str):
        self.file = file
        self.token_count = 0
        self.doc_count = 0

        if stemmer_type == "porter":
            self.json_file = "porter.json"
            self.stemmer = PorterStemmer()
        elif stemmer_type == "lancaster":
            self.json_file = "lancaster.json"
            self.stemmer = LancasterStemmer()
        else:
            print("Elija un stemmer valido")
            sys.exit(1)
        
        self.json_data = self.load_json()

        if "data" not in self.json_data:
            self.json_data["data"] = {}
        if "statistics" not in self.json_data:
            self.json_data["statistics"] = {}

    def readline_plus(self, file: str) -> str:
        """Lee una línea del archivo y actualiza el contador de tokens."""
        aux = re.sub("\n", "", file.readline())
        if aux:
            self.token_count += 1
        return aux

    def process_text(self):
        pattern = r"<DOCNO>\s*([0-9]+)\s*</DOCNO>"

        with open(self.file) as trec_file:
            line = trec_file.readline().strip()
            while line:
                if line == "<DOC>":
                # New document.
                    self.doc_count += 1
                    line = trec_file.readline().strip()
                    docno_match = re.search(pattern, line)
                    docno = docno_match.group(1)
                    line = trec_file.readline().strip()
                if line != "<\DOC>":
                    tokens = self.clean_and_divide(line)
                    for token in tokens:
                        self.token_count += 1
                        self.update_json_in_memory(self.json_data["data"], token, docno, 1)
                    line = trec_file.readline().strip()
        self.save_json()
        self.save_json_statistics()

    def clean_and_divide(self,text):
        clean_text = re.sub(r'[^a-zA-Z0-9ÁÉÍÓÚáéíóúÑñ]', ' ', text)
        words = clean_text.split()
        return words
    
    def sort_words_so(self, filename: str, output_file: str):
        """Ordena las palabras de un archivo usando el método adecuado según el sistema operativo."""
        if platform.system() == "Windows":
            self.sort_words_windows(filename, output_file)
        else:
            self.sort_words_unix(filename, output_file)

    def sort_words_unix(self, filename, output_file):
        """Ordena las palabras en sistemas Unix."""
        command = (
            f"cat {filename} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\n' "
            f"| sed 's/[^a-z]/ /g' | tr -s ' ' '\n' | sed '/^$/d' | sort > {output_file}"
        )
        subprocess.run(command, shell=True, check=True)

    def sort_words_windows(self, filename, output_file):
        """Ordena las palabras en Windows usando un heap."""
        heap = []

        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                line = re.sub(r'[^a-z\s]', ' ', line.lower())
                for word in line.split():
                    heapq.heappush(heap, word)

        with open(output_file, 'w', encoding='utf-8') as output:
            while heap:
                output.write(heapq.heappop(heap) + '\n')

    def update_json_in_memory(self, data, word: str, doc_id: str, freq: int):
        term = word.lower()
        stemmed_term = self.stemmer.stem(term)

        if stemmed_term not in data:
            data[stemmed_term] = {
                "palabra": stemmed_term, "df": 0, "apariciones": {}
            }

        if doc_id not in data[stemmed_term]["apariciones"]:
            data[stemmed_term]["df"] += 1

        data[stemmed_term]["apariciones"][doc_id] = freq

    def load_json(self):
        """Carga los datos desde el archivo JSON si existe."""
        try:
            with open(self.json_file, "r", encoding="iso-8859-1") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_json(self):
        """Guarda los datos en el archivo JSON."""
        with open(self.json_file, "w", encoding="iso-8859-1") as f:
            json.dump(self.json_data, f, ensure_ascii=False, indent=4)

    def save_json_statistics(self):
        """Guarda estadísticas del corpus en el JSON."""
        self.json_data["statistics"] = {
            "N": self.doc_count,
            "num_terms": len(self.json_data["data"]),
            "num_tokens": self.token_count,
        }
        self.save_json()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python script.py <ruta_del_archivo_corpus> <stemmer_type: porter | lancaster>")
        sys.exit(1)

    file_corpus = sys.argv[1]
    stemmer_type = sys.argv[2]

    if not os.path.isfile(file_corpus):
        print(f"Error: El archivo '{file_corpus}' no existe.")
        sys.exit(1)

    start_time = time.time()

    processor = TextProcessor(file_corpus, stemmer_type)
    processor.process_text()
    processor.save_json_statistics()

    end_time = time.time()
    print(f"Tiempo de ejecución con {stemmer_type} stemmer: {end_time - start_time:.2f} segundos")
