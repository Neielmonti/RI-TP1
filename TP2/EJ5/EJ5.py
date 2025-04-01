import os
import json
import re
import subprocess
import platform
import heapq
import sys
from nltk.stem import PorterStemmer, LancasterStemmer

class TextProcessor:
    def __init__(self, folder):
        self.folder = folder

        self.json_porter_file = "porter.json"
        self.json_lancaster_file = "lancaster.json"

        self.sorting_file = "ordenado.txt"
        self.token_count = 0
        self.doc_count = 0

        self.porter_stemmer = PorterStemmer()
        self.lancaster_stemmer = LancasterStemmer()

        self.porter_stemmer.term_count = 0
        self.lancaster_stemmer.term_count = 0
        
        self.json_porter_data = self.load_json(self.json_porter_file)
        self.json_lancaster_data = self.load_json(self.json_lancaster_file)

        self.prepareJsons(self.json_porter_data)
        self.prepareJsons(self.json_lancaster_data)

    def prepareJsons(self, json_data):
        if "data" not in json_data:
            json_data["data"] = {}
        if "statistics" not in json_data:
            json_data["statistics"] = {}

    def readline_plus(self, file) -> str:
        """Lee una línea del archivo y actualiza el contador de tokens."""
        aux = re.sub("\n", "", file.readline())
        if aux:
            self.token_count += 1
        return aux

    def open_files_from_folder(self):
        pattern = r"<DOCNO>\s*([0-9]+)\s*</DOCNO>"

        with open(self.folder) as trec_file:
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
                    tokens = self.limpiar_y_dividir(line)
                    for token in tokens:
                        self.token_count += 1
                        print(f"Token: {token}")
                        self.update_json_in_memory(self.json_porter_data["data"], token, docno, 1, self.porter_stemmer)
                        self.update_json_in_memory(self.json_lancaster_data["data"], token, docno, 1, self.lancaster_stemmer)
                    line = trec_file.readline().strip()
        self.save_json(self.json_porter_file, self.json_porter_data)
        self.save_json(self.json_lancaster_file, self.json_lancaster_data)
        self.save_json_statistics(self.json_porter_file, self.json_porter_data, self.porter_stemmer)
        self.save_json_statistics(self.json_lancaster_file, self.json_lancaster_data, self.lancaster_stemmer)


    def limpiar_y_dividir(self,texto):
        texto_limpio = re.sub(r'[^a-zA-Z0-9ÁÉÍÓÚáéíóúÑñ]', ' ', texto)
        palabras = texto_limpio.split()
        return palabras


    def sort_words_so(self, filename, output_file):
        """Ordena las palabras de un archivo usando el método adecuado según el sistema operativo."""
        if platform.system() == "Windows":
            self.sort_words_windows(filename, output_file)
        else:
            self.sort_words_unix(filename, output_file)

    def sort_words_unix(self, filename, output_file):
        command = (
            f"cat {filename} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\n' "
            f"| sed 's/[^a-z]/ /g' | tr -s ' ' '\n' | sed '/^$/d' | sort > {output_file}"
        )
        subprocess.run(command, shell=True, check=True)

    def sort_words_windows(self, filename, output_file):
        heap = []

        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                line = re.sub(r'[^a-z\s]', ' ', line.lower())
                for word in line.split():
                    heapq.heappush(heap, word)

        with open(output_file, 'w', encoding='utf-8') as output:
            while heap:
                output.write(heapq.heappop(heap) + '\n')
                

    def update_json_in_memory(self, data, palabra, doc_id, freq, stemmer):
        """Actualiza el diccionario JSON en memoria con la información de frecuencia de términos."""
        stemmed_term = stemmer.stem(palabra)

        stemmer.term_count += 1

        print(f"Stemmed by {stemmer.__class__.__name__}: {stemmed_term}")

        if stemmed_term not in data:
            data[stemmed_term] = {
                "palabra": stemmed_term, "df": 0, "apariciones": {}
            }

        if doc_id not in data[stemmed_term]["apariciones"]:
            data[stemmed_term]["df"] += 1

        data[stemmed_term]["apariciones"][doc_id] = freq

    def load_json(self,json_file):
        """Carga los datos desde el archivo JSON si existe."""
        try:
            with open(json_file, "r", encoding="iso-8859-1") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_json(self, file, data):
        """Guarda los datos en el archivo JSON."""
        with open(file, "w", encoding="iso-8859-1") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def save_json_statistics(self, file, data, stemmer):
        """Guarda estadísticas del corpus en el JSON."""
        data["statistics"] = {
            "N": self.doc_count,
            "num_terms": len(data["data"]),
            "num_tokens": self.token_count,
        }
        self.save_json(file,data)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Uso: python script.py <ruta_del_folder_corpus>")
        sys.exit(1)

    folder_corpus = sys.argv[1]

    if not os.path.isfile(folder_corpus):
        print(f"Error: El folder '{folder_corpus}' no existe.")
        sys.exit(1)

    processor = TextProcessor(folder_corpus)
    processor.open_files_from_folder()
