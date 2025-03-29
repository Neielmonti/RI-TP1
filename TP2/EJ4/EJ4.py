import os
import json
import re
import subprocess
import platform
import heapq
import sys
from nltk.stem import SnowballStemmer


class TextProcessor:
    def __init__(self, folder):
        self.folder = folder
        self.json_file = "palabras.json"
        self.sorting_file = "ordenado.txt"
        self.token_count = 0
        self.doc_count = 0
        self.stemmer = SnowballStemmer("spanish")
        self.json_data = self.load_json()

        if "data" not in self.json_data:
            self.json_data["data"] = {}
        if "statistics" not in self.json_data:
            self.json_data["statistics"] = {}

    def readline_plus(self, file) -> str:
        """Lee una línea del archivo y actualiza el contador de tokens."""
        aux = re.sub("\n", "", file.readline())
        if aux:
            self.token_count += 1
        return aux

    def open_files_from_folder(self):
        """Abre y procesa los archivos de texto dentro del folder."""
        files = sorted(f for f in os.listdir(self.folder) if f.endswith('.txt'))

        for file in files:
            self.doc_count += 1
            match = re.search(r'\d+', file)
            if not match:
                continue
            doc_id = match.group()

            # Ordenar las palabras dentro del archivo
            self.sort_words_so(os.path.join(self.folder, file), self.sorting_file)

            with open(self.sorting_file, "r", encoding="utf-8") as f:
                word1 = self.readline_plus(f)
                while word1:
                    contador = 1
                    word2 = self.readline_plus(f)

                    while word1 == word2 and word1:
                        contador += 1
                        word2 = self.readline_plus(f)

                    self.update_json_in_memory(self.json_data["data"], word1, doc_id, contador)
                    word1 = word2

        self.save_json()

    def sort_words_so(self, filename, output_file):
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

    def update_json_in_memory(self, data, palabra, doc_id, freq):
        """Actualiza el diccionario JSON en memoria con la información de frecuencia de términos."""
        palabra_stemmed = self.stemmer.stem(palabra)

        if palabra_stemmed not in data:
            data[palabra_stemmed] = {
                "palabra": palabra_stemmed, "df": 0, "apariciones": {}
            }

        if doc_id not in data[palabra_stemmed]["apariciones"]:
            data[palabra_stemmed]["df"] += 1

        data[palabra_stemmed]["apariciones"][doc_id] = freq

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
    if len(sys.argv) != 2:
        print("Uso: python script.py <ruta_del_folder_corpus>")
        sys.exit(1)

    folder_corpus = sys.argv[1]

    if not os.path.isdir(folder_corpus):
        print(f"Error: El folder '{folder_corpus}' no existe.")
        sys.exit(1)

    processor = TextProcessor(folder_corpus)
    processor.open_files_from_folder()
    processor.save_json_statistics()

    os.remove(processor.sorting_file)
