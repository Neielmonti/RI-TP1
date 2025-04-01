import os
import json
import re
import platform
import heapq
import subprocess
import argparse


class TextProcessor:
    def __init__(self, folder):
        self.folder = folder
        self.json_file = "palabras.json"
        self.sorting_file = "ordenado.txt"
        self.token_count = 0
        self.doc_count = 0
        self.json_data = self.load_json()

        if "data" not in self.json_data:
            self.json_data["data"] = {}
        if "statistics" not in self.json_data:
            self.json_data["statistics"] = {}

    def process_files(self):
        # Ordenamos los archivos a procesar.
        # Esto no es necesario, pero servia para ver el avance mas facil
        # al desarrollar el programa.
        files = sorted(
            f for f in os.listdir(self.folder) if f.endswith('.txt')
            )

        for file in files:
            self.doc_count += 1
            match = re.search(r'\d+', file)
            if not match:
                continue
            docID = match.group()

            # Creamos el archivo "ordenado.txt" para el corte de control
            self.sort_words(os.path.join(self.folder, file), self.sorting_file)

            with open(self.sorting_file, "r", encoding="utf-8") as f:
                word1 = self.readlinePlus(f)
                while word1:
                    term_count = 1
                    word2 = self.readlinePlus(f)

                    while word1 == word2 and word1:
                        term_count += 1
                        word2 = self.readlinePlus(f)
                    # Agregamos el termino y su frecuencia
                    self.update_json_in_memory(word1, docID, term_count)
                    word1 = word2

        # Guardamos y eliminamos los archivos correspondientes
        self.save_json()
        self.save_json_statistics()
        os.remove(self.sorting_file)

    def readlinePlus(self, file) -> str:
        aux = re.sub("\n", "", file.readline())
        if aux != "":
            self.token_count += 1
        return aux

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

    def update_json_in_memory(self, term, docID, freq):
        # Esta funcion verifica si el termino esta en el json_data
        if term not in self.json_data["data"]:
            # De no estar, la agrega con df 0 y con ninguna aparicion
            self.json_data["data"][term] = {
                "palabra": term, "df": 0, "apariciones": {}
                }
        # Si el docID no está en el campo de apariciones del termino,
        # lo agrega, y aumenta su df
        if docID not in self.json_data["data"][term]["apariciones"]:
            self.json_data["data"][term]["df"] += 1

        self.json_data["data"][term]["apariciones"][docID] = freq

    def save_json_statistics(self):
        # Esta funcion calcula las estadisticas (o mejor dicho las guarda)
        # en el JSON, a partir de atributos del objeto, (a excepcion del
        # term_count, ya que este lo extrae del len del JSON)
        term_count = len(self.json_data["data"])
        self.json_data["statistics"] = {
            "N": self.doc_count,
            "num_terms": term_count,
            "num_tokens": self.token_count,
        }
        self.save_json()

    """ESTAS FUNCIONES SIMPLEMENTE CARGAN Y ALMACENAN EL JSON"""
    def load_json(self):
        try:
            with open(self.json_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_json(self):
        with open(self.json_file, "w", encoding="utf-8") as f:
            json.dump(self.json_data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Procesar archivos de texto en una carpeta."
        )
    parser.add_argument(
        "folder", type=str,
        help="Ruta de la carpeta con los archivos de texto (corpus)"
        )
    args = parser.parse_args()
    processor = TextProcessor(args.folder)
    processor.process_files()
