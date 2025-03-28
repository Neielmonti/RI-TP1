import os
import json
from os import path
import re
import subprocess
import argparse
from collections import Counter

class DocumentProcessor:
    def __init__(self, corpus_folder, stop_words_folder=None):
        self.token_count = 0
        self.doc_count = 0
        self.min_len = 5
        self.max_len = 10
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

        #self.pattern_number = "([0-9]+[,])*[0-9]([.][0-9]+)?"
        self.pattern_url = r"(?:https?|ftp):\/\/(?:www\.)?[\w.-]+(?:\.[a-zA-Z]{2,6})+(?:\/[\w\/._-]*)*(?:\?[\w\&=\-;%\.\d]*)?(?:#[\w-]*)?"
        self.pattern_email = "[a-zA-Z0-9_]+(?:[.][a-zA-Z0-9_]+)*@[a-zA-Z0-9_]+(?:[.][a-zA-Z0-9_]+)*[.][a-zA-Z]{2,5}"
        self.pattern_number = "[0-9]+(?:-[0-9]+)*"
        self.pattern_abbr = r"(?:[A-Z][A-Z]?[a-z]*\.)+[A-Z]?"
        self.pattern_name = "[A-Z][a-z]+(?: [A-Z][a-z]+)*"

        self.patterns = [value for key, value in vars(self).items() if key.startswith("pattern_")]

    def loadStopWords(self):
        with open(self.stop_words_file, "r", encoding="iso-8859-1") as file:
            for line in file:
                self.stopWords.extend(line.strip().lower().split())

    def readlinePlus(self, file) -> str:
        aux = re.sub("\n", "", file.readline().strip())
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

    def findRegex(self, token: str) -> bool:
        for pattern in self.patterns:
            aux = re.search(pattern, token)
            print("token " + token + ":" , aux)

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
            print("Procesando archivo:", file)

            match = re.search(r'\d+', file)
            if not match:
                continue
            docID = match.group()

            with open(os.path.join(self.corpus_folder, file), "r", encoding="iso-8859-1") as f:
                extracted_terms = []
                word1 = self.readlinePlus(f)

                cleaned_text = word1

                # Iterar sobre los patrones para extraer y eliminar los términos coincidentes uno por uno
                for pattern in self.patterns:
                    matches = re.findall(pattern, cleaned_text)
                    if matches:
                        # Solo agregar términos únicos a la lista de términos extraídos
                        extracted_terms.extend([m[0] if isinstance(m, tuple) else m for m in matches])

                        # Eliminar los términos coincidentes del texto solo una vez
                        for term in matches:
                            # Eliminar el término del texto original
                            cleaned_text = re.sub(r'\b' + re.escape(term) + r'\b', '', cleaned_text, 1)

                # Contar la frecuencia de los términos extraídos
                extracted_terms_counts = Counter(extracted_terms)

                # Tokenización sin incluir vacíos
                tokens = [token for token in cleaned_text.split() if token.strip()]

                # Ordenar tokens alfabéticamente
                sorted_tokens = sorted(tokens)

                # Obtener frecuencia de cada token
                token_counts = Counter(sorted_tokens)

                print("Términos extraídos:", extracted_terms)
                print("Texto limpio:", cleaned_text)
                print("Frecuencia de tokens:", token_counts)

                # Actualizar la memoria con los términos extraídos y sus frecuencias
                for term, freq in extracted_terms_counts.items():
                    self.updateJsonInMemory(json_data["data"], term, docID, freq)

                # Actualizar la memoria con los tokens restantes y sus frecuencias
                for token, freq in token_counts.items():
                    self.updateJsonInMemory(json_data["data"], token, (docID + "NO REGEX"), freq)

            # Verificar tamaño del documento
            self.checkSizeDoc(docID, len(tokens))

        # Guardar los datos procesados
        self.save_json(self.json_file, json_data)
        self.save_terms_file(json_data)
        self.save_statistics_file(json_data)
        self.save_top_terms(self.json_file, top="max")
        self.save_top_terms(self.json_file, top="min")


    def sort_words_unix(self, filename, output_file):
        command = f"cat {filename} | tr -s '[:space:]' '\\n' | sort > {output_file}"
        subprocess.run(command, shell=True, check=True)

    def updateJsonInMemory(self, data, palabra, docID, freq):
        if palabra not in data:
            data[palabra] = {"palabra": palabra, "df": 0, "apariciones": {}}
        
        if "cf" not in data[palabra]:
            data[palabra]["cf"] = 0

        data[palabra]["cf"] += freq

        if docID not in data[palabra]["apariciones"]:
            data[palabra]["df"] += 1  

        data[palabra]["apariciones"][docID] = freq

    def load_json(self, json_file):
        try:
            with open(json_file, "r", encoding="iso-8859-1") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_json(self, json_file, data):
        with open(json_file, "w", encoding="iso-8859-1") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def save_json_statistics(self, json_file):
        json_data = self.load_json(json_file)

        term_count = len(json_data["data"])
        json_data["statistics"] = {"N": self.doc_count, "num_terms": term_count , "num_tokens": self.token_count}

        self.save_json(json_file, json_data)

    def get_terms_average_len(self, json_data) -> int:
        terms = json_data["data"]
        term_count = len(terms)
        total_length = sum(len(term) for term in terms)
        return (total_length // term_count)

    def save_statistics_file(self, json_data):
        with open(self.statistics_file, "w", encoding="iso-8859-1") as f:
            term_count = len(json_data["data"])
            term_average_len = self.get_terms_average_len(json_data)
            terms_with_freq1 = self.countTermsWithFreq1(json_data)

            f.write(f"{self.doc_count}\n")
            f.write(f"{self.token_count} {term_count}\n")
            f.write(f"{self.token_count/self.doc_count} {self.sum_terms_per_doc/self.doc_count}\n")
            f.write(f"{term_average_len}\n")
            f.write(f"{terms_with_freq1}\n")

    def save_terms_file(self, json_data):
        with open(self.terms_file, "w", encoding="iso-8859-1") as f:
            for term, data in json_data.get("data", {}).items():
                cf = sum(data["apariciones"].values()) 
                df = data["df"]
                f.write(f"{term} {cf} {df}\n")

    def save_top_terms(self, json_file, top="max"):
        with open(json_file, "r", encoding="iso-8859-1") as file:
            json_data = json.load(file)

        terms = json_data.get("data", {}) 
        term_list = [(term, info["cf"]) for term, info in terms.items()]
        term_list.sort(key=lambda x: x[1], reverse=(top == "max"))
        top_terms = term_list[:10]

        with open(self.frequency_file, "a", encoding="iso-8859-1") as file:
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parámetros del programa")
    parser.add_argument("corpus_folder", help="Ruta al directorio que contiene los documentos")
    parser.add_argument("stop_words_folder", nargs="?", default=None, help="Ruta al archivo que contiene las palabras vacías (opcional)")
    args = parser.parse_args()

    processor = DocumentProcessor(args.corpus_folder, args.stop_words_folder)
    processor.run()

