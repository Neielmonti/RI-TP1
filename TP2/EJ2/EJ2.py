import os
import json
from os import path
import re
import subprocess
import argparse
    
token_count = 0
doc_count = 0
min_len = 5
max_len = 10

sum_terms_per_doc = 0

json_file = "palabras.json"
sorting_file = "ordenado.txt"
terms_file = "terminos.txt"
statistics_file = "estadisticas.txt"
frequency_file = "frecuencias.txt"

stop_words_file = ""
check_for_stop_words = False
stopWords = []

shortest_doc = ""
shortest_doc_size = 99999999999999

largest_doc = ""
largest_doc_size = 0

def loadStopWords():
    global stop_words_file
    global stopWords
    with open(stop_words_file, "r", encoding="iso-8859-1") as file:
        for line in file:
            stopWords.extend(line.strip().lower().split())

def readlinePlus(file) -> str:
    global token_count
    
    aux = re.sub("\n", "", file.readline())
    if aux != "":
        token_count += 1

    return aux

def isAValidToken(token: str) -> bool:
    global min_len
    global max_len
    global check_for_stop_words
    global stopWords
    if len(token) < min_len or len(token) > max_len:
        return False
    if check_for_stop_words:
        if token in stopWords:
            return False
    return True

def checkSizeDoc(docID: int, size: int):
    global shortest_doc
    global shortest_doc_size
    global largest_doc
    global largest_doc_size

    if size < shortest_doc_size:
        shortest_doc = docID
        shortest_doc_size = size
    elif size > largest_doc_size:
        largest_doc = docID
        largest_doc_size = size

def openFilesFromFolder(folder: str):
    json_data = load_json(json_file)

    if "data" not in json_data:
        json_data["data"] = {}
    if "statistics" not in json_data:
        json_data["statistics"] = {}

    global doc_count
    global sum_terms_per_doc

    files = sorted(f for f in os.listdir(folder) if f.endswith('.txt')) 
    
    for file in files:
        doc_count += 1
        print("Procesando archivo: " + file)
        
        match = re.search(r'\d+', file)
        if not match:
            continue
        docID = match.group()

        doc_token_count = 0

        sort_words_unix(os.path.join(folder, file), sorting_file)
        
        with open(sorting_file, "r", encoding="iso-8859-1") as f:
            word1 = readlinePlus(f)
            
            while word1:
                contador = 1
                word2 = readlinePlus(f)
                
                while word1 == word2 and word1:
                    contador += 1
                    word2 = readlinePlus(f)
                if isAValidToken(word1):
                    updateJsonInMemory(json_data["data"], word1, docID, contador)
                    sum_terms_per_doc += 1
                doc_token_count += contador
                word1 = word2

        checkSizeDoc(docID, doc_token_count)
    
    save_json(json_file, json_data)
    save_terms_file(json_data)
    save_statistics_file(json_data)
    save_top_terms(json_file, top="max")
    save_top_terms(json_file, top="min")

def sort_words_unix(filename, output_file):
    command = f"cat {filename} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\\n' | sed 's/[^a-z]/ /g' | tr -s ' ' '\\n' | sed '/^$/d' | sort > {output_file}"
    subprocess.run(command, shell=True, check=True)

def updateJsonInMemory(data, palabra, docID, freq):
    if palabra not in data:
        data[palabra] = {"palabra": palabra, "df": 0, "apariciones": {}}
    
    if "cf" not in data[palabra]:
        data[palabra]["cf"] = 0

    data[palabra]["cf"] += freq

    if docID not in data[palabra]["apariciones"]:
        data[palabra]["df"] += 1  

    data[palabra]["apariciones"][docID] = freq

def load_json(json_file):
    try:
        with open(json_file, "r", encoding="iso-8859-1") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_json(json_file, data):
    with open(json_file, "w", encoding="iso-8859-1") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def get_json_values(json_file, palabra):
    try:
        with open(json_file, "r", encoding="iso-8859-1") as f:
            data = json.load(f)
        return data.get("data", {}).get(palabra, None)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def save_json_statistics(json_file):
    json_data = load_json(json_file)

    term_count = len(json_data["data"])
    json_data["statistics"] = {"N": doc_count, "num_terms": term_count , "num_tokens": token_count}

    save_json(json_file, json_data)

def get_terms_average_len(json_data) -> int:
    terms = json_data["data"]
    term_count = len(terms)
    total_length = sum(len(term) for term in terms)
    return (total_length//term_count)

def save_statistics_file(json_data):
    global doc_count
    global statistics_file
    global sum_terms_per_doc
    global token_count

    with open(statistics_file, "w", encoding="iso-8859-1") as f:
        term_count = len(json_data["data"])
        term_average_len = get_terms_average_len(json_data)
        terms_with_freq1 = countTermsWithFreq1(json_data)

        f.write(f"{doc_count}\n")
        f.write(f"{token_count} {term_count}\n")
        f.write(f"{token_count/doc_count} {sum_terms_per_doc/doc_count}\n")
        f.write(f"{term_average_len}\n")
        f.write(f"{terms_with_freq1}\n")


def save_terms_file(json_data):
    global terms_file
    with open(terms_file, "w", encoding="iso-8859-1") as f:
        for term, data in json_data.get("data", {}).items():
            cf = sum(data["apariciones"].values())  # Total de la frecuencia en la colección
            df = data["df"]  # Document Frequency
            # Escribir el término, CF y DF en el archivo de texto
            f.write(f"{term} {cf} {df}\n")

def save_top_terms(json_file, top="max"):
    global frequency_file

    # Cargar el JSON
    with open(json_file, "r", encoding="iso-8859-1") as file:
        json_data = json.load(file)

    terms = json_data.get("data", {})  # Extraer la sección de términos
    
    # Convertir a lista de tuplas (termino, cf)
    term_list = [(term, info["cf"]) for term, info in terms.items()]
    
    # Ordenar por CF de menor a mayor
    term_list.sort(key=lambda x: x[1], reverse=(top == "max"))

    # Seleccionar el top 10
    top_terms = term_list[:10]

    # Escribir en el archivo de salida
    with open(frequency_file, "a", encoding="iso-8859-1") as file:
        for term, cf in top_terms:
            file.write(f"{term} {cf}\n")

def countTermsWithFreq1(json_data):
    terms = json_data.get("data", {})
    term_list = [(term, info["cf"]) for term, info in terms.items()]
    terms_with_freq1 = [term for term, cf in term_list if cf == 1]
    return len(terms_with_freq1)

def main(corpus_folder, stop_words_folder):
    global stop_words_file
    global check_for_stop_words

    print(f"corpus_folder: {corpus_folder}")

    if stop_words_folder:
        print(f"stop_words_folder: {stop_words_folder}")
        stop_words_file = stop_words_folder
        check_for_stop_words = True
        loadStopWords()

    else:
        print("No se proporcionó stop_words_folder.")

    openFilesFromFolder(corpus_folder)
    save_json_statistics(json_file)
    os.remove(sorting_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parámetros del programa")
    parser.add_argument("corpus_folder", help="Ruta al directorio que contiene los documentos")
    parser.add_argument("stop_words_folder", nargs="?", default=None, help="Ruta al archivo que contiene las palabras vacías (opcional)")
    args = parser.parse_args()
    main(args.corpus_folder, args.stop_words_folder)

