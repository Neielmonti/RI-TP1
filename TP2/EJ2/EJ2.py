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
json_file = "palabras.json"
sorting_file = "ordenado.txt"
terms_file = "terminos.txt"
stop_words_file = ""
check_for_stop_words = False
stopWords = []

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

def openFilesFromFolder(folder: str):
    json_data = load_json(json_file)

    if "data" not in json_data:
        json_data["data"] = {}
    if "statistics" not in json_data:
        json_data["statistics"] = {}

    global doc_count

    files = sorted(f for f in os.listdir(folder) if f.endswith('.txt')) 
    
    for file in files:
        doc_count += 1
        print("Procesando archivo: " + file)
        
        match = re.search(r'\d+', file)
        if not match:
            continue
        docID = match.group()
        
        sort_words_unix(os.path.join(folder, file), sorting_file)
        
        with open(sorting_file, "r", encoding="utf-8") as f:
            word1 = readlinePlus(f)
            
            while word1:
                contador = 1
                word2 = readlinePlus(f)
                
                while word1 == word2 and word1:
                    contador += 1
                    word2 = readlinePlus(f)
                if isAValidToken(word1):
                    updateJsonInMemory(json_data["data"], word1, docID, contador)
                word1 = word2
    
    save_json(json_file, json_data)
    save_terms_file(json_data)

def sort_words_unix(filename, output_file):
    command = f"cat {filename} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\\n' | sed 's/[^a-z]/ /g' | tr -s ' ' '\\n' | sed '/^$/d' | sort > {output_file}"
    subprocess.run(command, shell=True, check=True)

def updateJsonInMemory(data, palabra, docID, freq):
    if palabra not in data:
        data[palabra] = {"palabra": palabra, "df": 0, "apariciones": {}}
    
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


def save_terms_file(json_data):
    global terms_file
    with open(terms_file, "w", encoding="iso-8859-1") as f:
        for term, data in json_data.get("data", {}).items():
            cf = sum(data["apariciones"].values())  # Total de la frecuencia en la colección
            df = data["df"]  # Document Frequency
            # Escribir el término, CF y DF en el archivo de texto
            f.write(f"{term} {cf} {df}\n")

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

