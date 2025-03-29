import os
import json
import re
import subprocess
from zipfile import ZipFile

with ZipFile("collection_test.zip", 'r') as zObject:
    zObject.extractall(path="collection_test")

with ZipFile(
    "collection_test/collection_test/TestCollection.zip", 'r'
) as zObject:
    zObject.extractall(path="collection_test/collection_test/TestCollection")

token_count = 0
doc_count = 0


def readlinePlus(file) -> str:
    global token_count
    aux = re.sub("\n", "", file.readline())
    if aux != "":
        token_count += 1
    return aux


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
                updateJsonInMemory(json_data["data"], word1, docID, contador)
                word1 = word2

    save_json(json_file, json_data)


def sort_words_unix(filename, output_file):
    command = (
        "cat {} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\n' "
        "| sed 's/[^a-z]/ /g' | tr -s ' ' '\n' | sed '/^$/d' | sort > {}"
    ).format(filename, output_file)
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
    json_data["statistics"] = {
        "N": doc_count,
        "num_terms": term_count,
        "num_tokens": token_count,
    }
    save_json(json_file, json_data)


json_file = "palabras.json"
sorting_file = "ordenado.txt"
openFilesFromFolder(
    "collection_test/collection_test/TestCollection/TestCollection"
)
save_json_statistics(json_file)
os.remove(sorting_file)
