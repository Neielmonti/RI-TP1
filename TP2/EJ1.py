import os
import json
from os import path
import re
import subprocess
from zipfile import ZipFile

with ZipFile("collection_test.zip", 'r') as zObject:

    # Extracting all the members of the zip
    # into a specific location.
    zObject.extractall(
        path="collection_test")

with ZipFile("collection_test/collection_test/TestCollection.zip", 'r') as zObject:

    # Extracting all the members of the zip
    # into a specific location.
    zObject.extractall(
        path="collection_test/collection_test/TestCollection")
    



def openFilesFromFolder(folder: str):
  limit = 1
  for root, dirs, files in os.walk(folder):
      for file in files:
          filename, extension = os.path.splitext(file)
          if extension == '.txt':

            sort_words_unix(folder + '/' + file , "ordenado.txt")
            f = open("ordenado.txt" ,"r")
            docID = re.findall(r'\d+', file)[0]
            seguir = True

            word1 = f.readline()

            while seguir:
              word2 = f.readline()
              contador = 1

              while word1 == word2 and word1 != "":
                contador += 1
                word2 = f.readline()
                if word2 == "":
                  seguir = False
                  actualizar_json(json_file, word1, docID, contador)

              if word1 != '':
                actualizar_json(json_file, word1, docID, contador)
                word1 = word2
              else:
                seguir = False


def sort_words_unix(filename, output_file):
    command = f"cat {filename} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\\n' | sed 's/[^a-z]/ /g' | tr -s ' ' '\\n' | sed '/^$/d' | sort > {output_file}"
    subprocess.run(command, shell=True, check=True)


def actualizar_json(json_file, palabra, docID, freq):
    try:
        with open(json_file, "r", encoding="iso-8859-1") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
        with open(json_file, 'w', encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    # Si la palabra no existe, crearla
    if palabra not in data:
        data[palabra] = {
            "palabra": palabra,
            "df": 0,
            "apariciones": {}
        }

    # Si el docID no existe para la palabra, iniciarlo y actualizar DF
    if docID not in data[palabra]["apariciones"]:
        data[palabra]["df"] += 1  

    # Asignar la frecuencia proporcionada
    data[palabra]["apariciones"][docID] = freq

    # Guardar cambios en el JSON
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def obtener_valores(json_file, palabra):
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(palabra, None)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


json_file = "palabras.json"


openFilesFromFolder('collection_test/collection_test/TestCollection/TestCollection')
