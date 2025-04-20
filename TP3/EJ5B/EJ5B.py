import json
import re
from collections import Counter
from collections import defaultdict
import math
import sys
import os
sys.path.append(os.path.abspath("D:\Facultad\5to anio\Primer cuatrimestre\Recuperacion de la informacion\TPS\RI-TP1\TP3\EJ4\indexador.py"))  # ajustá esta ruta
from indexador import indexar_y_buscar

parser = argparse.ArgumentParser(description="Indexador con PyTerrier.")
parser.add_argument("input_dir", type=str, help="Directorio raíz con los archivos HTML")
args = parser.parse_args()

def load_json():
    try:
        with open("palabras.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

json_data = load_json()

datos = json_data["data"]

query = "Isaac Newtone"

def contar_frecuencia(texto: str) -> list:
    palabras = re.findall(r'\b[a-záéíóúñü]+\b', texto.lower(), re.UNICODE)
    contador = Counter(palabras)

    lista_terminos = []
    for palabra, freq in contador.items():
        lista_terminos.append({
            "termino": palabra,
            "frecuencia": freq
            # Acá podés agregar más atributos después: "idf", "documentos", etc.
        })
    
    return lista_terminos

terminos = contar_frecuencia(query)
docs_por_termino = {}

# Ejemplo de cómo agregar atributos después
for t in terminos:
    if t['termino'] in datos:
        df = datos[t['termino']]["df"]
        t["idf"] = math.log10(df) if df > 0 else 0
        t["documentos"] = datos[t['termino']]["apariciones"]
    else:
        t["idf"] = 0
        t["documentos"] = {}


puntajes = defaultdict(float)

for t in terminos:
    freq_query = t["frecuencia"]
    idf = t["idf"]
    docs = t["documentos"]

    for doc_id, freq_doc in docs.items():
        tf = 1 + math.log10(freq_doc)
        peso = tf * idf * freq_query
        puntajes[doc_id] += peso

# Ordenar los documentos por puntaje descendente
ranking = sorted(puntajes.items(), key=lambda x: x[1], reverse=True)

# Mostrar resultados
print("Ranking de documentos:")
for doc_id, score in ranking:
    print(f"Doc {doc_id}: {score:.4f}")


input_dir = input_dir
query = "Isaac Newtone"

resultados = indexar_y_buscar(input_dir, query)

print("-----------------RANKING DE PYTERRIER-------------------")
print(resultados.head())