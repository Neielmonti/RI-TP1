from bs4 import BeautifulSoup
from pathlib import Path
import argparse
from pathlib import Path
from tokenicer import TextProcessor
import argparse
import re
from collections import Counter
from collections import defaultdict
import math

file_index = 0

textProcessor = TextProcessor()

def directory_dfs(path: Path, textProcessor: TextProcessor) -> None:
    global file_index
    for x in path.iterdir():
        if x.is_dir():
            directory_dfs(x, textProcessor)
        else:
            if (file_index % 250) == 0:
                print(f"Procesando archivo: {file_index}")
            with open(x, "r", encoding="utf-8") as f:
                html = f.read()
            soup = BeautifulSoup(html, features="html.parser")
            for script in soup(["script", "style"]):
                script.extract()
            text = soup.get_text()
            
            textProcessor.process_text(text, str(file_index))
            file_index += 1

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

def ejecutar_query(index_data, query: str):
    terminos = contar_frecuencia(query)
    puntajes = defaultdict(float)

    for t in terminos:
        if t['termino'] in index_data:
            df = index_data[t['termino']]["df"]
            t["idf"] = math.log10(df) if df > 0 else 0
            t["documentos"] = index_data[t['termino']]["apariciones"]
        else:
            t["idf"] = 0
            t["documentos"] = {}

    for t in terminos:
        freq_query = t["frecuencia"]
        idf = t["idf"]
        docs = t["documentos"]

        for doc_id, freq_doc in docs.items():
            tf = 1 + math.log10(freq_doc)
            peso = tf * idf * freq_query
            puntajes[doc_id] += peso

    ranking = sorted(puntajes.items(), key=lambda x: x[1], reverse=True)
    return ranking

def indexar_y_buscar(input_dir: str, query: str):
    textProcessor = TextProcessor()
    directory_dfs(Path(input_dir), textProcessor)
    
    # Acá no se guarda nada, solo se usa el índice directamente
    return ejecutar_query(textProcessor.json_data, query)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Indexador con PyTerrier.")
    parser.add_argument("input_dir", type=str, help="Directorio raíz con los archivos HTML")
    args = parser.parse_args()

    print("Indexando documentos...")
    textProcessor = TextProcessor()
    directory_dfs(Path(args.input_dir), textProcessor)

    print("¡Indexación completada! Ahora podés ingresar consultas (escribí 'salir' para terminar).")
    while True:
        query = input("\nIngrese una consulta: ")
        if query.lower() in ("salir", "exit", "quit"):
            break

        resultados = ejecutar_query(textProcessor.json_data, query)

        print("Ranking de documentos:")
        for doc_id, score in resultados:
            print(f"Doc {doc_id}: {score:.4f}")

