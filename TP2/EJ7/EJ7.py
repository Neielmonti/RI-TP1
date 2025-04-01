import re
import numpy as np
import argparse
import matplotlib.pyplot as plt
from collections import Counter

class DocumentProcessor:
    def __init__(self, corpus_file, stop_words_folder=None):
        self.stop_words_file = stop_words_folder
        self.check_for_stop_words = False
        self.stopWords = []

        if stop_words_folder:
            self.check_for_stop_words = True
            self.loadStopWords()

        self.pattern_word = r"\b\w+\b"
        self.openFilesFromFolder(corpus_file)

    def loadStopWords(self):
        with open(self.stop_words_file, "r", encoding="iso-8859-1") as file:
            for line in file:
                self.stopWords.extend(line.strip().lower().split())

    def openFilesFromFolder(self, corpus_file):
        with open(corpus_file, "r", encoding="iso-8859-1") as file:
            extracted_terms = []
            for line in file:
                matches = re.findall(self.pattern_word, line.lower())
                extracted_terms.extend(matches)

            self.extracted_terms_counts = Counter(extracted_terms)
        
    def fit_and_plot(self):
        # Obtener las frecuencias ordenadas de mayor a menor
        sorted_counts = sorted(self.extracted_terms_counts.values(), reverse=True)
        ranks = np.arange(1, len(sorted_counts) + 1)
        
        # Ajuste de la curva con polyfit en escala logarítmica
        log_ranks = np.log(ranks)
        log_freqs = np.log(sorted_counts)
        coefficients = np.polyfit(log_ranks, log_freqs, 1)
        poly = np.poly1d(coefficients)
        estimated_freqs = np.exp(poly(log_ranks))

        plt.figure(figsize=(10, 5))
        plt.plot(ranks, sorted_counts, label="Datos originales", marker="o")
        plt.plot(ranks, estimated_freqs, label="Ajuste (Polyfit)", linestyle="--")
        plt.xlabel("Rango")
        plt.ylabel("Frecuencia")
        plt.title("Distribución de frecuencia de palabras (Escala Lineal)")
        plt.legend()
        plt.grid()

        plt.figure(figsize=(10, 5))
        plt.loglog(ranks, sorted_counts, label="Datos originales", marker="o")
        plt.loglog(ranks, estimated_freqs, label="Ajuste (Polyfit)", linestyle="--")
        plt.xlabel("Log Rango")
        plt.ylabel("Log Frecuencia")
        plt.title("Distribución de frecuencia de palabras (Escala Log-Log)")
        plt.legend()
        plt.grid()
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parámetros del programa")
    parser.add_argument("corpus_file", help="Ruta al archivo que contiene los documentos")
    parser.add_argument(
        "stop_words_file", nargs="?", default=None,
        help="Ruta al archivo que contiene las palabras vacías (opcional)"
    )
    args = parser.parse_args()
    processor = DocumentProcessor(args.corpus_file, args.stop_words_file)
    processor.fit_and_plot()
