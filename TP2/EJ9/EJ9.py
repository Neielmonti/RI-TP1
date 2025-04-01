import re
import argparse
import matplotlib.pyplot as plt
from collections import Counter
import matplotlib.pyplot as plt

class DocumentProcessor:
    def __init__(self, corpus_file):
        self.check_for_stop_words = False
        self.pattern_word = r"\b\w+\b"
        self.extracted_terms_counts = Counter()
        self.process_corpus(corpus_file)
        self.show_values()

    def process_corpus(self, corpus_file):
        with open(corpus_file, "r", encoding="iso-8859-1") as file, open("term_statistics.txt", "w", encoding="utf-8") as stat_file:
            total_terms = 0

            for line in file:
                matches = re.findall(self.pattern_word, line.lower())

                for term in matches:
                    self.extracted_terms_counts[term] += 1
                    total_terms += 1
                    unique_terms = len(self.extracted_terms_counts)
                    stat_file.write(f"{total_terms}, {unique_terms}\n")

    def show_values(self):
        file_path = "term_statistics.txt"
        total_terms = []
        unique_terms = []
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                total, unique = map(int, line.strip().split(", "))
                total_terms.append(total)
                unique_terms.append(unique)
        plt.figure(figsize=(10, 5))
        plt.plot(total_terms, unique_terms, marker="o", linestyle="-", markersize=3, label="Evolución de términos únicos")
        plt.xlabel("Número total de términos procesados")
        plt.ylabel("Número de términos únicos")
        plt.title("Cumplimiento de la Ley de Heaps")
        plt.legend()
        plt.grid(True)
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parámetros del programa")
    parser.add_argument("corpus_file", help="Ruta al archivo que contiene los documentos")
    args = parser.parse_args()
    processor = DocumentProcessor(args.corpus_file)
