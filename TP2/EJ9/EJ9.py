import re
import argparse
import matplotlib.pyplot as plt
from collections import Counter
import numpy as np
from scipy.optimize import curve_fit

class DocumentProcessor:
    def __init__(self, corpus_file):
        self.pattern_word = r"\b\w+\b"
        self.extracted_terms_counts = Counter()
        self.total_terms = []
        self.unique_terms = []
        self.process_corpus(corpus_file)
        self.fit_heaps_law()

    def process_corpus(self, corpus_file):
        with open(corpus_file, "r", encoding="utf-8") as file:
            total_terms = 0
            
            for line in file:
                matches = re.findall(self.pattern_word, line.lower())
                
                for term in matches:
                    self.extracted_terms_counts[term] += 1
                    total_terms += 1
                    self.total_terms.append(total_terms)
                    self.unique_terms.append(len(self.extracted_terms_counts))

    def heaps_function(self, N, K, beta):
        return K * (N ** beta)

    def fit_heaps_law(self):
        params = curve_fit(self.heaps_function, self.total_terms, self.unique_terms, maxfev=10000)[0]
        K = params[0]
        beta = params[1]

        # Estas asignaciones las dejé para jugar :)

        # K-MEJOR-AJUSTE = 4.16
        # BETA-MEJOR-AJUSTE = 0.67

        K = 4.16
        beta = 0.6
        
        # Calculo de valores ajustados
        adjusted_values = self.heaps_function(np.array(self.total_terms), K, beta)
        
        # Coeficiente de determinación
        residuals = np.array(self.unique_terms) - adjusted_values
        ss_res = np.sum(residuals**2)
        ss_tot = np.sum((np.array(self.unique_terms) - np.mean(self.unique_terms))**2)
        r2 = 1 - (ss_res / ss_tot)
        
        # Mostrar los resultados
        plt.figure(figsize=(10, 5))
        plt.plot(self.total_terms, self.unique_terms, marker="o", linestyle="-", markersize=3, label="Datos reales")
        plt.plot(self.total_terms, adjusted_values, linestyle="--", label=f"Ajuste Heaps (K={K:.2f}, beta={beta:.2f}, R²={r2:.4f})")
        plt.xlabel("Numero total de terminos procesados")
        plt.ylabel("Numero de terminos unicos")
        plt.title("Verificacion de la Ley de Heaps")
        plt.legend()
        plt.grid(True)
        plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parametros del programa")
    parser.add_argument("corpus_file", help="Ruta al archivo corpus")
    args = parser.parse_args()
    processor = DocumentProcessor(args.corpus_file)

