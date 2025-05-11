import argparse

class ZipfAnalyzer:
    def __init__(self, file_path):
        self.file_path = file_path
        self.terms = []
        self.frequencies = []
        self.load_data()
    
    def load_data(self):
        with open(self.file_path, "r", encoding="utf-8") as file:
            for line in file:
                term, freq = line.strip().split()
                self.terms.append(term)
                self.frequencies.append(int(freq))
        
    def estimate_zipf_distribution(self, percentage):
        # Esta variable la ajusté yo a mano para la lista de frecuencias "zipf_example.txt"

        variable = 0.119

        total_vocab = len(self.terms)
        top_n = int(total_vocab * (percentage / 100))
        max_frequency = self.frequencies[0]
        
        # Estimación usando la ley de Zipf
        estimated_terms = sum(max_frequency / (r + variable) for r in range(top_n))
        
        return estimated_terms
    
    def compare_real_vs_estimated(self):
        percentages = [10, 20, 30]
        results = {}
        
        for p in percentages:
            estimated = self.estimate_zipf_distribution(p)
            real = sum(self.frequencies[:int(len(self.terms) * (p / 100))])
            results[p] = {"estimado": round(estimated), "real": real}
            print(f"Porcentaje: {p}, Resultados: {results[p]}")
        
        return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Análisis Zipf del vocabulario")
    parser.add_argument("zipf_file", help="Ruta al archivo que contiene las frecuencias")
    
    args = parser.parse_args()
    analyzer = ZipfAnalyzer(args.zipf_file)
    analyzer.compare_real_vs_estimated()
