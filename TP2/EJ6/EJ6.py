
import os
import sys

class LanguajeAnalicer:
    def __init__(self, training_folder):
        self.freq_lists = []

        self.training_folder = training_folder
        
        self.train(self.training_folder)
        for item in self.freq_lists:
            print("Lenguaje: ", item["languaje"])
            print("Frecuencias: ", item["freqs"])
    
    def train(self, folder):
        for file in sorted(os.listdir(folder)):
            file_path = os.path.join(folder, file)
            with open(file_path, "r", encoding="iso-8859-1") as f:
                freq_list = []
                for line in f:
                    self.count_chars(line, freq_list)
                freq_list = self.normalize_list(freq_list)
                freq_list = sorted(freq_list, key=lambda x: x[1], reverse=True)
                self.freq_lists.append({'languaje': file, "freqs": freq_list})

    def normalize_list(self, list):
        max_freq = max(list, key=lambda x: x[1])
        min_freq = min(list, key=lambda x: x[1])
        denominator = max_freq[1] - min_freq[1]
        if denominator == 0:
            return [[element[0], 0.5] for element in list]
        for element in list:
            norm = (element[1] - min_freq[1]) / denominator
            element[1] = norm
        return list

    def count_chars(self, text, freq_list):
        for char in text:
            if char != ' ':  # Evita almacenar espacios
                for item in freq_list:
                    if item[0] == char:
                        item[1] += 1
                        break
                else:
                    freq_list.append([char, 1])

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Uso: python script.py <ruta_de_la_carpeta_training>")
        sys.exit(1)

    training_folder = sys.argv[1]

    if not os.path.isdir(training_folder):
        print(f"Error: El folder '{training_folder}' no existe.")
        sys.exit(1)

    analicer = LanguajeAnalicer(training_folder)