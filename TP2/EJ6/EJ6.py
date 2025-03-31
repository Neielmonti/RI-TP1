import os
import sys
from scipy.stats import pearsonr


class LanguajeAnalicer:
    def __init__(self, training_folder, test_file, count_pairs):
        self.freq_lists = []
        self.training_folder = training_folder

        self.train(self.training_folder, count_pairs)
        self.analiseDoc(test_file, count_pairs)

    def train(self, folder, count_pairs):
        for file in sorted(os.listdir(folder)):
            file_path = os.path.join(folder, file)
            with open(file_path, "r", encoding="iso-8859-1") as f:
                freq_list = []
                for line in f:
                    self.accounting(line, freq_list, count_pairs)
                freq_list = self.normalize_list(freq_list)
                freq_list = sorted(freq_list, key=lambda x: x[1], reverse=True)
                self.freq_lists.append({'languaje': file, "freqs": freq_list})

    def normalize_list(self, list):
        if not list:
            return []
        max_freq = max(list, key=lambda x: x[1])
        min_freq = min(list, key=lambda x: x[1])
        denominator = max_freq[1] - min_freq[1]
        if denominator == 0:
            return [[element[0], 0.5] for element in list]
        return [
            [element[0], (
                element[1] - min_freq[1]
                ) / denominator] for element in list
            ]

    def analiseDoc(self, file_path, count_pairs):
        with open(file_path, "r", encoding="iso-8859-1") as f:
            output_list = []
            for line in f:
                line_freq_list = []
                self.accounting(line, line_freq_list, count_pairs)
                line_freq_list = self.normalize_list(line_freq_list)
                line_freq_list = sorted(
                    line_freq_list, key=lambda x: x[1], reverse=True
                    )
                correlation_scores = []
                for item in self.freq_lists:
                    all_chars = set(
                        car for car, _ in line_freq_list
                        ) | set(car for car, _ in item["freqs"])
                    dict1 = dict(line_freq_list)
                    dict2 = dict(item["freqs"])
                    complete_line_freq_list = [
                        [car, dict1.get(car, 0)] for car in all_chars
                        ]
                    complete_item_freq_list = [
                        [car, dict2.get(car, 0)] for car in all_chars
                        ]
                    correlation_scores.append({
                        'languaje': item["languaje"],
                        "score": self.compare_frequencies_pearson(
                            complete_line_freq_list, complete_item_freq_list
                        )
                    })
                output_list.append(
                    max(correlation_scores, key=lambda x: x["score"])
                    ["languaje"]
                    )
            with open("salida.txt", "w", encoding="iso-8859-1") as output:
                for i in range(0, len(output_list) - 1):
                    output.write(f"{i+1} {output_list[i]}\n")

    def compare_frequencies_pearson(self, freq_list1, freq_list2) -> float:
        vec1 = [x[1] for x in freq_list1]
        vec2 = [x[1] for x in freq_list2]
        pearson_score = pearsonr(vec1, vec2)[0]
        return pearson_score

    def accounting(self, text, freq_list, count_pairs=False):
        if count_pairs:
            self.count_pairs(text, freq_list)
        else:
            self.count_chars(text, freq_list)

    def count_chars(self, text, freq_list):
        for char in text:
            if char != ' ':
                for item in freq_list:
                    if item[0] == char:
                        item[1] += 1
                        break
                else:
                    freq_list.append([char, 1])

    def count_pairs(self, text, freq_list):
        for i in range(len(text) - 1):
            pair = text[i] + text[i + 1]
            if " " in pair:  # Evitamos contar pares con espacios
                continue
            for item in freq_list:
                if item[0] == pair:
                    item[1] += 1
                    break
            else:
                freq_list.append([pair, 1])


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print(
            "Uso: python script.py <ruta_de_la_carpeta_training> "
            "<ruta_al_archivo_test>"
        )
        sys.exit(1)

    training_folder = sys.argv[1]
    test_file = sys.argv[2]

    if not os.path.isdir(training_folder):
        print(
            f"Error: La carpeta '{training_folder}' no existe."
            )
        sys.exit(1)

    if not os.path.isfile(test_file):
        print(
            f"Error: El archivo '{test_file}' no existe."
            )
        sys.exit(1)

    analicer = LanguajeAnalicer(training_folder, test_file, True)
