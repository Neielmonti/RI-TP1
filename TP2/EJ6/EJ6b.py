import sys
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0

def detect_languages(input_file):
    results = {}

    with open(input_file, "r", encoding="iso-8859-1") as f:
        lines = f.readlines()

    for i, line in enumerate(lines, 1):
        try:
            language = detect(line.strip())
        except:
            language = "unknown"
        results[i] = language

    with open("langdetect.txt", "w", encoding="iso-8859-1") as txt_file:
        for key, value in results.items():
            txt_file.write(f"{key} {value}\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python EJ6b.py <archivo_de_prueba>")
        sys.exit(1)

    test_file = sys.argv[1]

    detect_languages(test_file)