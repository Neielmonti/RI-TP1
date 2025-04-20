from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import argparse
import shutil
from pathlib import Path
from EJ4 import TextProcessor

parser = argparse.ArgumentParser(description="Indexador con PyTerrier.")
parser.add_argument("input_dir", type=str, help="Directorio raíz con los archivos HTML")
args = parser.parse_args()

file_index = 0

textProcessor = TextProcessor()

def directory_dfs(path: Path) -> None:
    global file_index
    for x in path.iterdir():
        if x.is_dir():
            directory_dfs(x)
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
    textProcessor.saveData()

directory_dfs(Path(args.input_dir))

