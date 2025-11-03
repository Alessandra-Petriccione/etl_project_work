import pandas as pd
from src import ROOT_DIR, raw
import os
from tqdm import tqdm
from sys import stdout
pd.set_option("display.max.rows", None)
pd.set_option("display.max.columns", None)

def extract_csv(file_name, path, sep):
    try:
        #with open(ROOT_DIR / path / file_name, "r") as file:
            #content = file.read()
        #return content
        df = pd.read_csv(ROOT_DIR / path / file_name, sep=sep)
        return df
    except FileNotFoundError as ex:
        print(ex)
        print("file non trovato")

def check_path():
    orig = ROOT_DIR / "data"
    obj = os.scandir(orig)
    val = 1
    diz = {}
    for entry in obj:
        if entry.is_dir():
            if any(os.scandir(entry.path)):
                print(val, "-", entry.name)
                diz[val] = entry.name
                val += 1
        # print(diz)
    n = int(input("\nDa quale cartella vuoi prendere il file? Premi 0 per uscire"))
    if n == 0: return None
    try:
        folder_name = diz[n]  # più semplice
        path = orig / folder_name
        return path
    except KeyError as ex:
        print("Nessuna cartella valida selezionata")
        return None

def check_files(path):
    obj = os.scandir(path)
    val = 1
    diz = {}
    for entry in obj:
        if entry.is_file():
            print(val, "-", entry.name)
            diz[entry.name] = val
            val += 1

    n = int(input("quale file vuoi aprire? Premi 0 per uscire"))
    try:
        txt =[key for key, val in diz.items() if val == n]
        f_name = txt[0]
        print(f_name)
        return f_name
    except (IndexError,ValueError) as ex:
        print("Nessun file valido selezionato")
        return None

def process_with_progress(df):
    for index, row in tqdm(df.iterrows(), total=len(df),colour="magenta", file=stdout,
                    bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt}"):
        pass
    return "completato"

if __name__ == "__main__":
    print("modulo extract")

