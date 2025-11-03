import src.extract as e
import src.transform as t
import src.load as l
import pandas as pd
from src import raw, wip, processed
pd.set_option("display.max.rows", None)
pd.set_option("display.max.columns", None)

content = ""
flag = True
path = ""
f_name = ""
def menu():
    while flag:
        scelta= input("\n1- estrarre e visualizzare contenuto di un file"
                      "\n2- svolgimento etl completa"
                      "\n3- svolgimento etl per singolo file"
                      "\n4- visualizza contenuto di una tabella"
                      "\n5- caricamento di un file nel database"
                      "\n0- esci dal programma\n"
                      "\nSeleziona l'opzione desiderata:").strip()
        try:
            if scelta == "1":
                path = e.check_path()
                file = e.check_files(path)
                if file != "" and file is not None:
                    df = e.extract_csv(file, path, ",")
                    print(e.process_with_progress(df))
                    print(df.head(10))
            elif scelta == "2":
                l.etl_completa()
            elif scelta == "3":
                l.etl_singola()
            elif scelta == "4":
                l.visualizzare_tabelle()
            elif scelta == "5":
                l.loader_singolo()
            elif scelta == "0":
                print("\nUscita dal menu in corso.."
                      "Arrivederci!")
                break
            else:
                raise ValueError
        except ValueError as ex:
            print(ex)
            print("Opzione non valida. Inserire solo 1, 2, 3 o 4")

if __name__ == "__main__":
    print("Questo è il modulo main")
    menu()
