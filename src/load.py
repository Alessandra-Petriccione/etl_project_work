import os
from src import processed, ROOT_DIR, raw
import pandas as pd
from dotenv import load_dotenv
import psycopg
import src.extract as e
import src.transform as t
from datetime import date, datetime
from decimal import Decimal
import numpy as np
from src.logging_config import logger
from tqdm import tqdm
from sys import stdout

load_dotenv()

class DatabaseConnection:
    _instance = None
    _connection = None

    def __new__(cls):
        if cls._instance is None:
            print("Nessuna istanza ancora: ne creo una nuova")
            cls._instance = super(DatabaseConnection, cls).__new__(cls)

            print("Apro la connessione al DB")
            cls._connection = psycopg.connect(
                host=os.getenv("dbhost"),
                dbname=os.getenv("dbname"),
                user=os.getenv("dbuser"),
                password=os.getenv("dbpassword"),
                port=os.getenv("dbport")
            )
        else:
            print("Istanza già esistente: la riuso")
        return cls._instance

    @property
    def connection(self):
        return self._connection

def load_customers(df):
    #columns = ["id_cliente","regione","provincia","CAP"]
    #df_customers = pd.read_csv(ROOT_DIR/ processed/ f_name, usecols= columns)
    #df_customers["CAP"] = df_customers["CAP"].astype(str).str.zfill(5)
    #df_customers = df_customers[columns]

    db = DatabaseConnection()
    with db.connection.cursor() as cur:
        sql= """
        CREATE TABLE IF NOT EXISTS clienti(
        id_cliente character(32) PRIMARY KEY,
        regione character varying(200),
        provincia character varying(200),
        "CAP" character(5) NOT NULL CHECK ("CAP"~ '^[0-9]{5}$')
        );
        """
        cur.execute(sql)
        sql= """
        INSERT INTO clienti(id_cliente, regione, provincia, "CAP") VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
        """
        for index, row in tqdm(df.iterrows(),
                    total=len(df.index),
                    colour="magenta",
                    file=stdout,
                    bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt}"):
            cur.execute(sql, row.to_list())
            #print("sto eseguendo il record di indice: ", index)
        db.connection.commit()

def load_sellers(df):
    #columns = ["id_venditore", "regione"]
    #df_sellers = pd.read_csv(ROOT_DIR / processed / f_name, usecols=columns)
    #df_sellers = df_customers[columns]

    db = DatabaseConnection()
    with db.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS venditori(
        id_venditore character(32) PRIMARY KEY,
        regione character varying(200)
        );
        """
        cur.execute(sql)
        sql = """
        INSERT INTO venditori(id_venditore, regione) VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """
        for index, row in tqdm(df.iterrows(),
                    total=len(df.index),
                    colour="magenta",
                    file=stdout,
                    bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt}"):
            cur.execute(sql, row.to_list())
            #print("sto eseguendo il record di indice: ", index)
        db.connection.commit()

def load_categories(df):
    #columns = ["id","nome_eng_categoria","nome_ita_categoria"]
    #df_categories = pd.read_csv(ROOT_DIR / processed / f_name, usecols=columns)
    #df_categories = df_customers[columns]

    db = DatabaseConnection()
    with db.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS categorie(
        id_categoria serial PRIMARY KEY,
        nome_eng_categoria character varying(200) UNIQUE,
        nome_ita_categoria character varying(200) UNIQUE
        );
        """
        cur.execute(sql)
        sql = """
        INSERT INTO categorie("nome_eng_categoria", "nome_ita_categoria") VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
        """
        for index, row in tqdm(df.iterrows(),
                    total=len(df.index),
                    colour="magenta",
                    file=stdout,
                    bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt}"):
            cur.execute(sql, row.to_list())
            #print("sto eseguendo il record di indice: ", index)
        db.connection.commit()

def load_products(df):
    #columns = ["id_prodotto","nome_ita_categoria","lunghezza_nome","lunghezza_descrizione","numero_foto"]
    #df_products = pd.read_csv(ROOT_DIR / processed / f_name, usecols=columns)
    #df_products = df_products[columns]

    db = DatabaseConnection()
    with db.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS prodotti(
        id_prodotto character(32) PRIMARY KEY,
        fk_nome_ita_categoria character varying(200) NOT NULL,
        lunghezza_nome integer,
        lunghezza_descrizione integer,
        numero_foto integer, 
        FOREIGN KEY (fk_nome_ita_categoria) REFERENCES public.categorie (nome_ita_categoria)
        );
        """
        cur.execute(sql)
        sql = """
        INSERT INTO prodotti("id_prodotto","fk_nome_ita_categoria","lunghezza_nome","lunghezza_descrizione","numero_foto")
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        for index, row in tqdm(df.iterrows(),
                    total=len(df.index),
                    colour="magenta",
                    file=stdout,
                    bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt}"):
            cur.execute(sql, row.to_list())
            #print("sto eseguendo il record di indice: ", index)
        db.connection.commit()

def load_orders(df):
    #columns = ["id_ordine","id_cliente","stato_ordine","data_e_ora_acquisto","data_e_ora_consegna","data_di_consegna_stimata"]
    #df_orders = pd.read_csv(ROOT_DIR / processed / f_name, usecols=columns)
    #df_orders["data_e_ora_acquisto"] = pd.to_datetime(df_orders["data_e_ora_acquisto"])
    #df_orders["data_e_ora_consegna"] = pd.to_datetime(df_orders["data_e_ora_consegna"])
    #df_orders["data_di_consegna_stimata"] = pd.to_datetime(df_orders["data_di_consegna_stimata"]).dt.date
    #df_orders = df_orders[columns]
    df = df.replace({np.nan: None})
    db = DatabaseConnection()
    with db.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS ordini(
        id_ordine character(32) PRIMARY KEY,
        fk_id_cliente character (32) NOT NULL,
        stato_ordine character varying(200) NOT NULL,
        data_e_ora_acquisto timestamp NOT NULL,
        data_e_ora_consegna timestamp CHECK (data_e_ora_consegna > data_e_ora_acquisto),
        data_di_consegna_stimata date CHECK (data_di_consegna_stimata >= DATE (data_e_ora_acquisto)),
        FOREIGN KEY (fk_id_cliente) REFERENCES public.clienti (id_cliente)
        );
        """
        cur.execute(sql)
        sql = """
        INSERT INTO ordini("id_ordine","fk_id_cliente","stato_ordine","data_e_ora_acquisto","data_e_ora_consegna","data_di_consegna_stimata")
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        for index, row in tqdm(df.iterrows(),
                    total=len(df.index),
                    colour="magenta",
                    file=stdout,
                    bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt}"):
            cur.execute(sql, row.to_list())
            #print("sto eseguendo il record di indice: ", index)
        db.connection.commit()

def load_items(df):
    #columns = ["id","id_ordine","numero_oggetti","id_prodotto","id_venditore","prezzo","prezzo_spedizione"]
    #df_items = pd.read_csv(ROOT_DIR / processed / f_name, usecols=columns)
    #df_items = df_items[columns]

    db = DatabaseConnection()
    with db.connection.cursor() as cur:
        sql = """
            CREATE TABLE IF NOT EXISTS articoli(
            id_articolo serial PRIMARY KEY,
            fk_id_ordine character(32) NOT NULL ,
            numero_oggetti integer NOT NULL CHECK(numero_oggetti> 0),
            fk_id_prodotto character(32) NOT NULL,
            fk_id_venditore character(32) NOT NULL,
            prezzo numeric(10,2) CHECK(prezzo>= 0),
            prezzo_spedizione numeric(10,2) CHECK(prezzo_spedizione>= 0),
            FOREIGN KEY (fk_id_ordine) REFERENCES public.ordini (id_ordine),
            FOREIGN KEY (fk_id_prodotto) REFERENCES public.prodotti (id_prodotto),
            FOREIGN KEY (fk_id_venditore) REFERENCES public.venditori (id_venditore),
            CONSTRAINT uc_articoli UNIQUE (fk_id_ordine, numero_oggetti, fk_id_prodotto, fk_id_venditore)
            );
            """
        cur.execute(sql)
        sql = """
            INSERT INTO articoli("fk_id_ordine","numero_oggetti","fk_id_prodotto","fk_id_venditore","prezzo","prezzo_spedizione")
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """
        for index, row in tqdm(df.iterrows(),
                    total=len(df.index),
                    colour="magenta",
                    file=stdout,
                    bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt}"):
            cur.execute(sql, row.to_list())
            #print("sto eseguendo il record di indice: ", index)
        db.connection.commit()


TRANSFORMERS = {
    "customers" : t.transform_customers,
    "sellers" : t.transform_sellers,
    "categories" : t.transform_categories,
    "products" : t.transform_products,
    "orders" : t.transform_orders,
    "items" : t.transform_items
}
LOADERS = {
    "customers": load_customers,
    "sellers": load_sellers,
    "categories": load_categories,
    "products": load_products,
    "orders": load_orders,
    "items": load_items
}

def etl_completa():
    lista_file = [f for f in os.listdir(raw)]

    diz_file = {k: [] for k in TRANSFORMERS.keys()}
    for f in lista_file:
        for k in TRANSFORMERS.keys():
            if k in f:
                diz_file[k].append(f)

    for k, file_list in diz_file.items():
        dfs = []
        for f in file_list:
            try:
                df = e.extract_csv(f, raw, ",")
                dfs.append(df)
                logger.info(f"File {f} caricato per {k}")
                print(f"File {f} caricato per {k}")
            except Exception as ex:
                logger.error(f"Errore caricando file {f} ({k}): {ex}")
                print(f"Errore caricando file {f} ({k}): {ex}")
                continue

        df_concat = pd.concat(dfs, ignore_index=True)
        transformer = TRANSFORMERS.get(k)
        if transformer:
            try:
                df_concat = transformer(df_concat)
                logger.info(f"Trasformazione completata per {k}")
                print(f"\nTrasformazione completata per {k}")
            except Exception as ex:
                logger.error(f"Errore nella trasformazione di {k}: {ex}")
                print(f"\nErrore nella trasformazione di {k}: {ex}")
                continue
        else:
            logger.warning(f"Nessuna trasformazione definita per {k}")
            continue

        loader = LOADERS.get(k)
        if loader:
            try:
                loader(df_concat)
                logger.info(f"Caricati {len(df_concat)} record di {k}")
                print(f"\nCaricati {len(df_concat)} record di {k}\n")
            except Exception as ex:
                logger.error(f"Errore nel caricamento di {k}: {ex}")
                print(f"\nErrore nel caricamento di {k}: {ex}")
        else:
            logger.warning(f"Nessun loader definito per {k}")

def visualizzare_tabelle():
    n_tabella = ""
    db = DatabaseConnection()
    with db.connection.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cur.fetchall()]

    db.connection.commit()

    mapping = {str(i + 1): t for i, t in enumerate(tables)}
    while True:
        print("\n---Scegli una tabella---")
        for k, v in mapping.items():
            print(f"{k} - {v}")
        print("0 - esci dal programma")

        tab = input("> ").strip()

        if tab in mapping:
            n_tabella = mapping[tab]
            break
        elif tab == "0":
            break
        else:
            print("Scelta non valida, riprova.")

    with db.connection.cursor() as cur:
        sql = f"""
                SELECT * FROM {n_tabella}
               """
        cur.execute(sql)
        for record in cur:
            clean_record = tuple(
                val.strftime("%Y-%m-%d") if isinstance(val, date) and not isinstance(val, datetime)
                else val.strftime("%Y-%m-%d %H:%M:%S") if isinstance(val, datetime)
                else f"{val:.2f}" if isinstance(val, Decimal)
                else val
                for val in record
            )
            print(clean_record)
    db.connection.commit()

def etl_singola():
    file_name = e.check_files(raw)
    df = e.extract_csv(file_name, raw, ",")

    transformer = None
    loader = None

    for key, funz in TRANSFORMERS.items():
        if key in file_name:
            transformer = funz
            break

    for key, funz in LOADERS.items():
        if key in file_name:
            loader = funz
            break

    if transformer:
        df = transformer(df)
    else:
        print(f"Nessuna trasformazione definita per {file_name}")
        return

    if loader:
        loader(df)
    else:
        print(f"Nessun loader definito per {file_name}")

def loader_singolo():
    file_name = e.check_files(processed)
    df = e.extract_csv(file_name, processed, ",")
    loader = None
    for key, funz in LOADERS.items():
        if key in file_name:
            loader = funz
            break
    if loader:
        loader(df)
        print(f"il file {file_name} è stato caricato correttamente nel database!")
    else:
        print(f"Nessun loader definito per {file_name}")

if __name__ == "__main__":
    db1 = DatabaseConnection()
    print("Connessione 1:", db1.connection)
    #print()
    #db2 = DatabaseConnection()
    #print("Connessione 2:", db2.connection)
    #with db1.cursor() as cur:
    print("modulo load")