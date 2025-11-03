import pandas as pd
from src import extract as e, ROOT_DIR, wip, processed, raw
from datetime import datetime
pd.set_option("display.max.rows", None)
pd.set_option("display.max.columns", None)
mappa_categorie = {
        "GIOCATTOLI": ["giocattoli"],
        "MOBILI": ["mobili_decorazione", "mobili_ufficio", "mobili_materassi_imbottiti", "mobili_soggiorno", "mobili_camera_da_letto",
                   "mobili_cucina_lavanderia_sala_giardino"],
        "ARTICOLI PER LA CASA": ["articoli_per_la_casa", "letto_bagno_tavola", "climatizzazione", "casa_comfort",
                                 "piccoli_elettrodomestici_casa_forno_caffe"],
        "ALIMENTARI": ["alimenti", "cibi_bevande", "la_cuisine", "bevande"],
        "MODA": ["moda_borse_accessori", "moda_abbigliamento_donna", "moda_abbigliamento_uomo", "moda_calzature", "moda_abbigliamento_bambini",
                 "moda_sport", "intimo_costumi_da_bagno"],
        "ANIMALI": ["negozio_animali"],
        "ELETTRONICA": ["telefonia", "telefonia_fissa", "informatica_accessori", "elettronica",
                        "computer", "dvd_blu_ray", "elettrodomestici", "tablet_stampa_immagine", "piccoli_elettrodomestici",
                        "elettrodomestici_2"],
        "SALUTE E BELLEZZA": ["salute_bellezza", "profumeria", "pannolini_igiene"],
        "SPORT E TEMPO LIBERO": ["sport_tempo_libero"],
        "AUTOMOBILE": ["auto"],
        "LIBRI": ["libri_interesse_generale", "libri_tecnici", "libri_importati"],
        "IDEE REGALO": ["articoli_cool", "orologi_regali"],
        "PRODOTTI INDUSTRIALI": ["industria_commercio_affari", "agroindustria_e_commercio"],
        "GIARDINAGGIO": ["attrezzi_da_giardino", "fiori", ],
        "ARTICOLI PER L'INFANZIA": ["neonati"],
        "ARTICOLI PER FESTE": ["articoli_natalizi", "articoli_per_feste"],
        "MUSICA": ["strumenti_musicali", "audio", "cd_dvd_musicali", "musica"],
        "CARTOLERIA": ["cartoleria"],
        "SICUREZZA": ["sicurezza_e_servizi", "segnaletica_sicurezza"],
        "EDILIZIA": ["casa_costruzione", "attrezzi_da_giardino_costruzione", "costruzione_attrezzi_edili",
                     "attrezzi_da_costruzione", "illuminazione_costruzione"],
        "ARTE": ["arte", "arti_e_artigianato"],
        "VIAGGI": ["valigie_accessori"],
        "CATEGORIA NON DISPONIBILE": ["nan","category unavailable","categoria non disponibile"]
    }
mappa_eng = {
        "toys": ["toys"],
        "furniture": ["furniture_decor", "office_furniture", "furniture_mattress_and_upholstery", "furniture_living_room",
                      "furniture_bedroom", "kitchen_dining_laundry_garden_furniture"],
        "housewares": ["housewares", "bed_bath_table", "air_conditioning", "home_confort", "home_confort_2",
                        "small_appliances", "small_appliances_home_oven_and_coffee"],
        "food": ["food", "drinks", "food_drink", "la_cuisine"],
        "fashion": ["fashion_bags_accessories", "fashion_male_clothing", "fashio_female_clothing", "fashion_shoes",
                    "fashion_childrens_clothes", "fashion_sport", "fashion_underwear_beach"],
        "pets": ["pet_shop"],
        "electronics": ["telephony", "fixed_telephony", "computers_accessories", "electronics", "computers",
                        "dvds_blu_ray", "home_appliances", "tablets_printing_image", "small_appliances",
                        "home_appliances_2", 'pc_gamer',"video_photo"],
        "health and beauty": ["health_beauty", "perfumery", "diapers_and_hygiene"],
        "sports and leisure": ["sports_leisure"],
        "automotive": ["auto"],
        "books": ["books_general_interest", "books_technical", "books_imported"],
        "gift ideas": ["cool_stuff", "watches_gifts"],
        "industrial products": ["industry_commerce_and_business", "agro_industry_and_commerce"],
        "gardening": ["garden_tools", "flowers"],
        "baby products": ["baby"],
        "party supplies": ["christmas_supplies", "party_supplies"],
        "music": ["musical_instruments", "audio", "cds_dvds_musicals", "music"],
        "stationery": ["stationery"],
        "security": ["security_and_services", "signaling_and_security"],
        "construction": ["home_construction", "costruction_tools_garden", "costruction_tools_tools", "construction_tools_construction", "construction_tools_lights"],
        "art": ["art", "arts_and_craftmanship"],
        "travel": ["luggage_accessories"],
        "category unavailable": ["nan","category unavailable","categoria non disponibile"]
    }

def inv_mappa(diz):
    return {sub: m for m, sub_list in diz.items() for sub in sub_list}


def save_csv(df, destination_path):
   temp= input("Inserisci il nome del file: ")
   cur_date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
   file_name = temp + "_" + cur_date_time + ".csv" #questi due comandi servono per salvare il file con
   #all'interno del nome anche il giorno e l'ora.
   df.to_csv(ROOT_DIR / destination_path / file_name, encoding="utf-8", index= False)

   print(f"il file {file_name} è stato creato nella cartella {destination_path}")

def save_items(df,path):
    cur_date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    temp = input("Inserisci il nome del file: ")
    f_name = temp + cur_date_time + ".csv"
    df.to_csv(ROOT_DIR / path / f_name, encoding="utf-8", index_label="id",float_format="%.2f")
    print(f"il file {f_name} è stato creato nella cartella {path}")

def transform_customers(df):
    df.rename(columns={
        "customer_id": "id_cliente",
        "region": "regione",
        "city": "provincia",
        "cap": "CAP"
    }, inplace= True)
    save_csv(df, wip)
    df.drop_duplicates(inplace= True)
    df.drop_duplicates(subset= "id_cliente", inplace=True)
    df.reset_index(drop= True, inplace= True)
    df["CAP"] = df["CAP"].astype(str).str.zfill(5)
    save_csv(df, processed)
    return df

def transform_sellers(df):
    df.rename(columns={
        "seller_id" : "id_venditore",
        "region" : "regione"
    }, inplace= True)
    save_csv(df, wip)
    df.drop_duplicates(subset= "id_venditore", inplace=True)
    df.reset_index(drop= True, inplace= True)
    save_csv(df, processed)
    return df

def transform_categories(df):
    df.rename(columns={
        "product_category_name_english": "nome_eng_categoria",
        "product_category_name_italian": "nome_ita_categoria"
    }, inplace=True)
    m_ita = inv_mappa(mappa_categorie)
    df["nome_ita_categoria"] = df["nome_ita_categoria"].map(m_ita).astype("str").str.lower()
    m_eng = inv_mappa(mappa_eng)
    df["nome_eng_categoria"] = df["nome_eng_categoria"].map(m_eng).astype("str").str.lower()
    nuova_riga_df = pd.DataFrame([{"nome_eng_categoria": "category unavaible",
                                   "nome_ita_categoria": "categoria non disponibile"}], index=[len(df)])
    df = pd.concat([df, nuova_riga_df])
    save_csv(df, wip)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    save_csv(df, processed)
    return df

def transform_products(df):
    df.rename(columns={
        "product_id" : "id_prodotto",
        "category" : "nome_ita_categoria",
        "product_name_length" : "lunghezza_nome",
        "product_description_length" : "lunghezza_descrizione",
        "product_photos_qty" : "numero_foto"
    }, inplace = True)
    save_csv(df, wip)
    df.drop_duplicates(subset="id_prodotto", inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.fillna({"nome_ita_categoria" : "categoria non disponibile","lunghezza_nome": 0,
           "lunghezza_descrizione": 0, "numero_foto": 0}, inplace= True)
    df = df.astype({"lunghezza_nome": "int", "lunghezza_descrizione": "int", "numero_foto": "int"})
    italian_keys = list(mappa_categorie.keys())
    english_keys = list(mappa_eng.keys())
    for i in range(len(italian_keys)):
        eng_k = english_keys[i]
        ita_k = italian_keys[i]
        mappa_eng[ita_k] = mappa_eng.pop(eng_k)

    m_eng = inv_mappa(mappa_eng)
    df["nome_ita_categoria"] = df["nome_ita_categoria"].map(m_eng).astype("str").str.lower()
    save_csv(df, processed)
    return df

def transform_orders(df):
    df.rename(columns={
        "order_id": "id_ordine",
        "customer_id": "id_cliente",
        "order_status": "stato_ordine",
        "order_purchase_timestamp": "data_e_ora_acquisto",
        "order_delivered_customer_date": "data_e_ora_consegna",
        "order_estimated_delivery_date": "data_di_consegna_stimata"
    }, inplace= True)

    map_ordini = {
        "delivered": "consegnato",
        "invoiced": "fatturato",
        "canceled": "cancellato",
        "shipped": "spedito",
        "unavailable": "non disponibile",
        "processing": "in elaborazione",
        "approved": "approvato",
        "created": "creato"
    }
    df["stato_ordine"] = df["stato_ordine"].map(map_ordini)
    #print(df["data_e_ora_consegna"].info())
    df["data_e_ora_acquisto"] = pd.to_datetime(df["data_e_ora_acquisto"])
    df["data_e_ora_consegna"] = pd.to_datetime(df["data_e_ora_consegna"])
    df["data_di_consegna_stimata"] = pd.to_datetime(df["data_di_consegna_stimata"]).dt.date
    save_csv(df, wip)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop= True, inplace=True)
    save_csv(df, processed)
    return df

def transform_items(df):
    df.rename(columns={
        "order_id": "id_ordine",
        "order_item": "numero_oggetti",
        "product_id": "id_prodotto",
        "seller_id": "id_venditore",
        "price":"prezzo",
        "freight": "prezzo_spedizione"
    }, inplace=True)
    #df = df.astype({"numero_oggetti": "int"})
    #df["numero_oggetti"] = df["numero_oggetti"].astype(int)
    #print(df)
    #df.info()
    save_items(df, wip)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    save_items(df, processed)
    return df

if __name__ == "__main__":
    print("modulo transform")


