import ast
from datetime import datetime
from zoneinfo import ZoneInfo

from connect_db import connect_db

def get_current_timestamp():
    now_est = datetime.now(ZoneInfo("America/Toronto"))
    return now_est.strftime("%Y-%m-%d %H:%M:%S")

def get_pannes(data):
    pannes = data.get("pannes", [])
    return pannes

def get_entries(panne):
    coords = ast.literal_eval(panne[4])
    if panne[2] == '':
        panne[2] = None
    entries = {
        'villes': {
            'code_hq': panne[8]
        },
        'pannes': {
            'latitude': coords[0],
            'longitude': coords[1],
            'debut': panne[1],
            'code_hq': panne[8]
        },
        'statuts': {
            'statut': panne[5],
            'evaluation': panne[2],
            'cause': panne[6],
            'nb_clients': panne[0]
        }
    }
    return entries

def list_ongoing(cursor):
    cursor.execute("""
        SELECT id, latitude, longitude, debut
        FROM pannes
        WHERE fin IS NULL
    """)
    return cursor.fetchall()

def insert_data(conn, cursor, data, timestamp):
    insert_ville(cursor, data)
    conn.commit()
    id_panne = set_panne(cursor, data)
    conn.commit()
    insert_statut(cursor, data, id_panne, timestamp)
    conn.commit()

def insert_ville(cursor, entries):
    ville = entries['villes']

    cursor.execute("""
        SELECT id FROM villes WHERE code_hq = %s
    """, (ville['code_hq'],))
    result = cursor.fetchone()
    if result:
        print(f"Ville {ville['code_hq']} already exists in the database.")
        return
    
    cursor.execute("""
        INSERT INTO villes (code_hq)
        VALUES (%s)
    """, (ville['code_hq'],))

def check_ongoing(panne, ongoing):
    for id_panne, lat, lon, debut in ongoing:
        if (
            float(panne['latitude']) == float(lat) and
            float(panne['longitude']) == float(lon) and
            str(panne['debut']) == str(debut)
        ):
            return id_panne
    return None

def get_panne_id(cursor, panne):
    cursor.execute("""
        SELECT id FROM pannes
        WHERE
            ABS(latitude - %s) < 1e-8
            AND ABS(longitude - %s) < 1e-8
            AND debut = %s
    """, (panne['latitude'], panne['longitude'], panne['debut']))
    result = cursor.fetchone()
    return result[0] if result else None

def insert_panne(cursor, panne):
    code_hq = panne['code_hq']

    cursor.execute("""
        SELECT id FROM villes WHERE code_hq = %s
    """, (code_hq,))
    result = cursor.fetchone()

    if not result:
        print(f"Ville code_hq={code_hq} introuvable.")
        return

    cursor.execute("""
        INSERT INTO pannes (id_ville, latitude, longitude, debut)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, debut) DO NOTHING
    """, (result[0], panne['latitude'], panne['longitude'], panne['debut']))

def set_panne(cursor, entries):
    panne = entries['pannes']
    insert_panne(cursor, panne) 
    return get_panne_id(cursor, panne)  

def insert_statut(cursor, entries, id_panne, timestamp):
    statut = entries['statuts']
    cursor.execute("""
        INSERT INTO statuts (statut, evaluation, cause, nb_clients, id_panne, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (statut['statut'], statut['evaluation'], statut['cause'], statut['nb_clients'], id_panne, timestamp))

def main(data):
    timestamp = get_current_timestamp()
    pannes = get_pannes(data)
    if not pannes:
        print("No data to process.")
        return
    conn = connect_db()
    if conn is None:
        return
    cursor = conn.cursor()
    for panne in pannes:
        entries = get_entries(panne)
        insert_data(conn, cursor, entries, timestamp)
    cursor.close()
    conn.close()
    return timestamp
