import requests
from connect_db import connect_db
import psycopg2
from psycopg2.extras import Json

ACCUWEATHER_GEOPOSITION_URL = 'http://dataservice.accuweather.com/locations/v1/cities/geoposition/search?apikey={apikey}&q={latitude}%2C{longitude}'
ACCUWEATHER_24H_HISTORICAL_URL = 'http://dataservice.accuweather.com/currentconditions/v1/{location_key}/historical/24?apikey={apikey}&details=true'

API_KEY = 'TaGvkflUrEGhQkK3lEbsxiux9kv0ns73'

GEOPOSITION_ERR_MSG = "Erreur lors de la récupération des données de géoposition depuis l'API AccuWeather"
HISTORICAL_ERR_MSG = "Erreur lors de la récupération des données historiques depuis l'API AccuWeather"

def fetch_data(url, error_message):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"{error_message}: {e}")
        return None
    
def get_location_key(json):
    return (json['LocalizedName'], json['Key']) if json and len(json) > 0 else None

def set_historical_url(apikey, location_key):
    return ACCUWEATHER_24H_HISTORICAL_URL.format(apikey=apikey, location_key=location_key)

def set_geoposition_url(apikey, latitude, longitude):
    return ACCUWEATHER_GEOPOSITION_URL.format(apikey=apikey, latitude=latitude, longitude=longitude)

def get_city_code(cursor, id):
    cursor.execute("""
        SELECT id_ville
        FROM pannes
        WHERE id = %s;
    """, (id,))
    result = cursor.fetchone()
    return result[0] if result else None

def get_code_meteo_psql(cursor, id):
    cursor.execute("""
        SELECT code_meteo
        FROM villes
        WHERE id = %s;
    """, (id,))
    result = cursor.fetchone()
    return result[0] if result else None

def list_pannes(cursor, id):
    cursor.execute("""
        SELECT latitude, longitude
        FROM pannes
        WHERE id_ville = %s;
    """, (id,))
    return cursor.fetchall()

def moyenne_coord(coordonnees):
    if not coordonnees:
        return None

    latitudes = [lat for lat, _ in coordonnees]
    longitudes = [lon for _, lon in coordonnees]

    latitude_avg = (max(latitudes) + min(latitudes)) / 2
    longitude_avg = (max(longitudes) + min(longitudes)) / 2

    return round(latitude_avg, 2), round(longitude_avg, 2)

def fetch_code_meteo(latitude, longitude):
    url = set_geoposition_url(API_KEY, latitude, longitude)
    json = fetch_data(url, GEOPOSITION_ERR_MSG)
    location_key = get_location_key(json)
    if not location_key:
        return None
    return location_key

def update_city(cursor, id, lat, long, code_meteo, name):
    cursor.execute("""
        UPDATE villes
        SET latitude_avg = %s, longitude_avg = %s, code_meteo = %s, nom = %s
        WHERE id = %s;
    """, (lat, long, code_meteo, name, id))
    cursor.connection.commit()

def code_meteo(cursor, id):
    city_code = get_city_code(cursor, id)
    if not city_code:
        return None
    code_meteo = get_code_meteo_psql(cursor, city_code)
    if code_meteo:
        print(f"Code météo trouvé dans la base de données : {code_meteo}")
        return code_meteo
    coords = list_pannes(cursor, city_code)
    lat, long = moyenne_coord(coords)
    location_key = fetch_code_meteo(long, lat)
    if not location_key:
        print(f"Aucun code météo trouvé pour la ville avec ID {city_code}.")
        return None
    update_city(cursor, city_code, lat, long, location_key[1], location_key[0])
    return location_key[1] if location_key else None

def fetch_meteo_data(code_meteo):
    url = set_historical_url(API_KEY, code_meteo)
    json = fetch_data(url, HISTORICAL_ERR_MSG)
    if not json:
        return None
    return json

def update_meteo_data(cursor, id, data):
    cursor.execute("""
        UPDATE pannes
        SET meteo24h = %s
        WHERE id = %s;
    """, (Json(data), id))
    cursor.connection.commit()

def main(data):
    conn = connect_db()
    if not conn:
        return "Database connection failed."
    cursor = conn.cursor()
    for id in data:
        print(f"Traitement de la panne ID: {id}")
        meteo_code = code_meteo(cursor, id[0])
        if meteo_code:
            print(f"Code météo pour la panne ID {id}: {meteo_code}")
        else:
            print(f"Aucun code météo trouvé pour la panne ID {id}.")
            break
        json = fetch_meteo_data(meteo_code)
        if json:
            update_meteo_data(cursor, id[0], json)
            print(f"Données météo mises à jour pour la panne ID {id}.")
        else:
            print(f"Aucune donnée météo trouvée pour la panne ID {id}.")
            break
    cursor.close()
    conn.close()