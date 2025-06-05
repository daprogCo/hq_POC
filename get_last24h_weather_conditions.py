import requests

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

def main(latitude, longitude):
    url = set_geoposition_url(API_KEY, latitude, longitude)
    geoposition_data = fetch_data(url, GEOPOSITION_ERR_MSG)
    if geoposition_data:
        location_key = get_location_key(geoposition_data)
        print(f"Localisation trouvée : {location_key[0]} (Key: {location_key[1]})")
        if location_key:
            historical_url = set_historical_url(API_KEY, location_key[1])
            historical_data = fetch_data(historical_url, HISTORICAL_ERR_MSG)
            return historical_data
        else:
            print("Aucune donnée de géoposition trouvée.")
            return None

if __name__ == "__main__":
    # Example coordinates for Saint-Constant, QC
    latitude = 45.48
    longitude = -73.47
    historical_data = main(latitude, longitude)
    if historical_data:
        print(historical_data)
    else:
        print("Aucune donnée historique trouvée.")