import requests

HQ_API_URL_TIMESTAMP = "http://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
HQ_API_URL_PANNES = "http://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{BIS_VERSION}.json"

def fetch_data(url, error_message):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"{error_message}: {e}")
        return None

def set_url(url, bis_version):
    return url.replace("{BIS_VERSION}", bis_version)

def main():
    timestamp = fetch_data(HQ_API_URL_TIMESTAMP, "Erreur horodatage")
    if timestamp:
        url = set_url(HQ_API_URL_PANNES, timestamp)
        pannes_data = fetch_data(url, "Erreur pannes")
        if pannes_data:
            return pannes_data