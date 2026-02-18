import requests
import os
from dotenv import load_dotenv

load_dotenv()

class OpenWeatherClient:
    def __init__(self):
        self.api_key = os.getenv("OPENWEATHER_API_KEY")
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"

    def fetch_weather(self, city):
        """
        Queries the API for a specific city and handles basic HTTP errors.
        """
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric" # We want Celsius
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status() # Handles 4xx and 5xx errors
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {e}")
            return None