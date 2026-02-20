# simple client that talks to OpenWeatherMap's REST API
# it uses requests to make HTTP calls and dotenv to read the API key
import requests
import os
from dotenv import load_dotenv

# read variables from .env file into environment
load_dotenv()

class OpenWeatherClient:
    def __init__(self):
        self.api_key = os.getenv("OPENWEATHER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENWEATHER_API_KEY is not set. Please add it to your .env file or environment.")
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"

    def fetch_weather(self, city):
        """
        Queries the API for a specific city and handles basic HTTP errors.
        """
        # build query parameters for the HTTP GET request
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric" # ask the API to return temperatures in Celsius
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status() # Handles 4xx and 5xx errors
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {e}")
            return None