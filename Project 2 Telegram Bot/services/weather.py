import httpx

async def current_temperature(location: str) -> tuple[float | None, str | None]:
    # from config import MY_WEATHER_TOKEN
    import os

    OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": location,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }

    async with httpx.AsyncClient(timeout = 10) as client:
        try:
            response = await client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                return (data["main"]["temp"], None)
            
            elif response.status_code == 404:
                return (None, "location")
            elif response.status_code == 401:
                return (None, "api")
        except httpx.RequestError:
            return (None, "network")

    return (None, "unknown")


