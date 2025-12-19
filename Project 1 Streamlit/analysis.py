import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import requests
import httpx
import asyncio
from datetime import datetime

def analyze_city(df_with_city: pd.DataFrame, window: int=30) -> pd.DataFrame:
    """
    Принимает данные одного города.
    Возвращает DataFrame с rolling mean, rolling std и флагом аномалии.
    """
    df_with_city_copy = df_with_city.sort_values("timestamp").copy()

    rolling = df_with_city_copy["temperature"].rolling(window=window, min_periods=window)

    df_with_city_copy["rolling_mean"] = rolling.mean()
    df_with_city_copy["rolling_std"] = rolling.std()

    df_with_city_copy["lower_bound"] = df_with_city_copy["rolling_mean"] - 2 * df_with_city_copy["rolling_std"]
    df_with_city_copy["upper_bound"] = df_with_city_copy["rolling_mean"] + 2 * df_with_city_copy["rolling_std"]

    df_with_city_copy["is_anomaly"] = (
        (df_with_city_copy["temperature"] < df_with_city_copy["lower_bound"]) |
        (df_with_city_copy["temperature"] > df_with_city_copy["upper_bound"])
    )
    return df_with_city_copy

def analyze_all_cities(df: pd.DataFrame, window: int = 30) -> pd.DataFrame:
    """
    Применяет анализ временного ряда ко всем городам.
    Возвращает объединённый DataFrame.
    """
    cities = df["city"].unique()
    results = []

    for city in cities:
        df_with_city = analyze_city(df[df["city"] == city], window=window)
        results.append(df_with_city)
    return pd.concat(results, ignore_index=True)

def _analyze_city_wrapper(args):
    df_city, window = args
    return analyze_city(df_city, window)


def analyze_all_cities_parallel(df: pd.DataFrame, window: int = 30) -> pd.DataFrame:
    """
    Параллельный анализ временных рядов по городам.
    """
    cities = df["city"].unique()
    tasks = []

    for city in cities:
        df_city = df[df["city"] == city]
        tasks.append((df_city, window))

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(_analyze_city_wrapper, tasks))

    return pd.concat(results, ignore_index=True)


def compute_season_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Считает среднюю температуру и стандартное отклонение
    для каждого сезона в каждом городе.
    """
    df_season = df.groupby(["city", "season"])["temperature"].agg(["mean", "std"]).reset_index()
    return df_season

def get_current_temperature_sync(city: str, api_key: str):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP error {response.status_code}: {response.text}"
        )
    
    data = response.json()

    return data["main"]["temp"]

async def get_current_temperature_async(city: str, api_key: str):

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }

    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(url, params=params)

        if response.status_code != 200:
            raise RuntimeError(
                f"HTTP error {response.status_code}: {response.text}"
            )
        
        data = response.json()
    
    return data["main"]["temp"]

def check_current_temperature_anomaly(
    city: str,
    current_temp: float,
    season_stats: pd.DataFrame
):

    month_num = datetime.now().month

    if month_num in [12, 1, 2]:
        season = "winter"
    elif month_num in [3, 4, 5]:
        season = "spring"
    elif month_num in [6, 7, 8]:
        season = "summer"
    else:
        season = "autumn"

    season_filtered = season_stats[
        (season_stats["city"] == city) & 
        (season_stats["season"] == season)
        ]
    
    season_mean = season_filtered["mean"].iloc[0]
    season_std = season_filtered["std"].iloc[0]

    season_lower_bound = season_mean - 2 * season_std
    season_upper_bound = season_mean + 2 * season_std

    if (current_temp < season_lower_bound) or (current_temp > season_upper_bound):
        return True
    
    return False
    # return {
    #     "city": city,
    #     "season": season,
    #     "current_temp": current_temp,
    #     "mean": season_mean,
    #     "std": season_std,
    #     "lower_bound": season_lower_bound,
    #     "upper_bound": season_upper_bound,
    #     "is_anomaly": not (season_lower_bound <= current_temp <= season_upper_bound)
    # }