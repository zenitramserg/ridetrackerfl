"""
weather.py
Fetch weather forecast for upcoming rides using Open-Meteo (free, no API key required).
Weston, FL coordinates: 26.1004° N, 80.3997° W
"""

import json
import urllib.request
import urllib.parse
from datetime import datetime
from typing import Optional

WESTON_LAT = 26.1004
WESTON_LON = -80.3997

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def _fetch_forecast(lat: float, lon: float, date_str: str) -> Optional[dict]:
    """
    Fetch hourly forecast from Open-Meteo for a given date.
    date_str: "YYYY-MM-DD"
    Returns the raw API response or None on error.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "precipitation_probability,windspeed_10m",
        "daily": "precipitation_probability_max,windspeed_10m_max",
        "timezone": "America/New_York",
        "start_date": date_str,
        "end_date": date_str,
        "wind_speed_unit": "mph",
    }
    url = OPEN_METEO_URL + "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f"[weather] Error fetching forecast: {e}")
        return None


def get_ride_weather(ride_date: str, ride_time: str) -> dict:
    """
    Get weather for a specific ride.

    Args:
        ride_date: "YYYY-MM-DD" or full date string like "Friday March 28, 2026"
        ride_time: "06:00 AM"

    Returns dict with:
        rain_probability (int, 0-100)
        wind_speed (float, mph)
        weather_summary (str)
    """
    empty = {"rain_probability": None, "wind_speed": None, "weather_summary": ""}

    # Parse date to YYYY-MM-DD
    date_str = _parse_date(ride_date)
    if not date_str:
        return empty

    # Only fetch for rides within the next 7 days (Open-Meteo free tier limit)
    try:
        ride_dt = datetime.strptime(date_str, "%Y-%m-%d")
        days_out = (ride_dt.date() - datetime.now().date()).days
        if days_out < 0 or days_out > 7:
            return empty
    except Exception:
        return empty

    data = _fetch_forecast(WESTON_LAT, WESTON_LON, date_str)
    if not data:
        return empty

    # Extract hourly data for the ride start hour
    ride_hour = _parse_hour(ride_time)
    rain_prob = _extract_hourly(data, "precipitation_probability", ride_hour)
    wind_speed = _extract_hourly(data, "windspeed_10m", ride_hour)

    # Fall back to daily max if hourly not available
    if rain_prob is None:
        try:
            rain_prob = data["daily"]["precipitation_probability_max"][0]
        except Exception:
            rain_prob = None

    if wind_speed is None:
        try:
            wind_speed = data["daily"]["windspeed_10m_max"][0]
        except Exception:
            wind_speed = None

    summary = _build_summary(rain_prob, wind_speed)

    return {
        "rain_probability": rain_prob,
        "wind_speed": round(wind_speed, 1) if wind_speed is not None else None,
        "weather_summary": summary,
    }


def _extract_hourly(data: dict, field: str, hour: int) -> Optional[float]:
    try:
        times = data["hourly"]["time"]
        values = data["hourly"][field]
        for i, t in enumerate(times):
            if datetime.fromisoformat(t).hour == hour:
                return values[i]
    except Exception:
        pass
    return None


def _parse_hour(time_str: str) -> int:
    """Parse '06:00 AM' → 6, '05:30 AM' → 5, '06:00 PM' → 18"""
    try:
        dt = datetime.strptime(time_str.strip(), "%I:%M %p")
        return dt.hour
    except Exception:
        return 6  # default to 6 AM for early morning rides


def _parse_date(date_str: str) -> Optional[str]:
    """Convert various date formats to YYYY-MM-DD."""
    if not date_str:
        return None

    # Already ISO format
    if len(date_str) == 10 and date_str[4] == "-":
        return date_str

    # "Friday March 28, 2026" or "Friday March 28 2026"
    formats = [
        "%A %B %d, %Y",
        "%A %B %d %Y",
        "%B %d, %Y",
        "%B %d %Y",
        "%m/%d/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _build_summary(rain_prob: Optional[int], wind_speed: Optional[float]) -> str:
    """Build a human-readable weather note."""
    parts = []

    if rain_prob is not None:
        if rain_prob >= 70:
            parts.append(f"Rain likely ({rain_prob}%)")
        elif rain_prob >= 40:
            parts.append(f"Chance of rain ({rain_prob}%)")
        else:
            parts.append(f"Dry ({rain_prob}% rain)")

    if wind_speed is not None:
        if wind_speed >= 20:
            parts.append(f"strong winds {wind_speed:.0f} mph")
        elif wind_speed >= 12:
            parts.append(f"moderate winds {wind_speed:.0f} mph")
        else:
            parts.append(f"light winds {wind_speed:.0f} mph")

    return ", ".join(parts) if parts else ""


if __name__ == "__main__":
    # Quick test
    result = get_ride_weather("Friday March 28, 2026", "06:00 AM")
    print(json.dumps(result, indent=2))
