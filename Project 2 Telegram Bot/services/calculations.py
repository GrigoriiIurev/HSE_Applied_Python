import datetime

def calculate_calories(profile: dict) -> int:
    weight = profile["weight"]
    height = profile["height"]
    age = profile["age"]
    train = profile["train"]
    sex = profile["sex"]

    if sex == "male":
        bmr = 10 * weight + 6.25 * height - 5 * age + 5
    else:
        bmr = 10 * weight + 6.25 * height - 5 * age - 161

    if train <= 15:
        activity_coef = 1.2
    elif train <= 45:
        activity_coef = 1.375
    elif train <= 75:
        activity_coef = 1.55
    elif train <= 120:
        activity_coef = 1.725
    else:
        activity_coef = 1.9

    total_calories = bmr * activity_coef
    return int(total_calories)

def calculate_water(user: dict, temperature: float | None) -> int:
    weight = user["profile"]["weight"]
    train_minutes = user["profile"]["train"]

    today = datetime.date.today()

    base_water = weight * 30

    if user["water_goal_date"] != today:
        weather_bonus = _weather_bonus(temperature)
        total = base_water + weather_bonus

        user["water_goal"] = total
        user["water_goal_date"] = today
        user["current_temp"] = temperature

        return total

    if temperature is not None:
        prev_temp = user["current_temp"]

        if prev_temp is None or temperature > prev_temp:
            weather_bonus = _weather_bonus(temperature)
            total = base_water + weather_bonus

            user["water_goal"] = total
            user["current_temp"] = temperature

            return total

    return user["water_goal"]


def _weather_bonus(temperature: float | None) -> int:
    if temperature is None:
        return 0
    if 25 <= temperature <= 30:
        return 500
    if temperature > 30:
        return 1000
    return 0

def calculate_workout(user: dict, workout_type: str, minutes: int):
    today = datetime.date.today()

    if user["workout_date"] != today:
        user["workout_date"] = today
        user["total_workout_today"] = 0

    workout_coefs = {
        "бег": 10,
        "ходьба": 5,
        "велосипед": 8,
        "силовая": 7,
        "плавание": 9,
    }

    coef = workout_coefs.get(workout_type.lower(), 6)

    burned_calories = coef * minutes

    old_total = user["total_workout_today"]
    new_total = old_total + minutes

    old_blocks = old_total // 30
    new_blocks = new_total // 30

    extra_water = (new_blocks - old_blocks) * 200

    user["total_workout_today"] = new_total

    return burned_calories, extra_water