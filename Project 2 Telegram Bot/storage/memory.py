users = {}

def create_user(profile: dict) -> dict:
    return {
        "profile": profile,
        "water_goal": 0,
        "water_goal_date": None,
        "current_temp": None,
        "kcal_goal_date": None,
        "workout_date": None,
        "calorie_goal": 0,
        "logged_water": 0,
        "logged_calories": 0,
        "burned_calories": 0,
        "total_workout_today": 0,
    }

def user_exists(user_id: int) -> bool:
    return user_id in users