from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

start_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/set_profile")]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="/log_food"),
            KeyboardButton(text="/log_water"),
        ],
        [
            KeyboardButton(text="/log_workout"),
            KeyboardButton(text="/check_progress"),
        ],
    ],
    resize_keyboard=True
)

