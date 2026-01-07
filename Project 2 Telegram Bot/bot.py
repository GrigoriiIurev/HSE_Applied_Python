import asyncio
from aiogram import Bot, Dispatcher

# from config import MY_BOT_TOKEN

from handlers.start import start_handler
from handlers.profile import profile_handler
from handlers.water import log_water_handler
from handlers.food import log_food_handler
from handlers.workout import log_workout_handler
from handlers.progress import check_progress_handler
import os

MY_BOT_TOKEN = os.getenv("MY_BOT_TOKEN")

async def main():
    if MY_BOT_TOKEN is None:
        raise RuntimeError("MY_BOT_TOKEN is not set")
    bot = Bot(token=MY_BOT_TOKEN)
    dp = Dispatcher()

    start_handler(dp)
    profile_handler(dp)
    log_water_handler(dp)
    log_food_handler(dp)
    log_workout_handler(dp)
    check_progress_handler(dp)

    print("Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())