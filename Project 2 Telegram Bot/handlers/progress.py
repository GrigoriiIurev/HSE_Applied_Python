from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command

from storage.memory import users, user_exists

def check_progress_handler(dp: Dispatcher):
    @dp.message(Command("check_progress"))
    async def check_progress(message: Message):
        if message.from_user is None:
            await message.reply("Пользователь не определен")
            return
        
        user_id = message.from_user.id

        if user_exists(user_id) is False:
            await message.reply("Для начала введите команду /set_profile")
            return
        
        if message.text is None:
            await message.reply("Некорректно использована команда")
            return
        
        user = users[user_id]

        water_goal = user["water_goal"]
        logged_water = user["logged_water"]
        water_left = max(water_goal - logged_water, 0)

        calorie_goal = user["calorie_goal"]
        logged_calories = user["logged_calories"]
        burned_calories = user["burned_calories"]
        balance = calorie_goal - logged_calories + burned_calories

        response = (
            "📊 Прогресс:\n"
            "Вода:\n"
            f"- Выпито: {logged_water} мл из {water_goal} мл.\n"
            f"- Осталось: {water_left} мл.\n\n"
            "Калории:\n"
            f"- Потреблено: {logged_calories} ккал из {calorie_goal} ккал.\n"
            f"- Сожжено: {burned_calories} ккал.\n"
            f"- Баланс: {balance} ккал."
        )

        await message.answer(response)
