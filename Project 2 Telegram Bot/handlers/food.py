from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
import datetime

from storage.memory import users, user_exists
from services.food_api import get_calories

class FoodStates(StatesGroup):
    waiting_for_grams = State()

def log_food_handler(dp: Dispatcher):
    @dp.message(Command("log_food"))
    async def log_food(message: Message, state: FSMContext):
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
        
        product_name = message.text.replace("/log_food", "").strip()

        if product_name == "":
            await message.reply("Укажите название продукта")
            return
        
        name, kcal, error = await get_calories(product_name)

        if error == "network":
            await message.reply("Проблемы с подключением к OpenFoodFacts")
            return
        
        if error == "not_found":
            await message.reply("Продукт не найден")
            return
        
        await state.update_data(kcal=kcal, product_name=name)
        await message.reply(f"{name} — {kcal} ккал на 100 г. Сколько грамм вы съели?")
        await state.set_state(FoodStates.waiting_for_grams)

    @dp.message(FoodStates.waiting_for_grams)
    async def how_much_gram(message: Message, state: FSMContext):
        if message.from_user is None:
            await message.reply("Пользователь не определен")
            return
        
        user_id = message.from_user.id

        if user_exists(user_id) is False:
            await message.reply("Для начала введите команду /set_profile")
            return
        if message.text is None:
            await message.reply("Введите число")
            return
        try:
            grams = float(message.text)
            if grams < 0:
                raise ValueError
        except ValueError:
            await message.reply("Некорректно введена масса продукта")
            return
        today = datetime.date.today()

        if users[user_id]["kcal_goal_date"] != today:
            users[user_id]["kcal_goal_date"] = today
            users[user_id]["logged_calories"] = 0
            users[user_id]["burned_calories"] = 0

        data = await state.get_data()
        kcal = data["kcal"]
        product_name = data["product_name"]

        total_kcal = grams / 100 * kcal

        users[user_id]["logged_calories"] += total_kcal

        left_to_eat = users[user_id]["calorie_goal"] - users[user_id]["logged_calories"] + users[user_id]["burned_calories"]
        await message.reply(f"Осталось съесть {left_to_eat} ккал")
        await state.clear()