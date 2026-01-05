from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command

from storage.memory import users, user_exists
from services.calculations import calculate_water
from services.weather import current_temperature

def log_water_handler(dp: Dispatcher):
    @dp.message(Command("log_water"))
    async def log_water(message: Message):
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
        
        message_parts = message.text.split()

        if len(message_parts) != 2:
            await message.reply("Необходимо корректно ввести /log_water <мл>")
            return
        
        count_water = message_parts[1]

        try:
            count_water = int(count_water)

            if count_water <= 0:
                raise ValueError
        except ValueError:
            await message.reply("Необходимо корректно количестно воды, напримео: 300")
            return
        
        users[user_id]["logged_water"] += count_water

        await message.reply(f"Занесено количестно воды: {count_water} мл")
        await message.reply(f"Общее количество воды за сегодня: {users[user_id]['logged_water']} мл")

        temp, error = await current_temperature(users[user_id]["profile"]["location"])

        if error == "location":
            await message.reply("Температура воздуха не учтена, так как город не распознан. " \
            "Попробуйте указать корректное название. Например: Москва или Moscow.")
        elif error == "api":
            await message.reply("Температура воздуха не учтена, так как сервис погоды временно недоступен.")
        elif error == "network":
            await message.reply("Температура воздуха не учтена, так как проблемы с сетью. Попробуйте позже.")
        elif error is not None:
            await message.reply("Температура воздуха не учтена.")
        
        total_water = calculate_water(users[user_id], temp)
        left_to_drink = total_water - users[user_id]['logged_water']
        await message.reply(f"Осталось выпить {left_to_drink} мл")

