from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from storage.memory import users, user_exists
from services.calculations import calculate_water
from services.weather import current_temperature

class WaterStates(StatesGroup):
    waiting_for_amount = State()

def log_water_handler(dp: Dispatcher):
    @dp.message(Command("log_water"))
    async def log_water(message: Message, state: FSMContext):
        if message.from_user is None:
            await message.answer("Пользователь не определен")
            return
        
        user_id = message.from_user.id

        if user_exists(user_id) is False:
            await message.answer("Для начала введите команду /set_profile")
            return
        
        if message.text is None:
            await message.answer("Некорректно использована команда")
            return
        
        message_parts = message.text.split()

        if len(message_parts) == 2:
            count_water = message_parts[1]

        elif len(message_parts) == 1:
            await message.answer("Введите количество воды в мл")
            await state.set_state(WaterStates.waiting_for_amount)
            return
        else:
            await message.answer("Необходимо корректно ввести /log_water <мл>")
            return
        
        try:
            count_water = int(count_water)

            if count_water <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Необходимо корректно количестно воды, напримео: 300")
            return
        
        users[user_id]["logged_water"] += count_water

        await message.answer(f"Занесено количестно воды: {count_water} мл")
        await message.answer(f"Общее количество воды за сегодня: {users[user_id]['logged_water']} мл")

        temp, error = await current_temperature(users[user_id]["profile"]["location"])

        if error == "location":
            await message.answer("Температура воздуха не учтена, так как город не распознан. " \
            "Попробуйте указать корректное название. Например: Москва или Moscow.")
        elif error == "api":
            await message.answer("Температура воздуха не учтена, так как сервис погоды временно недоступен.")
        elif error == "network":
            await message.answer("Температура воздуха не учтена, так как проблемы с сетью. Попробуйте позже.")
        elif error is not None:
            await message.answer("Температура воздуха не учтена.")
        
        total_water = calculate_water(users[user_id], temp)
        left_to_drink = max(total_water - users[user_id]['logged_water'], 0)
        await message.answer(f"Осталось выпить {left_to_drink} мл")

    @dp.message(WaterStates.waiting_for_amount)
    async def process_water_amount(message: Message, state: FSMContext):
        if message.from_user is None:
            await message.answer("Пользователь не определен")
            return

        if message.text is None:
            await message.answer("Введите количество воды числом")
            return

        try:
            count_water = int(message.text.strip())
            if count_water <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Введите корректное количество воды, например: 300")
            return

        user_id = message.from_user.id

        users[user_id]["logged_water"] += count_water

        await message.answer(f"Занесено количество воды: {count_water} мл")
        await message.answer(f"Общее количество воды за сегодня: {users[user_id]['logged_water']} мл")

        temp, error = await current_temperature(users[user_id]["profile"]["location"])

        if error == "location":
            await message.answer("Температура воздуха не учтена, так как город не распознан.")
        elif error == "api":
            await message.answer("Температура воздуха не учтена, так как сервис погоды недоступен.")
        elif error == "network":
            await message.answer("Температура воздуха не учтена из-за проблем с сетью.")

        total_water = calculate_water(users[user_id], temp)
        left_to_drink = max(total_water - users[user_id]['logged_water'], 0)

        await message.answer(f"Осталось выпить {left_to_drink} мл")

        await state.clear()
