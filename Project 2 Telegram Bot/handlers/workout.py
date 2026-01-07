from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from storage.memory import users, user_exists
from services.calculations import calculate_workout

class WorkoutStates(StatesGroup):
    waiting_for_type = State()
    waiting_for_minutes = State()

def log_workout_handler(dp: Dispatcher):
    @dp.message(Command("log_workout"))
    async def log_workout(message: Message, state: FSMContext):
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

        # формат из задания: /log_workout <тип> <время>
        if len(message_parts) >= 3:
            workout_type = " ".join(message_parts[1:-1])
            minutes_part = message_parts[-1]

        # формат через кнопку: /log_workout
        elif len(message_parts) == 1:
            await message.answer("Введите тип тренировки")
            await state.set_state(WorkoutStates.waiting_for_type)
            return
        else:
            await message.answer("Используйте формат: /log_workout <тип тренировки> <время в минутах>")
            return
        
        try:
            minutes = int(minutes_part)
            if minutes <= 0:
                raise ValueError
        except ValueError:
            await message.reply("Время тренировки должно быть положительным числом минут")
            return

        user = users[user_id]

        burned_calories, extra_water = calculate_workout(
            user=user,
            workout_type=workout_type,
            minutes=minutes
        )

        user["burned_calories"] += burned_calories

        response = (
            f"{workout_type} {minutes} минут — {burned_calories} ккал.\n"
            f"Дополнительно: рекомендуется выпить {extra_water} мл воды."
        )

        await message.answer(response)

    @dp.message(WorkoutStates.waiting_for_type)
    async def workout_type_input(message: Message, state: FSMContext):
        if message.from_user is None:
            await message.answer("Пользователь не определен")
            return

        if message.text is None:
            await message.answer("Введите тип тренировки текстом")
            return

        workout_type = message.text.strip()

        if not workout_type:
            await message.answer("Введите тип тренировки")
            return

        await state.update_data(workout_type=workout_type)
        await message.answer("Введите длительность тренировки в минутах")
        await state.set_state(WorkoutStates.waiting_for_minutes)

    @dp.message(WorkoutStates.waiting_for_minutes)
    async def workout_minutes_input(message: Message, state: FSMContext):
        if message.from_user is None:
            await message.answer("Пользователь не определен")
            return

        if message.text is None:
            await message.answer("Введите длительность тренировки числом")
            return

        try:
            minutes = int(message.text.strip())
            if minutes <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Введите корректное количество минут")
            return

        data = await state.get_data()
        workout_type = data["workout_type"]

        user_id = message.from_user.id
        user = users[user_id]

        burned_calories, extra_water = calculate_workout(
            user=user,
            workout_type=workout_type,
            minutes=minutes
        )

        user["burned_calories"] += burned_calories

        response = (
            f"{workout_type} {minutes} минут — {burned_calories} ккал.\n"
            f"Дополнительно: рекомендуется выпить {extra_water} мл воды."
        )

        await message.answer(response)
        await state.clear()
