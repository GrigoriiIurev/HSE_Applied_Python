from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command

from storage.memory import users, user_exists
from services.calculations import calculate_workout

def log_workout_handler(dp: Dispatcher):
    @dp.message(Command("log_workout"))
    async def log_workout(message: Message):
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

        if len(message_parts) < 3:
            await message.reply("Используйте формат: /log_workout <тип тренировки> <время в минутах>")
            return
        
        workout_type = " ".join(message_parts[1:-1])

        try:
            minutes = int(message_parts[-1])
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

        await message.reply(response)
