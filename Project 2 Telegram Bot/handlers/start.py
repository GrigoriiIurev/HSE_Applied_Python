from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from UI.keyboards import start_keyboard, main_keyboard
from storage.memory import user_exists

def start_handler(dp: Dispatcher):
    @dp.message(Command("start"))
    async def cmd_start(message: Message):

        if message.from_user is None:
            await message.answer("Не удалось определить пользователя")
            return

        user_id = message.from_user.id

        if user_exists(user_id):
            keyboard = main_keyboard
            text = "С возвращением 👋"
        else:
            keyboard = start_keyboard
            text = "Добро пожаловать! Сначала настройте профиль 👇"

        await message.answer(text, reply_markup=keyboard)