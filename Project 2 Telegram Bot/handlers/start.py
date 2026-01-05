from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command

def start_handler(dp: Dispatcher):
    @dp.message(Command("start"))
    async def cmd_start(message: Message):
        await message.reply("Добро пожаловать! Я ваш бот.")