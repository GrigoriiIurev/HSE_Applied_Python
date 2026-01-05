from aiogram import Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from storage.memory import create_user, users
from services.calculations import calculate_calories

class ProfileStates(StatesGroup):
    weight = State()
    height = State()
    age = State()
    train = State()
    location = State()
    sex = State()

def profile_handler(dp: Dispatcher):
    @dp.message(Command("set_profile"))
    async def start_profile(message: Message, state: FSMContext):
        await message.reply("Введите ваш вес (в кг):")
        await state.set_state(ProfileStates.weight)

    @dp.message(ProfileStates.weight)
    async def process_weight(message: Message, state: FSMContext):
        if message.text is None:
            await message.reply("Введите число")
            return
        try:
            weight = int(message.text)
            if weight <= 0:
                raise ValueError
        except ValueError:
            await message.reply("Некорректно введен вес")
            return
        await state.update_data(weight = weight)
        await message.reply("Введите ваш рост (в см):")
        await state.set_state(ProfileStates.height)
    
    @dp.message(ProfileStates.height)
    async def process_height(message: Message, state: FSMContext):
        if message.text is None:
            await message.reply("Введите число")
            return
        try:
            height = int(message.text)
            if height <= 0 or height > 300:
                raise ValueError
        except ValueError:
            await message.reply("Некорректно введен рост")
            return
        await state.update_data(height = height)
        await message.reply("Введите ваш возраст:")
        await state.set_state(ProfileStates.age)

    @dp.message(ProfileStates.age)
    async def process_age(message: Message, state: FSMContext):
        if message.text is None:
            await message.reply("Введите число")
            return
        try:
            age = int(message.text)
            if age <= 0 or age > 140:
                raise ValueError
        except ValueError:
            await message.reply("Некорректно введен возраст")
            return
        await state.update_data(age = age)
        await message.reply("Сколько минут в среднем активности у вас в день?")
        await state.set_state(ProfileStates.train)

    @dp.message(ProfileStates.train)
    async def process_train(message: Message, state: FSMContext):
        if message.text is None:
            await message.reply("Введите число")
            return
        try:
            train= int(message.text)
            if train < 0:
                raise ValueError
        except ValueError:
            await message.reply("Некорректно введено время")
            return
        await state.update_data(train= train)
        await message.reply("В каком городе вы находитесь?")
        await state.set_state(ProfileStates.location)
    
    @dp.message(ProfileStates.location)
    async def process_location(message: Message, state: FSMContext):
        if message.text is None:
            await message.reply("Введите название города")
            return
        await state.update_data(location=message.text)
        await message.reply("Введите ваш пол:")
        await state.set_state(ProfileStates.sex)
    
    @dp.message(ProfileStates.sex)
    async def process_sex(message: Message, state: FSMContext):
        if message.text is None:
            await message.reply("Введите пол: м или ж")
            return

        sex_input = message.text.strip().lower()

        if sex_input in ("м", "муж", "мужчина", "male"):
            sex = "male"
        elif sex_input in ("ж", "жен", "женщина", "female"):
            sex = "female"
        else:
            await message.reply("Введите пол корректно: м или ж")
            return

        await state.update_data(sex=sex)
        
        data = await state.get_data()
        if message.from_user is None:
            await message.reply("Ошибка: не удалось определить пользователя")
            return

        user_id = message.from_user.id

        calorie_goal = calculate_calories(data)

        user = create_user(data)
        user["calorie_goal"] = calorie_goal
        users[user_id] = user

        await state.clear()
        await message.reply("Профиль сохранён")