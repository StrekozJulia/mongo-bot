import asyncio
import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command


load_dotenv()

CHAT_ID = os.getenv('CHAT_ID')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TELEGRAM_TOKEN)

dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        f"Приветствую, {message.from_user.first_name}! "
        "Введите данные запроса в формате json."
    )


@dp.message()
async def aggregate_data(msg: types.Message):
    result = msg.text
    await bot.send_message(msg.from_user.id, result)


async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
