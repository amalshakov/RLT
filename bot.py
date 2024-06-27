import asyncio
import json
import os

from aiogram.client.bot import Bot
from aiogram.dispatcher.dispatcher import Dispatcher
from aiogram.enums.parse_mode import ParseMode
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message
from dotenv import load_dotenv

from main import aggregate_data

load_dotenv()

API_TOKEN = os.getenv("SECRET_API_TOKEN")

bot = Bot(token=API_TOKEN, default_parse_mode=ParseMode.MARKDOWN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


@dp.message(Command(commands=["start"]))
async def send_welcome(message: Message):
    await message.answer("Привет! Отправьте JSON данные для агрегации.")


@dp.message()
async def handle_message(message: Message):
    try:
        request_data = json.loads(message.text)
        dt_from = request_data["dt_from"]
        dt_upto = request_data["dt_upto"]
        group_type = request_data["group_type"]

        aggregated_data = aggregate_data(dt_from, dt_upto, group_type)

        await message.answer(json.dumps(aggregated_data, indent=4))
    except Exception as error:
        await message.answer(f"Ошибка: {error}")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
