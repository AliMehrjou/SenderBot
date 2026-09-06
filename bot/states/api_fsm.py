from aiogram.fsm.state import State, StatesGroup

class APIStates(StatesGroup):
    waiting_for_api_credentials = State()