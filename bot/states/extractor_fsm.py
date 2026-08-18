from aiogram.fsm.state import State, StatesGroup

class ExtractorStates(StatesGroup):
    waiting_for_link = State()