from aiogram.fsm.state import State, StatesGroup

class ExtractorStates(StatesGroup):
    waiting_for_analysis_type = State()
    waiting_for_link = State()