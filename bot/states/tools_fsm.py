from aiogram.fsm.state import State, StatesGroup

class ToolsStates(StatesGroup):
    waiting_for_raw_ids = State()