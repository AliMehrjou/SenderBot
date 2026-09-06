from aiogram.fsm.state import State, StatesGroup

class AdminManageStates(StatesGroup):
    waiting_for_admin_id = State()