from aiogram.fsm.state import State, StatesGroup

class LoginStates(StatesGroup):
    waiting_for_category = State()
    waiting_for_login_method = State()
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_string_session = State()