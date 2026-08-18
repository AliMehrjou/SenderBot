from aiogram.fsm.state import State, StatesGroup

class SettingsStates(StatesGroup):
    """
    FSM for the Settings and Configuration process.
    """
    waiting_for_category = State()
    waiting_for_proxies = State()
    waiting_for_send_limit = State()