from aiogram.fsm.state import State, StatesGroup


class SettingsStates(StatesGroup):
    
    waiting_for_category = State()
    waiting_for_proxies = State()
    waiting_for_send_limit = State()
    waiting_for_max_accounts_api = State()
    waiting_for_cooldown_hours = State()
    waiting_for_spam_penalty = State()

    
    waiting_for_category_edit = State()