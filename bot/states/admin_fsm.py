from aiogram.fsm.state import State, StatesGroup

class AdminManageStates(StatesGroup):
    waiting_for_admin_id = State()

class AdminMessageStates(StatesGroup):
    waiting_for_recipient_selection = State()
    waiting_for_message_content = State()
    waiting_for_send_confirmation = State()