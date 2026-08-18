from aiogram.fsm.state import State, StatesGroup

from aiogram.fsm.state import State, StatesGroup

class CreateOrderStates(StatesGroup):
    waiting_for_category = State()
    waiting_for_order_type = State()
    waiting_for_target_data = State()
    waiting_for_message = State()
    waiting_for_button = State() 
    waiting_for_schedule = State()