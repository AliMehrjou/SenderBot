from aiogram.fsm.state import State, StatesGroup

class CreateOrderStates(StatesGroup):
    waiting_for_category = State()
    waiting_for_order_type = State()
    waiting_for_send_type = State() 
    waiting_for_target_data = State() 
    waiting_for_send_method = State()
    waiting_for_source_channel = State()
    waiting_for_source_messages = State()
    waiting_for_messages = State()    
    waiting_for_banner_pool = State() 
    waiting_for_smart_flow = State()    
    waiting_for_message = State()    
    waiting_for_button = State()     
    waiting_for_schedule = State()  
    waiting_for_filter = State()
    waiting_for_forward_style = State()