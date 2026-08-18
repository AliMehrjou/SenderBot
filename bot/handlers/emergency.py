from aiogram import Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

# Import the cleanup function
from bot.handlers.login_handlers import cleanup_client

router = Router(name="emergency_router")

@router.message(Command("start", "reset", "fix"))
async def emergency_reset_handler(message: types.Message, state: FSMContext) -> None:
    # 1. Clear the FSM state
    await state.clear()
    
    # 2. FIX MEMORY LEAK: Safely disconnect any active Pyrogram client in RAM
    await cleanup_client(message.from_user.id)
    
    safe_menu_text = (
        "🔄 <b>System Reset Successful</b>\n\n"
        "Your state has been cleared and you have been returned to the main menu. "
        "Any active FSM locks and memory cache have been safely removed.\n\n"
        "<i>What would you like to do next?</i>"
    )
    await message.answer(safe_menu_text)