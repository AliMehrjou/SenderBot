from aiogram import Router, types, F
from bot.keyboards.main_menu import get_main_menu_keyboard

# Initialize the admin panel router
router = Router(name="admin_panel_router")

@router.message(F.text == "/menu")
async def show_main_menu_command(message: types.Message) -> None:
    """
    A simple command to display the main menu.
    """
    await message.answer(
        "🎛 <b>پنل کنترل اصلی</b>\n\nلطفاً یک گزینه را انتخاب کنید:",
        reply_markup=get_main_menu_keyboard()
    )