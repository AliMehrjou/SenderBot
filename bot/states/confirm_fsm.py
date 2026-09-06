# bot/states/confirm_fsm.py
from aiogram.fsm.state import State, StatesGroup


class ConfirmStates(StatesGroup):
    """
    🔵 فاز ۱ (تأیید دو مرحله‌ای):
    استیت مشترک برای الگوی استاندارد تأیید عملیات‌های تخریبی.
    این استیت در فازهای بعدی (حذف API، دسته‌بندی، اکانت و ادمین) نیز مورد استفاده قرار می‌گیرد.
    """
    waiting_for_confirmation = State()