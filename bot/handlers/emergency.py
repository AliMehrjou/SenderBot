from aiogram import Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select, func
from database.models import Order, OrderStatus
from workers.session_manager import worker_pool
from bot.keyboards.main_menu import get_main_menu_keyboard
# Import the cleanup function
from bot.handlers.login_handlers import cleanup_client
import html
router = Router(name="emergency_router")


@router.message(Command("start"))
async def start_handler(message: types.Message) -> None:
    """
    هندلر اولین ورود کاربر.
    نکته مهم: state پاک نمی‌شود تا اگر کاربر در میانه فلوی خاصی ربات را استارت زد، فلوی او مختل نشود.
    """
    # در اینجا state عمداً پاک نمی‌شود.
    first_name = message.from_user.first_name if message.from_user else "کاربر"
    first_name = html.escape(first_name)
    
    welcome_text = (
        f"👋 سلام {first_name}!\n\n"
        f"به ربات Bulk Sender خوش آمدید.\n"
        f"لطفاً یک گزینه را انتخاب کنید:"
    )
    
    await message.answer(
        welcome_text,
        reply_markup=get_main_menu_keyboard() 
    )


@router.message(Command("reset"))
async def reset_handler(message: types.Message, state: FSMContext) -> None:
    """
    هندلر ریست سیستم. state و کلاینت‌های متصل را پاک می‌کند.
    """
    # 1. Clear the FSM state
    await state.clear()
    
    # 2. FIX MEMORY LEAK: Safely disconnect any active Pyrogram client in RAM
    await cleanup_client(message.from_user.id)
    
    reset_text = (
        "🔄 سیستم ریست شد.\n\n"
        "وضعیت شما پاک شد و به منوی اصلی بازگشتید."
    )
    
    await message.answer(
        reset_text,
        reply_markup=get_main_menu_keyboard() 
    )

@router.message(Command("fix"))
async def fix_handler(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    """
    هندلر تعمیر و عیب‌یابی سیستم (ارائه گزارش واقعی از وضعیت زیرساخت).
    """
    await state.clear()
    await cleanup_client(message.from_user.id)
    
    try:
        # ۱. تست اتصال دیتابیس
        await session.execute(text("SELECT 1"))
        
        # ۲. شمارش ورکرهای متصل
        connected_workers = sum(1 for client in worker_pool.values() if getattr(client, "is_connected", False))
        
        # ۳. شمارش سفارش‌های در انتظار
        pending_count = await session.scalar(
            select(func.count(Order.id)).where(Order.status == OrderStatus.pending)
        ) or 0
        
        fix_text = (
            "🛠 <b>گزارش عیب‌یابی و پاکسازی سیستم</b>\n\n"
            "✅ وضعیت کاربر (State) ریست شد.\n"
            "✅ نشست‌های موقت با موفقیت از حافظه پاک شدند.\n\n"
            "📊 <b>وضعیت فعلی زیرساخت:</b>\n"
            f"🔌 ارتباط با دیتابیس: <b>برقرار</b>\n"
            f"🤖 ورکرهای متصل: <code>{connected_workers}</code>\n"
            f"⏳ سفارش‌های در صف: <code>{pending_count}</code>"
        )
    except Exception as e:
        fix_text = (
            "❌ <b>خطا در عیب‌یابی سیستم</b>\n\n"
            "✅ وضعیت کاربر و نشست‌های موقت پاک شدند.\n\n"
            "⚠️ <b>ارتباط با دیتابیس یا اجرای کوئری با مشکل مواجه شد:</b>\n"
            f"<code>{html.escape(str(e))}</code>"
        )
    
    await message.answer(
        fix_text,
        reply_markup=get_main_menu_keyboard() 
    )