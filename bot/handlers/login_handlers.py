import logging
import re
from typing import Dict
from contextlib import suppress

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, 
    PhoneCodeInvalid, 
    PhoneCodeExpired, 
    PasswordHashInvalid
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from workers.session_manager import start_single_worker
from bot.states.login_fsm import LoginStates
from database.models import Account, Category
from config import config  
from utils.crypto import encrypt_session # ایمپورت ابزار رمزنگاری که در مرحله امنیت ساختیم

logger = logging.getLogger(__name__)

router = Router(name="login_fsm_router")

temp_clients: Dict[int, Client] = {}


# ==========================================
# ENTRY POINT: WAITING FOR CATEGORY
# ==========================================
@router.callback_query(F.data == "menu_add_account/")
async def enter_add_account_flow(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await callback.answer()
    
    # واکشی دسته‌بندی‌ها از دیتابیس
    stmt = select(Category)
    result = await session.execute(stmt)
    categories = result.scalars().all()
    
    if not categories:
        return await callback.message.answer("⚠️ هیچ دسته‌بندی یافت نشد. لطفاً ابتدا از بخش تنظیمات یک دسته‌بندی ایجاد کنید.")

    # ساخت کیبورد شیشه‌ای برای انتخاب دسته‌بندی
    builder = InlineKeyboardBuilder()
    for cat in categories:
        builder.button(text=cat.name, callback_data=f"logincat_{cat.id}/")
    builder.adjust(2) 
    
    await state.set_state(LoginStates.waiting_for_category)
    await callback.message.answer(
        "📁 <b>انتخاب دسته‌بندی</b>\n\n"
        "لطفاً مشخص کنید این اکانت به کدام دسته‌بندی تعلق دارد:",
        reply_markup=builder.as_markup()
    )


# ==========================================
# STATE: PROCESS CATEGORY & ASK FOR PHONE
# ==========================================
@router.callback_query(LoginStates.waiting_for_category, F.data.startswith("logincat_") & F.data.endswith("/"))
async def process_category_selection(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    
    raw_id = callback.data.replace("logincat_", "").replace("/", "")
    if not raw_id.isdigit():
        return await callback.message.answer("⚠️ خطای نامعتبر در انتخاب دسته‌بندی.")
        
    category_id = int(raw_id)
    
    # ذخیره دسته‌بندی در FSM برای استفاده در پایان لاگین
    await state.update_data(category_id=category_id)
    
    await state.set_state(LoginStates.waiting_for_phone)
    await callback.message.edit_text(
        "📱 <b>اضافه کردن اکانت جدید</b>\n\n"
        "لطفاً شماره موبایل را با فرمت بین‌المللی ارسال کنید.\n"
        "<i>مثال: +1234567890</i>"
    )


# ==========================================
# STATE: WAITING FOR PHONE
# ==========================================
@router.message(LoginStates.waiting_for_phone, F.text)
async def process_phone_number(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    phone_number = message.text.strip()
    
    if not re.match(r"^\+\d{7,15}$", phone_number):
        return await message.answer("⚠️ فرمت نامعتبر...")

    # --- جلوگیری از خطای دیتابیس با چک کردن شماره تکراری ---
    stmt = select(Account).where(Account.phone_number == phone_number)
    result = await session.execute(stmt)
    if result.scalar_one_or_none():
        return await message.answer("⚠️ این شماره موبایل قبلاً در سیستم ثبت شده است.")
    # --------------------------------------------------------

    admin_id = message.from_user.id
    
    client = Client(
        name=f"temp_{admin_id}",
        api_id=config.API_ID,      
        api_hash=config.API_HASH,  
        in_memory=True
    )
    
    # --- ثبت کلاینت برای پاکسازی (Garbage Collection) در توقف اضطراری ---
    temp_clients[admin_id] = client
    # ---------------------------------------------------------------------
    
    try:
        await client.connect()
        sent_code = await client.send_code(phone_number)
        
        temp_session = await client.export_session_string()
        
        await state.update_data(
            phone_number=phone_number,
            phone_code_hash=sent_code.phone_code_hash,
            temp_session=temp_session
        )
        
        await state.set_state(LoginStates.waiting_for_code)
        await message.answer(f"✅ کد تایید به <code>{phone_number}</code> ارسال شد.\nلطفاً کد ۵ رقمی را وارد کنید:")
        
    except Exception as e:
        logger.error(f"Failed to send code for {phone_number}: {e}")
        await message.answer(f"❌ <b>خطا:</b> امکان ارسال کد وجود ندارد.\n\n<code>{str(e)}</code>")
        await state.clear()
    finally:
        if client.is_connected:
            await client.disconnect()


# ==========================================
# STATE: WAITING FOR CODE
# ==========================================
@router.message(LoginStates.waiting_for_code, F.text)
async def process_auth_code(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    code = message.text.strip()
    fsm_data = await state.get_data()
    
    phone_number = fsm_data.get("phone_number")
    phone_code_hash = fsm_data.get("phone_code_hash")
    temp_session = fsm_data.get("temp_session")
    
    if not temp_session:
        await state.clear()
        return await message.answer("⚠️ نشست منقضی شده است. لطفاً دوباره شروع کنید.")

    client = Client(name="temp_auth", session_string=temp_session, in_memory=True)
    
    try:
        await client.connect()
        await client.sign_in(
            phone_number=phone_number,
            phone_code_hash=phone_code_hash,
            phone_code=code
        )
        await finalize_login_and_save(message, state, session, client, phone_number)
        
    except SessionPasswordNeeded:
        new_temp_session = await client.export_session_string()
        await state.update_data(temp_session=new_temp_session)
        await state.set_state(LoginStates.waiting_for_password)
        await message.answer("🔐 <b>تایید دو مرحله‌ای (2FA) فعال است.</b>\n\nلطفاً پسورد خود را وارد کنید:")
        
    except (PhoneCodeInvalid, PhoneCodeExpired) as e:
        await message.answer(f"❌ <b>کد نامعتبر:</b> {str(e)}\nلطفاً دوباره تلاش کنید.")
        
    except Exception as e:
        logger.error(f"Sign in error: {e}")
        await message.answer(f"❌ <b>خطای پیش‌بینی نشده:</b>\n\n<code>{str(e)}</code>")
        await state.clear()
    finally:
        if client.is_connected:
            await client.disconnect()

    
# ==========================================
# STATE: WAITING FOR PASSWORD (2FA)
# ==========================================
@router.message(LoginStates.waiting_for_password, F.text)
async def process_2fa_password(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    password = message.text.strip()
    
    with suppress(Exception):
        await message.delete()

    fsm_data = await state.get_data()
    temp_session = fsm_data.get("temp_session")
    phone_number = fsm_data.get("phone_number")
    
    if not temp_session:
        await state.clear()
        return await message.answer("⚠️ نشست منقضی شده است. لطفاً دوباره شروع کنید.")
        
    client = Client(name="temp_auth_2fa", session_string=temp_session, in_memory=True)
    
    try:
        await client.connect()
        await client.check_password(password)
        await finalize_login_and_save(message, state, session, client, phone_number)
        
    except PasswordHashInvalid:
        await message.answer("❌ <b>پسورد اشتباه است.</b> لطفاً دوباره تلاش کنید.")
    except Exception as e:
        logger.error(f"Password error: {e}")
        await message.answer(f"❌ <b>خطای پیش‌بینی نشده:</b>\n\n<code>{str(e)}</code>")
        await state.clear()
    finally:
        if client.is_connected:
            await client.disconnect()


# ==========================================
# UTILITY: FINALIZE & SAVE TO DB
# ==========================================
async def finalize_login_and_save(
    message: types.Message, 
    state: FSMContext, 
    session: AsyncSession, 
    client: Client, 
    phone_number: str
) -> None:
    try:
        fsm_data = await state.get_data()
        category_id = fsm_data.get("category_id")
        
        session_string = await client.export_session_string()
        encrypted_session = encrypt_session(session_string)
        
        new_account = Account(
            phone_number=phone_number,
            session_string=encrypted_session,
            category_id=category_id, 
            is_banned=False
        )
        
        session.add(new_account)
        await session.commit()
        await session.refresh(new_account) # 🔴 مهم: برای دریافت آیدی تخصیص یافته توسط دیتابیس
        
        # 🔴 استارت پویای ورکر بدون نیاز به ری‌استارت ربات
        started = await start_single_worker(new_account, session)
        
        if started:
            await message.answer(
                f"🎉 <b>اکانت با موفقیت اضافه و روشن شد!</b>\n\n"
                f"<b>شماره:</b> <code>{phone_number}</code>\n"
                f"اکنون این اکانت در استخر ورکرها فعال است و می‌تواند سفارش دریافت کند."
            )
        else:
            await message.answer(
                f"⚠️ <b>اکانت اضافه شد اما روشن نشد!</b>\n\n"
                f"لطفاً وضعیت پراکسی‌ها را بررسی کنید."
            )
            
    except Exception as e:
        await session.rollback() 
        logger.error(f"Database error saving account: {e}")
        await message.answer("❌ <b>خطای دیتابیس:</b> ذخیره اکانت با مشکل مواجه شد.")
    finally:
        await cleanup_client(message.from_user.id)
        await state.clear()


async def cleanup_client(admin_id: int) -> None:
    client = temp_clients.pop(admin_id, None)
    if client:
        try:
            if client.is_connected:
                await client.disconnect()
        except Exception as e:
            logger.warning(f"Error disconnecting temporary client: {e}")