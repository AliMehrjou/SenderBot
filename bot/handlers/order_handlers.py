import logging
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import os
from aiogram import Bot

from bot.states.order_fsm import CreateOrderStates
from database.models import Category
from database.queries import create_new_order
from datetime import datetime, timezone
import os
import uuid
import logging

logger = logging.getLogger(__name__)

router = Router(name="order_fsm_router")
os.makedirs("downloads", exist_ok=True)
# ==========================================
# ENTRY POINT
# ==========================================
@router.callback_query(F.data == "menu_create_order/")
async def enter_create_order_flow(
    callback: types.CallbackQuery, 
    state: FSMContext, 
    session: AsyncSession
) -> None:
    await callback.answer()
    
    stmt = select(Category)
    result = await session.execute(stmt)
    categories = result.scalars().all()
    
    if not categories:
        await callback.message.answer("⚠️ No categories found in the database. Please add a category first.")
        return

    builder = InlineKeyboardBuilder()
    for cat in categories:
        builder.button(text=cat.name, callback_data=cat.custom_id)
    
    builder.adjust(2) 
    
    await state.set_state(CreateOrderStates.waiting_for_category)
    await callback.message.answer(
        "📝 <b>Create New Order</b>\n\nPlease select the target category for this task:",
        reply_markup=builder.as_markup()
    )


# ==========================================
# STATE: WAITING FOR CATEGORY
# ==========================================
# 1. INTEGRATION: Filter and string replacement updated to exactly match user_xxx/ format
@router.callback_query(CreateOrderStates.waiting_for_category, F.data.startswith("user_") & F.data.endswith("/"))
async def process_category_selection(
    callback: types.CallbackQuery, 
    state: FSMContext
) -> None:
    await callback.answer()
    
    raw_id = callback.data.replace("user_", "").replace("/", "")
    
    if not raw_id.isdigit():
        await callback.message.answer("⚠️ Invalid category selection. Please try again.")
        return
        
    category_id = int(raw_id)
    
    await state.update_data(category_id=category_id)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔴 Target via Group Link", callback_data="ordertype_link/")
    builder.button(text="🔵 Target via Username List", callback_data="ordertype_list/")
    builder.button(text="🟢 Extract Active Users", callback_data="ordertype_extract/")
    builder.adjust(1)
    
    await state.set_state(CreateOrderStates.waiting_for_order_type)
    await callback.message.edit_text(
        "✅ Category selected.\n\nNow, select the type of order you want to run:",
        reply_markup=builder.as_markup()
    )


# ==========================================
# STATE: WAITING FOR ORDER TYPE
# ==========================================
@router.callback_query(CreateOrderStates.waiting_for_order_type, F.data.startswith("ordertype_") & F.data.endswith("/"))
async def process_order_type_selection(
    callback: types.CallbackQuery, 
    state: FSMContext
) -> None:
    await callback.answer()
    
    order_type = callback.data.replace("ordertype_", "").replace("/", "")
    await state.update_data(order_type=order_type)
    
    await state.set_state(CreateOrderStates.waiting_for_target_data)
    await callback.message.edit_text(
        f"✅ Order type set to: <code>{order_type}</code>\n\n"
        "Please send the target data as a text message.\n"
        "<i>(e.g., A public group link, or a multiline list of target usernames)</i>"
    )


# ==========================================
# STATE: WAITING FOR TARGET DATA
# ==========================================
@router.message(CreateOrderStates.waiting_for_target_data, F.text)
async def process_target_data(message: types.Message, state: FSMContext) -> None:
    target_data = message.text.strip()
    if not target_data:
        return await message.answer("⚠️ داده‌ها نمی‌توانند خالی باشند.")
        
    await state.update_data(target_data=target_data)
    await state.set_state(CreateOrderStates.waiting_for_message)
    
    await message.answer(
        "📝 <b>محتوای پیام را ارسال کنید:</b>\n\n"
        "شما می‌توانید یک <b>متن، عکس، ویدیو یا فایل</b> ارسال کنید. "
        "همچنین می‌توانید از متغیرهای <code>{first_name}</code> و <code>{username}</code> برای شخصی‌سازی استفاده کنید.\n\n"
        "<i>(مثال: سلام {first_name} عزیز، به گروه ما بپیوند!)</i>"
    )

@router.message(CreateOrderStates.waiting_for_message)
async def process_message_content(message: types.Message, state: FSMContext, bot: Bot) -> None:

    message_text = message.text or message.caption or ""
    
    # --- بلاک اعتبارسنجی برای جلوگیری از خطای MessageEmpty ---
    if not message_text and not message.photo and not message.video and not message.document:
        return await message.answer("⚠️ فرمت پشتیبانی نمی‌شود! لطفاً فقط متن، عکس، ویدیو یا فایل (Document) ارسال کنید.")
    # ---------------------------------------------------------
    media_path = None
    media_type = None
    message_text = message.text or message.caption or ""

    # ۱. اطمینان از وجود پوشه دانلود (جلوگیری از خطای FileNotFoundError در محیط خارج از داکر)
    os.makedirs("downloads", exist_ok=True)
    
    # ۲. تولید یک نام کاملاً یکتا (UUID) برای جلوگیری از تداخل فایل‌های همنام (Race Condition)
    unique_filename = str(uuid.uuid4())

    try:
        if message.photo:
            media_type = "photo"
            file_id = message.photo[-1].file_id
            file = await bot.get_file(file_id)
            media_path = f"downloads/{unique_filename}.jpg"
            await bot.download_file(file.file_path, destination=media_path)
            
        elif message.video:
            # ۳. اعمال محدودیت حجم منطقی (مثلاً ۲۰ مگابایت) برای جلوگیری از پر شدن RAM و هارد
            if message.video.file_size and message.video.file_size > 20 * 1024 * 1024:
                return await message.answer("⚠️ حجم ویدیو نباید بیشتر از ۲۰ مگابایت باشد.")
                
            media_type = "video"
            file_id = message.video.file_id
            file = await bot.get_file(file_id)
            media_path = f"downloads/{unique_filename}.mp4"
            await bot.download_file(file.file_path, destination=media_path)
            
        elif message.document:
            if message.document.file_size and message.document.file_size > 20 * 1024 * 1024:
                return await message.answer("⚠️ حجم فایل نباید بیشتر از ۲۰ مگابایت باشد.")
                
            media_type = "document"
            file_id = message.document.file_id
            
            # استخراج پسوند فایل اصلی برای حفظ فرمت
            ext = os.path.splitext(message.document.file_name)[1] if message.document.file_name else ".dat"
            file = await bot.get_file(file_id)
            media_path = f"downloads/{unique_filename}{ext}"
            await bot.download_file(file.file_path, destination=media_path)
            
    except Exception as e:
        logger.error(f"Error downloading media for order: {e}")
        return await message.answer("❌ خطا در دانلود و ذخیره فایل مدیا. لطفاً دوباره تلاش کنید.")

    # ذخیره در FSM
    await state.update_data(
        message_text=message_text,
        media_path=media_path,
        media_type=media_type
    )
    
    await state.set_state(CreateOrderStates.waiting_for_button)
    await message.answer(
        "🔘 **افزودن دکمه شیشه‌ای (اختیاری):**\n\n"
        "اگر می‌خواهید دکمه شیشه‌ای به پیام اضافه شود، آن را با فرمت زیر بفرستید:\n"
        "`متن دکمه - https://link.com`\n\n"
        "*(در غیر این صورت، روی /skip کلیک کنید.)*"
    )


@router.message(CreateOrderStates.waiting_for_button)
async def process_button_and_ask_schedule(message: types.Message, state: FSMContext) -> None:
    button_text = None
    button_url = None
    
    if message.text and message.text.strip().lower() != "/skip":
        parts = message.text.split("-", 1)
        if len(parts) == 2:
            button_text = parts[0].strip()
            button_url = parts[1].strip()
        else:
            return await message.answer("⚠️ فرمت نامعتبر! متن و لینک باید با خط تیره (-) جدا شوند یا /skip را بزنید.")

    await state.update_data(button_text=button_text, button_url=button_url)
    await state.set_state(CreateOrderStates.waiting_for_schedule)
    
    await message.answer(
        "⏱ <b>زمان‌بندی سفارش:</b>\n\n"
        "آیا می‌خواهید این سفارش الان اجرا شود یا برای زمان دیگری برنامه‌ریزی شود؟\n\n"
        "🔸 برای اجرای فوری دستور <code>/now</code> را ارسال کنید.\n"
        "🔸 برای زمان‌بندی، تاریخ و زمان را به فرمت زیر (بر اساس ساعت جهانی UTC) ارسال کنید:\n"
        "<code>YYYY-MM-DD HH:MM</code>\n"
        "<i>مثال: 2026-08-20 15:30</i>"
    )

@router.message(CreateOrderStates.waiting_for_schedule, F.text)
async def process_schedule_and_save(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    scheduled_time = None
    user_input = message.text.strip().lower()
    
    if user_input != "/now":
        try:
            # تبدیل رشته متنی به آبجکت زمان (UTC)
            dt_obj = datetime.strptime(message.text.strip(), "%Y-%m-%d %H:%M")
            scheduled_time = dt_obj.replace(tzinfo=timezone.utc)
            
            if scheduled_time < datetime.now(timezone.utc):
                return await message.answer("⚠️ زمان وارد شده در گذشته است! لطفاً زمان معتبری در آینده وارد کنید.")
        except ValueError:
            return await message.answer("⚠️ فرمت تاریخ نامعتبر است! لطفاً دقیقاً مشابه مثال ارسال کنید یا /now را بزنید.")

    fsm_data = await state.get_data()
    
    new_order = Order(
        category_id=fsm_data["category_id"],
        order_type=fsm_data["order_type"],
        target_data=fsm_data["target_data"],
        message_text=fsm_data.get("message_text"),
        media_path=fsm_data.get("media_path"),
        media_type=fsm_data.get("media_type"),
        button_text=fsm_data.get("button_text"),
        button_url=fsm_data.get("button_url"),
        scheduled_for=scheduled_time # <-- فیلد زمان‌بندی اضافه شد
    )
    
    session.add(new_order)
    await session.commit()
    await state.clear()
    
    time_msg = "فوری (اکنون)" if user_input == "/now" else f"برنامه‌ریزی شده برای {scheduled_time.strftime('%Y-%m-%d %H:%M')} UTC"
    await message.answer(f"🎉 <b>سفارش با موفقیت ثبت شد!</b>\n\nزمان اجرا: <b>{time_msg}</b>\nتارگت‌ها و مدیا به صف سیستم اضافه شدند.")

from database.models import OrderStatus

# ==========================================
# ACTIVE ORDERS & KILL SWITCH
# ==========================================
@router.callback_query(F.data == "menu_active_orders/")
async def list_active_orders(callback: types.CallbackQuery, session: AsyncSession) -> None:
    await callback.answer()
    
    # پیدا کردن سفارشاتی که هنوز تمام نشده‌اند
    stmt = select(Order).where(Order.status.in_([OrderStatus.pending, OrderStatus.running]))
    result = await session.execute(stmt)
    active_orders = result.scalars().all()
    
    if not active_orders:
        return await callback.message.edit_text(
            "✅ <b>هیچ سفارش فعالی وجود ندارد.</b>\n\nتمامی کمپین‌ها به اتمام رسیده‌اند.",
            reply_markup=InlineKeyboardBuilder().button(text="🔙 بازگشت", callback_data="menu_home/").as_markup()
        )
        
    builder = InlineKeyboardBuilder()
    for order in active_orders:
        # نمایش آیکون متفاوت بر اساس وضعیت
        status_emoji = "⏳" if order.status == OrderStatus.pending else "🚀"
        button_text = f"{status_emoji} لغو سفارش #{order.id}"
        builder.button(text=button_text, callback_data=f"cancel_order_{order.id}/")
        
    builder.button(text="🔙 بازگشت به منو", callback_data="menu_home/")
    builder.adjust(1)
    
    await callback.message.edit_text(
        "🛑 <b>مدیریت سفارشات فعال</b>\n\n"
        "لیست زیر شامل کمپین‌های در حال اجرا یا در صف انتظار است.\n"
        "<i>برای توقف اضطراری (Kill Switch) روی هر سفارش کلیک کنید:</i>",
        reply_markup=builder.as_markup()
    )

@router.callback_query(F.data.startswith("cancel_order_") & F.data.endswith("/"))
async def cancel_order_handler(callback: types.CallbackQuery, session: AsyncSession) -> None:
    order_id_str = callback.data.replace("cancel_order_", "").replace("/", "")
    
    if not order_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.")
        
    order_id = int(order_id_str)
    
    stmt = select(Order).where(Order.id == order_id)
    result = await session.execute(stmt)
    order = result.scalar_one_or_none()
    
    if not order or order.status not in [OrderStatus.pending, OrderStatus.running]:
        await callback.answer("⚠️ این سفارش قبلاً لغو شده یا وجود ندارد.", show_alert=True)
        return await list_active_orders(callback, session)
        
    # اعمال Kill Switch: تغییر وضعیت به ارور و پاک کردن تارگت‌های باقی‌مانده از صف دیتابیس
    order.status = OrderStatus.error
    order.target_data = "" 
    
    await session.commit()
    await callback.answer(f"✅ سفارش #{order_id} با موفقیت متوقف شد و از صف ارسال خارج گردید.", show_alert=True)
    
    # رفرش کردن لیست سفارشات فعال
    await list_active_orders(callback, session)

from aiogram.filters import Command
import os

@router.message(Command("cancel"))
@router.message(F.text == "❌ انصراف")
async def cancel_order_creation(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state is None:
        return
        
    # بررسی وجود فایل مدیا در استیتِ رها شده
    fsm_data = await state.get_data()
    media_path = fsm_data.get("media_path")
    
    if media_path and os.path.exists(media_path):
        try:
            os.remove(media_path)
            logger.info(f"Garbage Collection: Deleted abandoned media {media_path}")
        except Exception as e:
            logger.error(f"Failed to delete abandoned media: {e}")
            
    await state.clear()
    await message.answer("🚫 عملیات ثبت سفارش لغو شد و فایل‌های موقت پاکسازی شدند.", reply_markup=types.ReplyKeyboardRemove())