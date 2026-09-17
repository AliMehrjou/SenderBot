import logging
import html

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command

from bot.keyboards.main_menu import get_main_menu_keyboard
from workers.session_manager import worker_pool
from bot.keyboards.cancel import with_cancel_hint
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer
from config import config

logger = logging.getLogger(__name__)

router = Router(name="general_handlers_router")

# ==========================================
# VERIFY JOIN HANDLER
# ==========================================
@router.callback_query(F.data == "menu_verify_join/")
async def verify_join_callback(callback: types.CallbackQuery) -> None:
    bot = callback.bot
    not_joined_channels = []

    for channel in config.FORCE_JOIN_CHANNEL_LIST:
        try:
            chat_member = await bot.get_chat_member(chat_id=channel, user_id=callback.from_user.id)
            if chat_member.status in ["left", "kicked", "banned"]:
                not_joined_channels.append(channel)
        except Exception as e:
            logger.warning(f"Verify-join: membership check failed for {channel}: {e}")
            not_joined_channels.append(channel)

    if not_joined_channels:
        channels_list = "\n".join(f"• {ch}" for ch in not_joined_channels)
        return await callback.answer(
            "❌ عضویت شما هنوز تایید نشده است!\n\n"
            f"لطفاً ابتدا در کانال(های) زیر عضو شوید:\n{channels_list}",
            show_alert=True
        )

    await callback.answer("✅ عضویت شما تایید شد!", show_alert=True)
    
    await callback.message.edit_text(
        "🎛 <b>پنل کنترل اصلی</b>\n\nلطفاً یک گزینه را انتخاب کنید:",
        reply_markup=get_main_menu_keyboard()
    )

# ==========================================
# 🌟 HELP MENU ENGINE (Dynamic & Beautiful)
# ==========================================

HELP_DATA = {
  "main_menu_help": {
    "title": "📖 راهنمای جامع و دوستانه کار با ربات",
    "text": "به راهنمای جامع ربات ما خوش آمدید! 🌸\nما تمام تلاشمان را کرده‌ایم تا فرآیندهای ارسال انبوه، استخراج ممبر و مدیریت اکانت‌ها را به هوشمندانه‌ترین و ساده‌ترین شکل ممکن طراحی کنیم. هدف ما این است که بدون درگیر شدن با تنظیمات پیچیده، بالاترین بازدهی را با خیالی آسوده تجربه کنید.\n\nبرای آشنایی بیشتر با هر بخش، لطفاً روی دکمه دلخواهتان کلیک بفرمایید: 👇",
    "buttons": [
      [
        {"text": "📋 دستورات و میانبرها", "callback_data": "help:shortcuts"},
        {"text": "🚀 ثبت سفارش و ارسال", "callback_data": "help:orders"}
      ],
      [
        {"text": "👥 استخراج ممبر", "callback_data": "help:extractor"},
        {"text": "📱 مدیریت اکانت‌ها و استراحت", "callback_data": "help:accounts"}
      ],
      [
        {"text": "🛠 ابزارها و بنرها", "callback_data": "help:tools"},
        {"text": "❓ سوالات و رفع اشکال", "callback_data": "help:faq"}
      ]
    ]
  },
  "sections": {
    "shortcuts": {
      "title": "📋 دستورات میانبر و کلیدهای اصلی",
      "content": "در هر کجای مسیر که باشید، این دستورات کوتاه مثل یک میانبر سریع به کمک شما می‌آیند. کافیست روی آن‌ها لمس کنید:\n\n🔹 <code>/start</code> : شروعی دوباره! ربات را از نو راه‌اندازی می‌کند و منوی اصلی را برایتان می‌آورد.\n\n🔹 <code>/restart</code> یا دکمه <b>🔄 ریستارت ربات</b> : اگر در مرحله‌ای گیر کردید یا نظرتان عوض شد، با این دستور ربات فوراً همه‌چیز را لغو کرده و به حالت اولیه برمی‌گردد.\n\n🔹 <code>/cancel</code> یا دکمه <b>❌ انصراف</b> : اگر از انجام مرحله فعلی منصرف شدید، این دستور شما را یک قدم به عقب می‌برد.\n\n🔹 <code>/help</code> : نمایش همین صفحه راهنمای دوستانه.\n\n🔹 <code>/status</code> : یک نگاه سریع به وضعیت سرور، اکانت‌های متصل و کارهایی که در حال انجام است.\n\n🔹 <code>/accounts</code> : رفتن به بخش مدیریت اکانت‌ها با یک کلیک.\n\n🔹 <code>/extract</code> : ورود مستقیم و سریع به بخش استخراج ممبر.",
      "back_button": {"text": "🔙 بازگشت به منوی راهنما", "callback_data": "help:main"}
    },
    "orders": {
      "title": "🚀 راهنمای ثبت سفارش ارسال انبوه",
      "content": "برای اینکه ارسال انبوه شما به بهترین شکل انجام شود، این مسیر ساده را با هم طی می‌کنیم:\n\n۱️⃣ <b>ورود به بخش ارسال:</b> از منوی اصلی، با خیالی راحت دکمه <b>«ثبت سفارش ارسال»</b> را انتخاب کنید.\n\n۲️⃣ <b>آماده‌سازی پیام:</b> می‌توانید متن زیبای خود را همینجا بنویسید یا از بین بنرهایی که قبلاً ذخیره کرده‌اید یکی را انتخاب کنید (البته امکان ارسال عکس هم وجود دارد!).\n\n۳️⃣ <b>جادوی متن تصادفی (اسپین‌تکس):</b> برای اینکه تلگرام حساس نشود، پیشنهاد می‌کنیم متن خود را با این فرمت بنویسید: <code>{سلام|درود|وقت بخیر}</code>. ربات ما خودش زحمت می‌کشد و برای هر شخص یکی از این کلمات را به صورت تصادفی قرار می‌دهد.\n\n۴️⃣ <b>لیست مخاطبان شما:</b> لطفاً فایل متنی (<code>.txt</code>) خود را که شامل آیدی‌ها یا نام‌های کاربری است، برای ربات بفرستید (در هر خط فقط یک آیدی).\n\n۵️⃣ <b>پیش‌نمایش قبل از ارسال:</b> ما همیشه قبل از شروع کار، یک نمونه واقعی از پیامتان را به شما نشان می‌دهیم تا خیالتان از بابت ظاهر آن راحت شود.\n\n۶️⃣ <b>تخمین هوشمند زمان:</b> سیستم ما به شما می‌گوید که این ارسال چقدر زمان می‌برد و وضعیت ایمنی اکانت‌ها به چه صورت است.\n\n۷️⃣ <b>کنترل پنل زنده:</b> پس از تایید، پیشرفت کار را به صورت لحظه‌ای در یک پیام می‌بینید و هر زمان که خواستید می‌توانید آن را موقتاً متوقف یا کاملاً لغو کنید.",
      "back_button": {"text": "🔙 بازگشت به منوی راهنما", "callback_data": "help:main"}
    },
    "extractor": {
      "title": "👥 راهنمای استخراج ممبر از گروه‌ها",
      "content": "این بخش به شما کمک می‌کند تا به راحتی مخاطبان هدف خود را پیدا کنید:\n\n"
                 "۱️⃣ <b>شروع کار:</b> لطفاً دکمه <b>«آنالیز»</b> (یا استفاده از میانبر /extract) را انتخاب کنید.\n\n"
                 "۲️⃣ <b>معرفی گروه:</b> لینک عمومی یا لینک دعوت (Join Link) گروه مدنظرتان را ارسال کنید.\n\n"
                 "۳️⃣ <b>انتخاب استراتژی:</b> برای دریافت بهترین نتیجه، یکی از ۴ حالت زیر را انتخاب کنید:\n"
                 "   ▫️ <b>👥 همه اعضا:</b> کل لیست اعضای گروه را استخراج می‌کند.\n"
                 "   ▫️ <b>💬 فرستندگان پیام:</b> فقط افراد فعال در چت گروه را پیدا می‌کند (عالی برای گروه‌هایی که ادمین لیست اعضا را مخفی کرده است!).\n"
                 "   ▫️ <b>🥇 طلایی:</b> ترکیبی هوشمند از لیست اعضا و فعالیت واقعی آن‌ها.\n"
                 "   ▫️ <b>🟢 فقط آنلاین:</b> کاربرانی که اخیراً در تلگرام آنلاین بوده‌اند.\n\n"
                 "۴️⃣ <b>دریافت فایل نهایی:</b> ربات کار خود را در پس‌زمینه انجام داده و در نهایت یک فایل متنی (txt) تر و تمیز به شما تحویل می‌دهد که مستقیماً در بخش «ثبت سفارش» قابل استفاده است.",
      "back_button": {"text": "🔙 بازگشت به منوی راهنما", "callback_data": "help:main"}
    },
    "accounts": {
      "title": "📱 مدیریت اکانت‌ها و قانون استراحت",
      "content": "اکانت‌های شما سرمایه‌های اصلی شما هستند و ما به شدت مراقب سلامت آن‌ها هستیم 🛡:\n\n➕ <b>اضافه کردن اکانت جدید:</b>\nاز بخش «مدیریت اکانت‌ها»، دکمه افزودن را بزنید. شماره همراهتان را با کد کشور وارد کنید، کدی که تلگرام می‌فرستد را به ما بدهید و تمام!\n\n🚦 <b>وضعیت اکانت‌ها به چه معناست؟</b>\n🟢 <b>آماده به کار:</b> اکانت کاملاً سرحال است و منتظر دستور شماست.\n🟡 <b>در حال استراحت (Cooldown):</b> برای اینکه تلگرام روی اکانت حساس نشود، ربات پس از ارسال پیام‌ها، اکانت را ۲۴ ساعت به مرخصی می‌فرستد. پس از این زمان، اکانت خودکار برمی‌گردد سر کار!\n🔴 <b>محدود شده:</b> متأسفانه این اکانت به دلیل گزارش کاربران موقتاً محدود شده است.\n⚪️ <b>آفلاین:</b> ارتباط ربات با این اکانت قطع شده و لطفاً دوباره آن را اضافه کنید.\n\n⚡️ <i>خیالتان راحت باشد: شما نیازی به تنظیم زمان‌بندی ندارید. ربات ما پیام‌ها را به آرامی و با فواصل زمانی ایمن بین اکانت‌های سالم پخش می‌کند تا هیچ مشکلی پیش نیاید.</i>",
      "back_button": {"text": "🔙 بازگشت به منوی راهنما", "callback_data": "help:main"}
    },
    "tools": {
      "title": "🛠 ابزارها و مدیریت بنرها",
      "content": "برای راحتی بیشتر شما، امکانات فوق‌العاده‌ای در منوی «ابزارها» قرار داده‌ایم:\n\n🖼 <b>ویترین بنرها (Banners):</b>\nمتن‌ها و عکس‌هایی که زیاد استفاده می‌کنید را یک‌بار اینجا ذخیره کنید. دفعات بعد فقط با یک کلیک آن‌ها را برای ارسال انتخاب کنید و از تایپ مجدد راحت شوید.\n\n🧹 <b>پاک‌سازی لیست‌ها:</b>\nابزاری عالی برای اینکه فایل‌های آیدی خود را مرتب کنید، تکراری‌ها را دور بریزید و یک لیست بی‌نقص بسازید.\n\n📊 <b>آمار و گزارش‌ها:</b>\nمی‌توانید کارهای گذشته، تعداد پیام‌های موفق و تاریخچه سفارش‌هایتان را به طور کامل در این بخش مرور کنید.",
      "back_button": {"text": "🔙 بازگشت به منوی راهنما", "callback_data": "help:main"}
    },
    "faq": {
      "title": "❓ سوالات متداول و حل مشکلات",
      "content": "💡 <b>چرا ربات از من می‌خواهد در کانال‌ها عضو شوم؟</b>\nبرای اینکه بتوانیم خدمات بهتری ارائه دهیم و ربات برای شما فعال بماند، خواهشمندیم در این دو کانال اسپانسر عضو شوید:\n1️⃣ @linkdoonifun\n2️⃣ @robotsfunlink\nبعد از عضویت، روی «✅ عضو شدم / بررسی مجدد» بزنید تا قفل ربات باز شود.\n\n💡 <b>اگر در میان منوها گم شدم یا خواستم کاری را لغو کنم چه کار کنم؟</b>\nجای نگرانی نیست! دستور <code>/restart</code> را در چت بنویسید یا دکمه «🔄 ریستارت ربات» را بزنید. ربات همه‌چیز را پاک کرده و منوی اصلی را تقدیمتان می‌کند.\n\n💡 <b>آیا لازم است خودم سرعت ارسال پیام‌ها را تنظیم کنم؟</b>\nخیر به هیچ وجه! سیستم ضداسپم داخلی ما، به صورت هوشمندانه و خودکار فواصل زمانی ارسال و استراحت اکانت‌ها را مدیریت می‌کند تا اکانت‌های شما در امن‌ترین حالت ممکن بمانند.\n\n💡 <b>آیا زمان‌بندی‌ها بر اساس ساعت ایران هستند؟</b>\nبله، تمام ساعت‌ها، زمان استراحت‌ها و گزارش‌هایی که می‌بینید دقیقاً با ساعت رسمی ایران (Asia/Tehran) تنظیم شده‌اند.",
      "back_button": {"text": "🔙 بازگشت به منوی راهنما", "callback_data": "help:main"}
    }
  }
}

# 1. تابع سازنده منوی اصلی راهنما
def get_help_main_keyboard():
    builder = InlineKeyboardBuilder()
    buttons_layout = HELP_DATA["main_menu_help"]["buttons"]
    
    # چیدمان کاملاً داینامیک بر اساس ردیف‌های تعیین شده در دیکشنری
    for row in buttons_layout:
        for btn in row:
            builder.button(text=btn["text"], callback_data=btn["callback_data"])
            
    # محاسبه تعداد دکمه در هر ردیف برای adjust شدن دقیق
    builder.adjust(*[len(row) for row in buttons_layout])
    
    # اضافه کردن دکمه بازگشت به منوی اصلی ربات در انتهای منو
    builder.row(types.InlineKeyboardButton(text="🏛 بازگشت به پنل اصلی", callback_data="menu_home/"))
    return builder.as_markup()

# 2. هندلرهای ورود به منوی راهنما (/help و دکمه شیشه‌ای)
@router.callback_query(F.data == "menu_help/")
async def show_help_menu(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    main_text = f"<b>{HELP_DATA['main_menu_help']['title']}</b>\n\n{HELP_DATA['main_menu_help']['text']}"
    await safe_edit_or_answer(callback.message, main_text, reply_markup=get_help_main_keyboard())

@router.message(Command("help"))
async def show_help_command(message: types.Message, state: FSMContext) -> None:
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    main_text = f"<b>{HELP_DATA['main_menu_help']['title']}</b>\n\n{HELP_DATA['main_menu_help']['text']}"
    await message.answer(main_text, reply_markup=get_help_main_keyboard())


# 3. 🧠 هندلر هوشمند و داینامیک زیرمنوها (جادوی اصلی اینجاست!)
@router.callback_query(F.data.startswith("help:"))
async def handle_dynamic_help_sections(callback: types.CallbackQuery, state: FSMContext) -> None:
    section_key = callback.data.split(":")[1]
    
    # اگر کاربر دکمه بازگشت به منوی اصلی راهنما را زد
    if section_key == "main":
        return await show_help_menu(callback, state)
        
    # واکشی اطلاعات بخش مربوطه از دیکشنری
    section_data = HELP_DATA["sections"].get(section_key)
    
    if not section_data:
        return await callback.answer("⚠️ این بخش در حال بروزرسانی است.", show_alert=True)
        
    # ساخت متن نهایی با استایل بسیار زیبا
    text = f"🌟 <b>{section_data['title']}</b>\n\n{section_data['content']}"
    
    # ساخت دکمه‌های بازگشت (یکی برای راهنما، یکی برای پنل کل ربات)
    builder = InlineKeyboardBuilder()
    builder.button(text=section_data["back_button"]["text"], callback_data=section_data["back_button"]["callback_data"])
    builder.button(text="🏛 منوی اصلی ربات", callback_data="menu_home/")
    builder.adjust(1, 1)
    
    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

# ==========================================
# COMING SOON / IGNORE HANDLERS
# ==========================================
@router.callback_query(F.data.in_(["menu_coming_soon_example/"]))
async def handle_coming_soon_menus(callback: types.CallbackQuery) -> None:
    await callback.answer("⏳ این بخش در حال توسعه است و به زودی اضافه خواهد شد!", show_alert=True)

@router.callback_query(F.data == "pagination_info_ignore/")
async def ignore_pagination_info(callback: types.CallbackQuery) -> None:
    await callback.answer()

# ==========================================
# --- استیت‌های مربوط به CRM ---
# ==========================================
class CRMStates(StatesGroup):
    waiting_for_reply = State()

def get_crm_cancel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_crm_reply/")
    return builder.as_markup()

# ==========================================
# هندلر کلیک روی دکمه پاسخ CRM
# ==========================================
@router.callback_query(F.data.startswith("crm_reply_"))
async def crm_reply_callback(callback: types.CallbackQuery, state: FSMContext) -> None:
    parts = callback.data.split("_")
    
    if len(parts) != 4:
        return await callback.answer("⚠️ دیتای نامعتبر.", show_alert=True)

    worker_id = parts[2]
    target_id = parts[3]

    await state.update_data(crm_worker_id=worker_id, crm_target_id=target_id)
    await state.set_state(CRMStates.waiting_for_reply)

    await callback.message.reply(
        with_cancel_hint(
            f"✍️ <b>ارسال پاسخ به تارگت:</b> <code>{target_id}</code>\n"
            f"🤖 <b>از طریق اکانت ورکر:</b> <code>{worker_id}</code>\n\n"
            "لطفاً متن پاسخ خود را ارسال کنید:"
        ),
        reply_markup=get_crm_cancel_keyboard()
    )
    await callback.answer()

# ==========================================
# هندلر لغو پاسخ‌گویی CRM
# ==========================================
@router.callback_query(F.data == "cancel_crm_reply/")
async def cancel_crm_reply(callback: types.CallbackQuery, state: FSMContext) -> None:
    if await state.get_state() is None:
        await callback.answer("هیچ عملیاتی فعال نبود.")
        return await safe_edit_or_answer(
            callback.message,
            "🏛 شما به منوی اصلی بازگشتید.",
            reply_markup=get_main_menu_keyboard()
        )
    
    await state.clear()
    await callback.answer("عملیات لغو شد.")
    
    await safe_edit_or_answer(
        callback.message,
        "❌ عملیات پاسخ‌گویی لغو شد.",
        reply_markup=get_main_menu_keyboard()
    )

# ==========================================
# هندلر دریافت متن و ارسال با Pyrogram (CRM)
# ==========================================
@router.message(CRMStates.waiting_for_reply)
async def send_crm_reply(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    worker_id_str = data.get("crm_worker_id")
    target_id = data.get("crm_target_id")

    if not worker_id_str or not target_id:
        await state.clear()
        return await message.answer("⚠️ اطلاعات نشست از دست رفته است. لطفاً دوباره روی دکمه پاسخ کلیک کنید.")

    reply_text = message.text or message.caption or ""
    if not reply_text:
        return await message.answer(with_cancel_hint("⚠️ لطفاً فقط متن ارسال کنید."))

    try:
        worker_id_int = int(worker_id_str)
    except ValueError:
        await state.clear()
        return await message.answer("⚠️ خطای سیستمی: آیدی ورکر نامعتبر است.")

    client = worker_pool.get(worker_id_int)
    
    if not client or not client.is_connected:
        await state.clear()
        return await message.answer(
            f"⚠️ <b>ارسال ناموفق:</b>\n"
            f"اکانت ورکر <code>{worker_id_str}</code> در حال حاضر آفلاین است یا اتصال آن با تلگرام قطع شده است."
        )

    try:
        await client.send_message(chat_id=int(target_id), text=reply_text)
        await message.answer(f"✅ <b>پیام شما با موفقیت ارسال شد!</b>\n👤 <b>مقصد:</b> <code>{target_id}</code>")
    except Exception as e:
        await message.answer(f"❌ <b>خطا در ارسال پیام:</b>\n<code>{html.escape(str(e))}</code>")
    finally:
        await state.clear()
async def safe_pin_message(bot, chat_id: int, message_id: int) -> bool:
    """
    سعی می‌کند پیام را پین کند. در صورت بروز محدودیت‌های تلگرام یا خطای دسترسی،
    خطا لاگ می‌شود اما ربات کرش نمی‌کند.
    """
    try:
        await bot.pin_chat_message(
            chat_id=chat_id, 
            message_id=message_id, 
            disable_notification=True
        )
        return True
    except Exception as e:
        logger.warning(f"Pin message failed in chat {chat_id} for message {message_id}: {e}")
        return False

async def safe_unpin_message(bot, chat_id: int, message_id: int) -> bool:
    """
    سعی می‌کند پیام را آنپین کند. 
    """
    try:
        await bot.unpin_chat_message(
            chat_id=chat_id, 
            message_id=message_id
        )
        return True
    except Exception as e:
        logger.warning(f"Unpin message failed in chat {chat_id} for message {message_id}: {e}")
        return False
    
# ==========================================
# 🌟 SHORTCUT COMMANDS HANDLERS (میانبرها)
# ==========================================

@router.message(Command("extract"))
async def shortcut_extract_command(message: types.Message, state: FSMContext) -> None:
    """هندلر میانبر /extract برای ورود سریع به بخش استخراج"""
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🌐 ورود به بخش استخراج", callback_data="menu_analysis/")
    
    await message.answer(
        "👇 برای ورود به بخش استخراج ممبر (آنالیز) کلیک کنید:", 
        reply_markup=builder.as_markup()
    )

@router.message(Command("accounts"))
async def shortcut_accounts_command(message: types.Message, state: FSMContext) -> None:
    """هندلر میانبر /accounts برای مدیریت اکانت‌ها"""
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📲 ورود به مدیریت اکانت‌ها", callback_data="menu_list_accounts/")
    
    await message.answer(
        "👇 برای ورود به بخش لیست و مدیریت اکانت‌ها کلیک کنید:", 
        reply_markup=builder.as_markup()
    )

@router.message(Command("status"))
async def shortcut_status_command(message: types.Message, state: FSMContext) -> None:
    """هندلر میانبر /status برای مشاهده وضعیت سرور"""
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 مشاهده آمار و وضعیت", callback_data="menu_stats/")
    
    await message.answer(
        "👇 برای مشاهده آمار سیستم و وضعیت اکانت‌ها کلیک کنید:", 
        reply_markup=builder.as_markup()
    )

@router.message(Command("restart"))
async def shortcut_restart_command(message: types.Message, state: FSMContext) -> None:
    """هندلر میانبر /restart برای بازگشت به حالت اولیه"""
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    # استفاده از کیبورد منوی اصلی که در بالای فایل ایمپورت شده است
    await message.answer(
        "🔄 ربات با موفقیت ریستارت شد و به حالت اولیه بازگشت.", 
        reply_markup=get_main_menu_keyboard()
    )