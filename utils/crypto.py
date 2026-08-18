import os
from cryptography.fernet import Fernet

# کلید رمزنگاری را از متغیر محیطی می‌گیریم
# توجه: یک کلید Fernet با این دستور تولید کن و در .env قرار بده: Fernet.generate_key().decode()
SECRET_KEY = os.getenv("FERNET_KEY") 

if not SECRET_KEY:
    # فال‌بک موقت برای جلوگیری از کرش (در پروداکشن حتما کلید را در .env بگذارید)
    SECRET_KEY = Fernet.generate_key().decode()
    os.environ["FERNET_KEY"] = SECRET_KEY

cipher = Fernet(SECRET_KEY.encode())

def encrypt_session(session_string: str) -> str:
    """رمزنگاری سشن استرینگ قبل از ذخیره در دیتابیس"""
    if not session_string:
        return None
    return cipher.encrypt(session_string.encode()).decode()

def decrypt_session(encrypted_string: str) -> str:
    """رمزگشایی سشن استرینگ پس از واکشی از دیتابیس"""
    if not encrypted_string:
        return None
    return cipher.decrypt(encrypted_string.encode()).decode()