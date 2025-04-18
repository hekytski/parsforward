import re
import os
import nest_asyncio
import asyncio
import time
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import InputMediaPhoto
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import ValueRenderOption

nest_asyncio.apply()

# Telegram API
api_id = 23666805
api_hash = "03adb57619a5ed7c7cace0a4a8947d04"

# Сессия
os.makedirs("sessions", exist_ok=True)
session_path = os.path.abspath("sessions/parsiv_forwarder")
app = Client(session_path, api_id=api_id, api_hash=api_hash)

# Google Sheets
doc_name = "ВЕСЕННИЙ ПРИЗЫВ 2025"
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
gs_client = gspread.authorize(creds)

spreadsheet = gs_client.open(doc_name)
sheet_ugresh = spreadsheet.worksheet("Розыск фам")
sheet_pep = spreadsheet.worksheet("Розыск ЕПП фам")
log_sheet = spreadsheet.worksheet("ЛОГ")

# 🧾 Список user_id по району
region_to_user_ids = {
    "Нагатинский затон": [1063427278],
    "Орехово-Борисово-Южное": [5169314683],
    "Донской": [416354641],
    "Братеево": [1602344399],
    "Чертаново-Южное": [212701167],
    "Чертаново-Центральное": [5292457876],
    "Москворечье-Сабурово": [1186911400],
    "Нагатино-Садовники": [1483860553],
    "Зябликово": [908765228],
    "Орехово-Борисово-Северное": [5297001151],
    "Бирюлево-Восточное": [5236483739],
    "Нагорный": [1565990655, 343441896],
    "Бирюлево-Западное": [278637133],
    "Даниловский": [236008528],
    "Царицыно": [5692734299],
    "Чертаново-Северное": [423475579]
}

def extract_fio_dob(text):
    match = re.search(r"([А-Яа-яЁёA-Za-z]+ [А-Яа-яЁёA-Za-z]+ [А-Яа-яЁёA-Za-z]+) (\d{2}\.\d{2}\.\d{4})", text)
    return (match.group(1).strip(), match.group(2).strip()) if match else (None, None)

def extract_camera_address(text):
    match = re.search(r"Камера: .*?\| (.+)", text)
    return match.group(1).strip() if match else ""

def normalize_address(address):
    address = re.sub(r'[^\w\s]', ' ', address)
    address = re.sub(r'\s+', ' ', address).strip().lower()
    return address

def soft_address_match(addr1, addr2, threshold=0.6):
    tokens1 = set(normalize_address(addr1).split())
    tokens2 = set(normalize_address(addr2).split())
    if not tokens1 or not tokens2:
        return False
    return len(tokens1 & tokens2) / min(len(tokens1), len(tokens2)) >= threshold

def get_hits(sheet, col_index, value):
    return sum(1 for v in sheet.col_values(col_index) if v.strip() == value.strip())

def find_region(fio, sheet_name, fio_col, region_col):
    sheet = spreadsheet.worksheet(sheet_name)
    all_rows = sheet.get_all_values()
    for idx, row in enumerate(all_rows[1:], start=2):
        if len(row) > max(fio_col, region_col) and row[fio_col].strip().lower() == fio.lower():
            return row[region_col].strip(), row, idx
    return None, None, None

@app.on_message((filters.text | filters.caption) & filters.user([1953996829, 264271992]))
async def handle_parsiv_message(client, message):
    raw_text = message.text or message.caption or ""
    if not raw_text.strip():
        print("❗ Пустое сообщение — пропускаем")
        return

    fio, dob = extract_fio_dob(raw_text)
    camera_address = extract_camera_address(raw_text)
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    link_to_case = ""

    if not fio or not dob:
        print("❗ Не удалось извлечь ФИО или ДР")
        return

    text_lower = raw_text.lower()
    region = None
    registration_address = ""
    full_row = None
    row_index = None

    if "мониторинг угреш" in text_lower:
        region, full_row, row_index = find_region(fio, "Розыск фам", 1, 3)
        sheet = sheet_ugresh
        if full_row and row_index:
            registration_address = full_row[5] if len(full_row) > 5 else ""
            cell_j = sheet.cell(row_index, 10, value_render_option=ValueRenderOption.formula).value
            match = re.search(r'"(https?://[^"]+)"', cell_j)
            if match:
                link_to_case = f"\n🔗 Ссылка на обращение: {match.group(1)}"

    elif "мониторинг пеп" in text_lower:
        region, full_row, row_index = find_region(fio, "Розыск ЕПП фам", 0, 11)
        sheet = sheet_pep
        if full_row and row_index:
            registration_address = full_row[9] if len(full_row) > 9 else ""
            cell_j = sheet.cell(row_index, 10, value_render_option=ValueRenderOption.formula).value
            match = re.search(r'"(https?://[^"]+)"', cell_j)
            if match:
                link_to_case = f"\n🔗 Ссылка на обращение: {match.group(1)}"
    else:
        print("❗ Неизвестный тип мониторинга")
        return

    if not region:
        print(f"❗ Район не найден для: {fio}")
        return

    user_ids = region_to_user_ids.get(region, [])
    if not user_ids:
        print(f"❗ Район '{region}' не в списке user_id")
        return

    fio_hits = get_hits(log_sheet, 1, fio)
    address_hits = get_hits(log_sheet, 5, camera_address)

    address_match_note = ""
    is_address_match = camera_address and registration_address and soft_address_match(camera_address, registration_address)
    if is_address_match:
        address_match_note = (
            f"\n✅ АДРЕС СРАБОТКИ СОВПАДАЕТ С АДРЕСОМ РЕГИСТРАЦИИ"
            f"\nАдрес регистрации: {registration_address}"
        )

    details_message = (
        f"# Сработок по данному человеку: {fio_hits + 1}\n"
        f"# Сработок по данному адресу: {address_hits + 1}"
        f"{link_to_case}{address_match_note}"
    )

    try:
        if message.media_group_id:
            group_msgs = await client.get_media_group(chat_id=message.chat.id, message_id=message.id)
            media_group = []
            for i, msg in enumerate(group_msgs):
                if msg.photo:
                    media_group.append(InputMediaPhoto(
                        media=msg.photo.file_id,
                        caption=raw_text if i == 0 else None
                    ))
        else:
            media_group = None

        for uid in user_ids:
            try:
                if media_group:
                    await client.send_media_group(uid, media=media_group)
                    time.sleep(1.5)
                else:
                    await client.copy_message(uid, message.chat.id, message.id)
                    time.sleep(1.5)

                await client.send_message(uid, details_message)
                print(f"✅ Переслано: {fio} → {region} → {uid}")

            except Exception as e:
                print(f"❌ Ошибка при пересылке на {uid}: {e}")
                if "PEER_ID_INVALID" in str(e):
                    print(f"⚠️ Пользователь {uid} ещё не активировал чат. Пропускаем.")
                elif "FLOOD_WAIT" in str(e):
                    print("⏳ Telegram просит подождать. Пауза 2 сек.")
                    time.sleep(2)
                try:
                    await client.send_message("me", f"❌ Ошибка при пересылке:\n{e}\n\n{raw_text}"[:4000])
                except Exception as inner_e:
                    print(f"⚠️ Ошибка при уведомлении: {inner_e}")

    except Exception as e:
        print(f"❌ Неизвестная ошибка при обработке сообщения:\n{e}")

    try:
        log_sheet.append_row([
            fio, dob, region, now, camera_address,
            registration_address, fio_hits + 1, address_hits + 1,
            "ДА" if is_address_match else ""
        ])
        print("📝 Лог записан")
    except Exception as e:
        print(f"⚠️ Ошибка при записи в лог: {e}")

# 🚀 Запуск
async def start_bot():
    try:
        await app.stop()
    except:
        pass
    await app.start()
    print("✅ Бот запущен. Жду сообщений...")
    await asyncio.Event().wait()

await start_bot()
