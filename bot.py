import os
import re
import shutil
import asyncio
import zipfile
import html
import gc
import threading
import xml.etree.ElementTree as ET
from flask import Flask
from docx import Document
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.error import RetryAfter

# ==========================================
# CONFIGURATION
# ==========================================
TOKEN = "8712072214:AAEJl5SW1TPisPZb7tiQbYolv-QlDvo_tTU" # Your Bot Token
DEFAULT_DOCX_CHUNK = 50
DEFAULT_EPUB_CHUNK = 500

# State Management
document_queue = None
user_chunk_sizes = {}
pending_uploads = {}

# ==========================================
# LIGHTNING WRITER & EXTRACTOR (Render Memory Safe)
# ==========================================
def fast_read_docx(input_path):
    """Clean XML text extraction for DOCX. Skips heavy library to save RAM."""
    lines = []
    try:
        with zipfile.ZipFile(input_path, 'r') as docx_zip:
            xml_content = docx_zip.read('word/document.xml')
            tree = ET.fromstring(xml_content)
            ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
            for para in tree.findall('.//w:p', namespaces=ns):
                texts = [t.text for t in para.findall('.//w:t', namespaces=ns) if t.text]
                if texts:
                    lines.append("".join(texts).strip())
    except Exception as e:
        print(f"⚠️ XML Extraction Failed: {e}")
    return lines

def fast_html_to_text(raw_html):
    """Clean Regex HTML parser."""
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', raw_html)
    text = re.sub(r'<(script|style|head)[^>]*>.*?</\1>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'</?(p|div|h[1-6]|br|tr|li)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    return [line.strip() for line in text.split('\n') if line.strip()]

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def save_chunk(lines, path, fmt):
    """⚡ Lightning Fast XML Writer. Bypasses 512MB RAM limits instantly."""
    print(f"💾 Fast-Saving {os.path.basename(path)}...")
    if fmt == 'txt':
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n\n'.join(lines))
    else:
        # 1. Instantly create a blank template
        doc = Document()
        doc.save(path)

        # 2. Generate raw XML strings instantly
        xml_paras = []
        for line in lines:
            safe_text = html.escape(line) # CRITICAL: Prevents XML corruption
            xml_paras.append(f'<w:p><w:r><w:t>{safe_text}</w:t></w:r></w:p>')

        body_xml = "".join(xml_paras)
        temp_path = path + ".tmp"

        # 3. Inject raw XML directly into the zipped DOCX structure
        with zipfile.ZipFile(path, 'r') as zin:
            with zipfile.ZipFile(temp_path, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    if item.filename == 'word/document.xml':
                        orig_xml = zin.read(item.filename).decode('utf-8')
                        new_xml = orig_xml.replace('<w:sectPr', body_xml + '<w:sectPr')
                        zout.writestr(item, new_xml.encode('utf-8'))
                    else:
                        zout.writestr(item, zin.read(item.filename))

        # Overwrite the blank file with our supercharged one
        os.replace(temp_path, path)
    gc.collect()

# ==========================================
# SPLITTER LOGIC (TXT & DOCX)
# ==========================================
def split_text_based_logic(input_path, output_dir, chunk_size, output_format, is_txt_file=False):
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    collector = []
    generated_files = []

    current_start = 1
    target_chapter = None
    first_chapter_found = False
    pattern_num = r"^(?:vol(?:ume)?\s*\d+\s*)?(?:chapter|ch|c|अध्याय|चैप्टर|#|नॉवेलटैप|उपन्यासटैप|सी|पेज|पृष्ठ|000)?\s*(\d+)(?:[:\s-]|$)"

    if is_txt_file:
        with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [line.strip() for line in f if line.strip()]
    else:
        lines = fast_read_docx(input_path)

    for text in lines:
        if text and not first_chapter_found:
            match = re.match(pattern_num, text, re.IGNORECASE)
            if match:
                detected_num = int(match.group(1))
                current_start = detected_num
                target_chapter = detected_num + chunk_size
                first_chapter_found = True

        is_boundary = False
        if text and first_chapter_found:
            match = re.match(pattern_num, text, re.IGNORECASE)
            if match and int(match.group(1)) == target_chapter:
                is_boundary = True

        if is_boundary:
            if collector:
                part_name = f"{current_start}_to_{target_chapter - 1}-{base_name}.{'txt' if output_format == 'txt' else 'docx'}"
                part_path = os.path.join(output_dir, part_name)
                save_chunk(collector, part_path, output_format)
                generated_files.append(part_path)

            collector = [text]
            current_start = target_chapter
            target_chapter += chunk_size
        else:
            collector.append(text)

    if collector:
        end_marker = "End" if first_chapter_found else "Full"
        part_name = f"{current_start}_to_{end_marker}-{base_name}.{'txt' if output_format == 'txt' else 'docx'}"
        part_path = os.path.join(output_dir, part_name)
        save_chunk(collector, part_path, output_format)
        generated_files.append(part_path)

    del lines
    gc.collect()
    return generated_files

def split_docx_logic(input_path, output_dir, chunk_size, output_format):
    return split_text_based_logic(input_path, output_dir, chunk_size, output_format, is_txt_file=False)

def split_txt_logic(input_path, output_dir, chunk_size, output_format):
    return split_text_based_logic(input_path, output_dir, chunk_size, output_format, is_txt_file=True)

# ==========================================
# SPLITTER LOGIC (10MB+ EPUB OPTIMIZED)
# ==========================================
def split_epub_logic(input_path, output_dir, chunk_size, output_format):
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    clean_name = re.sub(r'[^\w\-_]', '_', base_name)
    generated_files = []

    try:
        text_buffer = []
        chapter_count = 0
        chunk_count = 1

        with zipfile.ZipFile(input_path, 'r') as epub_zip:
            html_files = [f for f in epub_zip.namelist() if f.lower().endswith(('.html', '.xhtml', '.htm'))]
            html_files.sort(key=natural_sort_key)

            print(f"📦 Cracking {len(html_files)} internal HTML files from EPUB...")

            for i, file_name in enumerate(html_files):
                try:
                    content = epub_zip.read(file_name).decode('utf-8', errors='ignore')
                    lines = fast_html_to_text(content)

                    if lines:
                        text_buffer.extend(lines)
                        text_buffer.append("---")
                        chapter_count += 1

                    if chapter_count >= chunk_size:
                        part_name = f"Part_{chunk_count}-{clean_name}.{'txt' if output_format == 'txt' else 'docx'}"
                        part_path = os.path.join(output_dir, part_name)
                        save_chunk(text_buffer, part_path, output_format)
                        generated_files.append(part_path)

                        text_buffer = []
                        chapter_count = 0
                        chunk_count += 1
                        gc.collect()

                except Exception as e:
                    print(f"⚠️ Skipping corrupted EPUB section {file_name}: {e}")
                    continue

        if text_buffer:
            part_name = f"Part_{chunk_count}-{clean_name}.{'txt' if output_format == 'txt' else 'docx'}"
            part_path = os.path.join(output_dir, part_name)
            save_chunk(text_buffer, part_path, output_format)
            generated_files.append(part_path)

        gc.collect()
        return generated_files
    except Exception as e:
        print(f"❌ Total EPUB Zip failure: {e}")
        return []

# ==========================================
# BACKGROUND WORKER (Direct Delivery)
# ==========================================
async def queue_worker():
    while True:
        job = await document_queue.get()
        context, status_msg = job['context'], job['status_msg']
        input_path, output_dir = job['input_path'], job['output_dir']
        base_name, file_name = job['base_name'], job['file_name']

        try:
            loop = asyncio.get_running_loop()
            format_name = "TXT" if job['format'] == "txt" else "DOCX"

            if job['type'] == 'docx':
                await status_msg.edit_text(f"⚡ Processing Fast DOCX: `{file_name}` into chunks of {job['chunk_size']} as {format_name}...")
                files = await loop.run_in_executor(None, split_docx_logic, input_path, output_dir, job['chunk_size'], job['format'])

            elif job['type'] == 'txt':
                await status_msg.edit_text(f"⚡ Processing Fast TXT: `{file_name}` into chunks of {job['chunk_size']} as {format_name}...")
                files = await loop.run_in_executor(None, split_txt_logic, input_path, output_dir, job['chunk_size'], job['format'])

            elif job['type'] == 'epub':
                await status_msg.edit_text(f"⚡ High-Speed EPUB Extraction: `{file_name}` into chunks of {job['chunk_size']} as {format_name}...")
                files = await loop.run_in_executor(None, split_epub_logic, input_path, output_dir, job['chunk_size'], job['format'])

            if not files:
                await status_msg.edit_text("⚠️ No readable chapters found or file is corrupted.")
                continue

            await status_msg.edit_text(f"✅ Processing complete! \n📤 Sending {len(files)} files directly to you...")

            # Send files ONLY to the user
            for f in files:
                with open(f, 'rb') as doc:
                    try:
                        await status_msg.reply_document(document=doc, filename=os.path.basename(f))
                        await asyncio.sleep(0.5) # Tiny delay to prevent Telegram FloodWait
                    except Exception as e:
                        print(f"⚠️ Error sending {os.path.basename(f)}: {e}")

            await status_msg.reply_text("🎉 Done! All files processed and sent successfully.")

        except Exception as e:
            await status_msg.edit_text(f"❌ Error: {e}")
        finally:
            if os.path.exists(job['temp_dir']): shutil.rmtree(job['temp_dir'])
            document_queue.task_done()
            gc.collect()

# ==========================================
# BOT HANDLERS
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name
    await update.message.reply_text(
        f"👋 Hello {name}!\n\nThis is the Ultra-Fast Direct Splitter Bot.\n\n"
        f"Send me a **.docx**, **.txt**, or **.epub** file.\n"
        f"`/set 1000` - Change custom chunk size."
    )

async def set_chunk_size(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_size = int(context.args[0])
        user_chunk_sizes[update.effective_user.id] = new_size
        await update.message.reply_text(f"✅ Custom split size set to **{new_size}** chapters.")
    except: await update.message.reply_text("⚠️ Example: `/set 1000`")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    file_name = doc.file_name.lower()
    msg_id = update.message.message_id

    if not (file_name.endswith('.docx') or file_name.endswith('.epub') or file_name.endswith('.txt')):
        await update.message.reply_text("❌ Only .docx, .txt, or .epub files allowed.")
        return

    pending_uploads[msg_id] = {
        'document': doc,
        'user_mention': f"@{update.effective_user.username}" if update.effective_user.username else update.effective_user.first_name,
        'user_id': update.effective_user.id
    }

    if file_name.endswith('.docx'):
        keyboard = [[InlineKeyboardButton("📄 DOCX", callback_data=f"docx|docx|{msg_id}"), InlineKeyboardButton("📝 TXT", callback_data=f"docx|txt|{msg_id}")]]
        await update.message.reply_text("DOCX detected. Save chunks as:", reply_markup=InlineKeyboardMarkup(keyboard))
    elif file_name.endswith('.txt'):
        keyboard = [[InlineKeyboardButton("📄 DOCX", callback_data=f"txt|docx|{msg_id}"), InlineKeyboardButton("📝 TXT", callback_data=f"txt|txt|{msg_id}")]]
        await update.message.reply_text("TXT detected. Save chunks as:", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        keyboard = [
            [InlineKeyboardButton(f"📄 DOCX ({DEFAULT_EPUB_CHUNK})", callback_data=f"epub|docx_def|{msg_id}"),
             InlineKeyboardButton(f"📝 TXT ({DEFAULT_EPUB_CHUNK})", callback_data=f"epub|txt_def|{msg_id}")],
            [InlineKeyboardButton("⚙️ DOCX (Custom /set)", callback_data=f"epub|docx_cust|{msg_id}"),
             InlineKeyboardButton("⚙️ TXT (Custom /set)", callback_data=f"epub|txt_cust|{msg_id}")]
        ]
        await update.message.reply_text("EPUB detected. Choose format and chunk size:", reply_markup=InlineKeyboardMarkup(keyboard))

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("|")
    job_type, action, msg_id = data[0], data[1], int(data[2])

    if msg_id not in pending_uploads:
        await query.edit_message_text("❌ Session expired. Please upload again.")
        return

    job_info = pending_uploads.pop(msg_id)
    document = job_info['document']
    user_id = job_info['user_id']

    file_name = document.file_name

    # RENDER FIX: Use /tmp directory for temporary files
    temp_dir = f"/tmp/bot_{user_id}_{msg_id}"
    input_path = os.path.join(temp_dir, file_name)
    os.makedirs(os.path.join(temp_dir, "output"), exist_ok=True)

    await query.edit_message_text(f"📥 Downloading `{file_name}`...")
    await (await document.get_file()).download_to_drive(input_path)

    job_data = {
        'type': job_type,
        'update': update, 'context': context, 'status_msg': query.message,
        'temp_dir': temp_dir, 'input_path': input_path, 'output_dir': os.path.join(temp_dir, "output"),
        'file_name': file_name, 'base_name': os.path.splitext(file_name)[0][:64].strip(),
        'user_mention': job_info['user_mention']
    }

    if job_type in ['docx', 'txt']:
        job_data['format'] = action
        job_data['chunk_size'] = user_chunk_sizes.get(user_id, DEFAULT_DOCX_CHUNK)
    else:
        format_choice = action.split('_')[0]
        size_type = action.split('_')[1]

        job_data['format'] = format_choice
        job_data['chunk_size'] = user_chunk_sizes.get(user_id, DEFAULT_EPUB_CHUNK) if size_type == "cust" else DEFAULT_EPUB_CHUNK

    await document_queue.put(job_data)

# ==========================================
# FAKE WEB SERVER (Keep-Alive for Render)
# ==========================================
app_web = Flask(__name__)

@app_web.route('/')
def health_check():
    return "Bot is alive and running!", 200

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app_web.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# MAIN RUNNER (Render Safe)
# ==========================================
async def start_background_tasks(app: Application):
    global document_queue
    document_queue = asyncio.Queue()
    asyncio.create_task(queue_worker())

def main():
    print("🤖 Ultra-Fast Direct Bot Initializing on Render...")
    app = Application.builder().token(TOKEN).post_init(start_background_tasks).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("set", set_chunk_size))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(button_callback))

    print("🚀 Master Bot is LIVE! (XML Injection Active)")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    # 1. Start the Flask Keep-Alive Server
    threading.Thread(target=run_web, daemon=True).start()
    
    # 2. Start the Bot (Standard Asyncio)
    main()
