import os
import re
import shutil
import asyncio
import zipfile
import html
import gc
import threading
from flask import Flask
from docx import Document
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.error import RetryAfter

# ==========================================
# CONFIGURATION
# ==========================================
TOKEN = "7954112414:AAEXxK5PcckCiZChIWUXHsQSRD2XfiasW-Q"
GROUP_ID = -1003745983576
DEFAULT_DOCX_CHUNK = 50
DEFAULT_EPUB_CHUNK = 500

# Global State
document_queue = asyncio.Queue()
user_chunk_sizes = {}
pending_uploads = {}

# ==========================================
# FAKE WEB SERVER (For Render Health Check)
# ==========================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "Bot is active!", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host='0.0.0.0', port=port)

# ==========================================
# CORE LOGIC: SPLITTING & EXTRACTION
# ==========================================
def split_text_based_logic(input_path, output_dir, chunk_size, output_format, is_txt_file=False):
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    generated_files = []

    # Advanced Regex: Finds chapter numbers anywhere in the title line
    pattern_num = re.compile(
        r"(?:vol(?:ume)?\s*\d+\s*)?"
        r"(?:chapter|ch|c|अध्याय|चैप्टर|सी|page|पृष्ठ|#)?\s*"
        r"(\d+)"
        r"(?:[:\s,.-]|$)",
        re.IGNORECASE
    )

    def save_chunk(lines, start, end, chunk_num):
        ext = ".txt" if output_format == "txt" else ".docx"
        suffix = f"Chapters_{start}_to_{end}"
        part_name = f"{suffix}_{base_name}_Part{chunk_num}{ext}"
        part_path = os.path.join(output_dir, part_name)

        if output_format == "txt":
            with open(part_path, "w", encoding="utf-8", errors='ignore') as f:
                f.write("\n\n".join(lines))
        else:
            new_doc = Document()
            for line in lines:
                if line.strip(): new_doc.add_paragraph(line.strip())
            new_doc.save(part_path)
        generated_files.append(part_path)

    # Load content
    lines = []
    if is_txt_file:
        with open(input_path, "r", encoding="utf-8", errors='ignore') as f:
            lines = [line.strip() for line in f if line.strip()]
    else:
        doc = Document(input_path)
        lines = [re.sub(r'\x00', '', p.text).strip() for p in doc.paragraphs if p.text.strip()]
        del doc

    current_chunk = []
    current_start_ch = None
    last_detected_ch = None
    chunk_count = 1

    for line in lines:
        match = pattern_num.search(line)
        detected = int(match.group(1)) if match else None

        if detected is not None:
            if current_start_ch is None: current_start_ch = detected
            
            if detected >= (current_start_ch + chunk_size):
                save_chunk(current_chunk, current_start_ch, last_detected_ch or (detected-1), chunk_count)
                current_chunk = []
                current_start_ch = detected
                chunk_count += 1
            last_detected_ch = detected

        current_chunk.append(line)

    if current_chunk:
        save_chunk(current_chunk, current_start_ch or 1, last_detected_ch or "End", chunk_count)

    gc.collect()
    return generated_files

def split_epub_logic(input_path, output_dir, chunk_size, output_format):
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    base_name = re.sub(r'[^\w\-_]', '_', os.path.splitext(os.path.basename(input_path))[0])
    generated_files = []

    try:
        with zipfile.ZipFile(input_path, 'r') as epub:
            files = sorted([f for f in epub.namelist() if f.lower().endswith(('.html', '.xhtml'))])
            buffer, ch_in_chunk, chunk_idx = [], 0, 1
            for f_name in files:
                content = epub.read(f_name).decode('utf-8', errors='ignore')
                clean_lines = [html.unescape(re.sub(r'<[^>]+>', '', line)).strip() 
                               for line in re.split(r'</?(?:p|div|br|h[1-6])[^>]*>', content) if line.strip()]
                if clean_lines:
                    buffer.extend(clean_lines)
                    buffer.append("-" * 20)
                    ch_in_chunk += 1
                if ch_in_chunk >= chunk_size:
                    ext = ".txt" if output_format == "txt" else ".docx"
                    p_path = os.path.join(output_dir, f"Part_{chunk_idx}-{base_name}{ext}")
                    if output_format == "txt":
                        with open(p_path, "w", encoding="utf-8") as f: f.write("\n\n".join(buffer))
                    else:
                        d = Document(); [d.add_paragraph(l) for l in buffer]; d.save(p_path)
                    generated_files.append(p_path)
                    buffer, ch_in_chunk, chunk_idx = [], 0, chunk_idx + 1
                    gc.collect()
            if buffer:
                # Save final small chunk
                ext = ".txt" if output_format == "txt" else ".docx"
                p_path = os.path.join(output_dir, f"Part_{chunk_idx}-{base_name}{ext}")
                if output_format == "txt":
                    with open(p_path, "w", encoding="utf-8") as f: f.write("\n\n".join(buffer))
                else:
                    d = Document(); [d.add_paragraph(l) for l in buffer]; d.save(p_path)
                generated_files.append(p_path)
    except Exception as e: print(f"EPUB Error: {e}")
    return generated_files

# ==========================================
# ASYNC WORKER (QUEUE PROCESSOR)
# ==========================================
async def queue_worker():
    while True:
        job = await document_queue.get()
        context, status_msg = job['context'], job['status_msg']
        try:
            loop = asyncio.get_event_loop()
            if job['type'] == 'docx':
                files = await loop.run_in_executor(None, split_text_based_logic, job['input_path'], job['output_dir'], job['chunk_size'], job['format'], False)
            elif job['type'] == 'txt':
                files = await loop.run_in_executor(None, split_text_based_logic, job['input_path'], job['output_dir'], job['chunk_size'], job['format'], True)
            else:
                files = await loop.run_in_executor(None, split_epub_logic, job['input_path'], job['output_dir'], job['chunk_size'], job['format'])

            if files:
                topic = await context.bot.create_forum_topic(chat_id=GROUP_ID, name=job['base_name'][:128])
                for f in files:
                    # Dual-sending: Bot Chat + Group
                    with open(f, 'rb') as doc:
                        # 1. Send to Group
                        await context.bot.send_document(chat_id=GROUP_ID, message_thread_id=topic.message_thread_id, document=doc, filename=os.path.basename(f))
                        # 2. Send to User Bot Chat
                        doc.seek(0)
                        await context.bot.send_document(chat_id=job['user_id'], document=doc, filename=os.path.basename(f))
                    await asyncio.sleep(0.6) # Safety for rate limits
                await status_msg.edit_text(f"✅ Success: {job['base_name']}")
            else:
                await status_msg.edit_text("❌ Failed: No chapters detected.")
        except Exception as e:
            await status_msg.edit_text(f"❌ Worker Error: {str(e)}")
        finally:
            if os.path.exists(job['temp_dir']): shutil.rmtree(job['temp_dir'])
            document_queue.task_done()
            gc.collect()

# ==========================================
# TELEGRAM HANDLERS
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"👋 Hello {update.effective_user.first_name}!\nSend a .docx, .txt, or .epub to split.")

async def set_chunk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        size = int(context.args[0])
        user_chunk_sizes[update.effective_user.id] = size
        await update.message.reply_text(f"✅ Split size set to {size}")
    except: await update.message.reply_text("Usage: /set 100")

async def handle_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    msg_id = update.message.message_id
    pending_uploads[msg_id] = {'document': doc, 'user_id': update.effective_user.id, 'user_name': update.effective_user.name}
    
    keyboard = [[InlineKeyboardButton("📄 DOCX", callback_data=f"f|docx|{msg_id}"), 
                 InlineKeyboardButton("📝 TXT", callback_data=f"f|txt|{msg_id}")]]
    await update.message.reply_text("Choose Output Format:", reply_markup=InlineKeyboardMarkup(keyboard))

async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, fmt, msg_id = query.data.split("|")
    msg_id = int(msg_id)

    if msg_id not in pending_uploads:
        await query.edit_message_text("❌ Session expired."); return

    job_info = pending_uploads.pop(msg_id)
    temp_dir = os.path.join("/tmp", f"job_{msg_id}") # FIXED FOR RENDER
    os.makedirs(os.path.join(temp_dir, "output"), exist_ok=True)
    input_p = os.path.join(temp_dir, job_info['document'].file_name)

    status = await query.edit_message_text("📥 Downloading & Processing...")
    f_obj = await context.bot.get_file(job_info['document'].file_id)
    await f_obj.download_to_drive(input_p)

    await document_queue.put({
        'type': 'txt' if input_p.endswith('.txt') else ('epub' if input_p.endswith('.epub') else 'docx'),
        'format': fmt, 'chunk_size': user_chunk_sizes.get(job_info['user_id'], DEFAULT_DOCX_CHUNK),
        'input_path': input_p, 'output_dir': os.path.join(temp_dir, "output"),
        'temp_dir': temp_dir, 'base_name': os.path.basename(input_p),
        'status_msg': status, 'context': context, 'user_id': job_info['user_id']
    })

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    threading.Thread(target=run_flask, daemon=True).start()
    
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("set", set_chunk))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_doc))
    app.add_handler(CallbackQueryHandler(callback))
    
    asyncio.get_event_loop().create_task(queue_worker())
    
    print("🚀 Master Bot is ONLINE")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
