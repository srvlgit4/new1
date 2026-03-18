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
# It is better to set this in Render Environment Variables as TELEGRAM_TOKEN
TOKEN = "7954112414:AAEXxK5PcckCiZChIWUXHsQSRD2XfiasW-Q"
GROUP_ID = -1003745983576
DEFAULT_DOCX_CHUNK = 50
DEFAULT_EPUB_CHUNK = 500

# Global State
document_queue = None
user_chunk_sizes = {}
pending_uploads = {}

# ==========================================
# FAKE WEB SERVER (Required for Render)
# ==========================================
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "Bot is alive!", 200

def run_flask():
    # Render provides the PORT environment variable
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host='0.0.0.0', port=port)

# ==========================================
# LOGIC 1, 2 & 3 (Your original splitting logic)
# ==========================================

# ... [Keep your split_text_based_logic, split_docx_logic, 
#      split_txt_logic, natural_sort_key, fast_html_to_text, 
#      and split_epub_logic functions here exactly as they were] ...

# Note: Ensure you include the imports and the functions from your previous snippet here.

# ==========================================
# BACKGROUND WORKER
# ==========================================
async def queue_worker():
    global document_queue
    while True:
        job = await document_queue.get()
        context, status_msg = job['context'], job['status_msg']
        input_path, output_dir = job['input_path'], job['output_dir']
        base_name, file_name = job['base_name'], job['file_name']

        try:
            loop = asyncio.get_event_loop()
            format_name = "TXT" if job['format'] == "txt" else "DOCX"

            if job['type'] == 'docx':
                await status_msg.edit_text(f"⚡ Processing DOCX: `{file_name}`...")
                files = await loop.run_in_executor(None, split_docx_logic, input_path, output_dir, job['chunk_size'], job['format'])
            elif job['type'] == 'txt':
                await status_msg.edit_text(f"⚡ Processing TXT: `{file_name}`...")
                files = await loop.run_in_executor(None, split_txt_logic, input_path, output_dir, job['chunk_size'], job['format'])
            elif job['type'] == 'epub':
                await status_msg.edit_text(f"⚡ Processing EPUB: `{file_name}`...")
                files = await loop.run_in_executor(None, split_epub_logic, input_path, output_dir, job['chunk_size'], job['format'])

            if not files:
                await status_msg.edit_text("⚠️ No chapters found or file corrupted.")
                continue

            # Topic Creation and Forwarding logic
            topic = await context.bot.create_forum_topic(chat_id=GROUP_ID, name=base_name[:128])
            thread_id = topic.message_thread_id
            
            for f in files:
                with open(f, 'rb') as doc:
                    await context.bot.send_document(
                        chat_id=GROUP_ID, 
                        message_thread_id=thread_id, 
                        document=doc, 
                        filename=os.path.basename(f)
                    )
                await asyncio.sleep(0.5) # Avoid flood limits

            await status_msg.edit_text(f"✅ Done! Files sent to group topic: {base_name}")

        except Exception as e:
            print(f"Worker Error: {e}")
            await status_msg.edit_text(f"❌ Error: {str(e)}")
        finally:
            if os.path.exists(job['temp_dir']):
                shutil.rmtree(job['temp_dir'])
            document_queue.task_done()
            gc.collect()

# ==========================================
# BOT HANDLERS & MAIN
# ==========================================

# ... [Keep your start, set_chunk_size, handle_document, and button_callback here] ...

async def post_init(application: Application):
    global document_queue
    document_queue = asyncio.Queue()
    asyncio.create_task(queue_worker())

def main():
    # Start Flask in a separate thread
    threading.Thread(target=run_flask, daemon=True).start()

    print("🤖 Bot Initializing...")
    # Add post_init to start the background worker
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("set", set_chunk_size))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(button_callback))
    
    print("🚀 Bot is live on Render!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
