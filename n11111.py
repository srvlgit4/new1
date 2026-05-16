# ============================================
# INSTALL DEPENDENCIES (run once):
!pip install pyrogram tgcrypto edge-tts nest-asyncio flask --quiet
!apt-get update && apt-get install -y ffmpeg --quiet
# ============================================

import os
import time
import asyncio
import edge_tts
import nest_asyncio
import re
import random
import tempfile
import shutil
import logging
from threading import Thread
from flask import Flask
from pyrogram import Client, filters, idle
import sys

nest_asyncio.apply()

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
API_ID         = 35846534
API_HASH       = "129ba550b42323af0657fe6383ccd7f9"
SESSION_STRING = "BQIi-YYAc9bk0DnBUgVIXraq8LFLAt3x0WGLLYLmSj6w42GvQA4WFwnu5FpyxbTgneF25F9hUoOdXfedFrsv-C0Wztg3Wqkbayg-Zlxsl_MhmYxGDaX1kPwwC8MsGPm6k9xav0L5iZ__GN0PhZURQ0vWGGEpIjveLRLwcdEL83EwOcrrtOgGSUo4nqZFZRQ8shHmVFb716Q4ArIKmBxSMkRuLc53Tq4vCMw_WVa1j2y93sKLDIOKMqpbVkkZ5FUNd7EQf6mFdQoTZ9yFB4sCW_eNiCnWYSI9-gXT6Q_JH94GRmiVGFINAjNdWgzrrMUla6PiAoBIHTgxDifRcE8pu7kZPfgITAAAAAG6jo_iAA"
     # <-- Paste your session string here

VOICES  = ["hi-IN-MadhurNeural"]
RATE    = "+80%"       # Don't speed up — edge-tts quality drops at +98%
VOLUME  = "+10%"

MAX_CONCURRENT  = 100      # Keep low — edge-tts rate-limits aggressively
CHUNK_SIZE      = 3000    # Safe for edge-tts (hard limit is ~4990 chars)
EPISODE_SIZE    = 700000  # ~600 KB text per episode

MIN_DELAY = 1.5           # Seconds between successful TTS requests
MAX_DELAY = 3.5
PROGRESS_INTERVAL = 5     # Seconds between Telegram message edits

MAX_RETRIES      = 12
RETRY_BASE_DELAY = 8      # Grows with backoff, capped per error type

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ==========================================
# 🧹 TEXT CLEANER
# ==========================================
# Preserved Unicode ranges for Hindi:
#   \u0900-\u097F  Full Devanagari block (all matras, chandrabindu, visarga, nukta, digits)
#   \u200C         ZWNJ — required for half-letters (e.g. क् + ष = क्ष)
#   \u200D         ZWJ  — required for conjuncts
#   \u0964-\u0965  Danda (।) and double danda (॥)

_KEEP_RE = re.compile(
    r'[^\u0900-\u097F\u200C\u200D\w\s।॥\.\,\!\?\-\:\;\(\)]'
)

def clean_text(text: str) -> str:
    """
    Clean Hindi text for edge-tts (which uses SSML under the hood).
    Rules:
      1. Normalise line endings.
      2. Fix XML/SSML unsafe chars FIRST (& < > " ') — these crash edge-tts.
      3. Remove invisible/zero-width garbage (NOT ZWJ/ZWNJ — those are needed).
      4. Remove ASCII control characters.
      5. Replace anything not Hindi, alphanumeric, whitespace, or safe punctuation with space.
      6. Collapse whitespace.
    """
    if not text:
        return ""

    # 1. Normalise line endings; collapse 3+ blank lines → 2
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r'\n{3,}', '\n\n', text)

    # 2. SSML safety — must happen BEFORE any stripping
    text = (text
        .replace("&",  " और ")
        .replace("<",  " ")
        .replace(">",  " ")
        .replace('"',  " ")
        .replace("'",  " ")
    )

    # 3. Remove invisible garbage (keep ZWJ \u200D and ZWNJ \u200C)
    text = re.sub(r'[\u200B\u200E\u200F\uFEFF\u00AD]', '', text)

    # 4. Remove ASCII control chars (keep \n)
    text = re.sub(r'[\x00-\x09\x0b-\x1f\x7f]', '', text)

    # 5. Replace unusual symbols — keep Devanagari + safe ASCII punctuation
    text = _KEEP_RE.sub(' ', text)

    # 6. Replace underscores with space
    text = text.replace('_', ' ')

    # 7. Collapse horizontal whitespace; remove leading spaces after newline
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)

    return text.strip()


# ==========================================
# ✂️ SMART CHUNK SPLITTER
# ==========================================
# Priority: paragraph → sentence (। ? ! . \n) → clause (, ; :) → word
# NEVER slice inside a word — this breaks Devanagari grapheme clusters.
# edge-tts hard limit ≈ 4990 chars; we stay at CHUNK_SIZE (3500) to be safe.

_SENT_RE   = re.compile(r'(?<=[।॥?!\n])\s*|\.\s+')
_CLAUSE_RE = re.compile(r'(?<=[,;:])\s+')


def _flush(buf: str, chunks: list) -> str:
    if buf.strip():
        chunks.append(buf.strip())
    return ""


def smart_split(text: str, max_chars: int) -> list:
    chunks:  list = []
    current: str  = ""

    paragraphs = re.split(r'\n\n+', text)

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Whole paragraph fits
        if len(current) + len(para) + 2 <= max_chars:
            current += para + "\n\n"
            continue

        current = _flush(current, chunks)

        # Paragraph fits alone
        if len(para) <= max_chars:
            current = para + "\n\n"
            continue

        # Para too long → split by sentences
        for sent in _SENT_RE.split(para):
            sent = sent.strip()
            if not sent:
                continue

            if len(current) + len(sent) + 1 <= max_chars:
                current += sent + " "
                continue

            current = _flush(current, chunks)

            if len(sent) <= max_chars:
                current = sent + " "
                continue

            # Sentence too long → split by clauses
            for clause in _CLAUSE_RE.split(sent):
                clause = clause.strip()
                if not clause:
                    continue

                if len(current) + len(clause) + 1 <= max_chars:
                    current += clause + " "
                    continue

                current = _flush(current, chunks)

                if len(clause) <= max_chars:
                    current = clause + " "
                    continue

                # Clause too long → split by words (NEVER slice a word)
                for word in clause.split():
                    if len(word) > max_chars:
                        # Single token longer than limit (URL / garbage) — keep whole
                        current = _flush(current, chunks)
                        chunks.append(word)
                        continue
                    if len(current) + len(word) + 1 <= max_chars:
                        current += word + " "
                    else:
                        current = _flush(current, chunks)
                        current = word + " "

    _flush(current, chunks)
    return [c for c in chunks if c.strip()]


# ==========================================
# 📊 PROGRESS TRACKER
# ==========================================
class ProgressTracker:
    def __init__(self, status_msg):
        self.status_msg  = status_msg
        self.last_update = 0.0
        self.lock        = asyncio.Lock()

    async def update(self, text: str, force: bool = False):
        now = time.time()
        async with self.lock:
            if force or (now - self.last_update) >= PROGRESS_INTERVAL:
                try:
                    await self.status_msg.edit_text(text)
                    self.last_update = now
                except Exception:
                    pass


# ==========================================
# 🎙️ TTS CORE
# ==========================================
async def tts_chunk(text: str, filename: str, voice: str, timeout: int = 60) -> tuple:
    """
    Try to generate TTS audio for one chunk.
    Returns (success, rate_limited, empty_response).
    """
    if os.path.exists(filename):
        try:
            os.remove(filename)
        except OSError:
            pass

    try:
        comm = edge_tts.Communicate(text=text, voice=voice, rate=RATE, volume=VOLUME)
        await asyncio.wait_for(comm.save(filename), timeout=timeout)

        if os.path.exists(filename) and os.path.getsize(filename) >= 512:
            return True, False, False       # ✅ success

        # File too small → empty response
        if os.path.exists(filename):
            os.remove(filename)
        return False, False, True           # ⚠️ empty

    except asyncio.TimeoutError:
        if os.path.exists(filename):
            os.remove(filename)
        return False, False, True           # timeout → retry as empty

    except Exception as exc:
        if os.path.exists(filename):
            os.remove(filename)
        err = str(exc).lower()
        rate_limited = any(k in err for k in ("429", "too many", "closed", "reset", "throttl", "refused"))
        return False, rate_limited, False   # ❌ error


# ==========================================
# 🔄 EPISODE PROCESSOR
# ==========================================
async def process_episode(
    chunk_data: list,       # list of (text, mp3_path, index)
    progress: ProgressTracker,
    ep_num: int,
    total_ep: int,
) -> list:
    """
    Translate all chunks concurrently (bounded by semaphore).
    Returns ordered list of successful mp3 paths.
    Results dict keyed by index guarantees correct ordering regardless
    of which tasks finish first.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results   = {}          # {index: mp3_path | None}
    lock      = asyncio.Lock()
    done      = 0
    total     = len(chunk_data)

    async def worker(text: str, mp3_path: str, idx: int):
        nonlocal done

        if not text.strip():
            async with lock:
                results[idx] = None
                done += 1
            return

        voice = VOICES[idx % len(VOICES)]

        async with semaphore:
            for attempt in range(1, MAX_RETRIES + 1):
                # Build progress bar
                pct       = int(done / total * 100)
                filled    = pct // 5
                bar       = "█" * filled + "░" * (20 - filled)
                await progress.update(
                    f"🎙️ Episode {ep_num}/{total_ep}\n"
                    f"`{bar}` {pct}%\n"
                    f"✅ {done}/{total} chunks  |  🔄 Attempt {attempt}/{MAX_RETRIES}\n"
                    f"📦 Chunk {idx}/{total}"
                )

                success, rate_limited, empty = await tts_chunk(text, mp3_path, voice)

                if success:
                    async with lock:
                        results[idx] = mp3_path
                        done += 1
                    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                    return

                # Back-off strategy by error type
                if rate_limited:
                    backoff = min(90.0, RETRY_BASE_DELAY * (2.0 ** (attempt - 1)))
                elif empty:
                    backoff = min(45.0, RETRY_BASE_DELAY * (1.5 ** (attempt - 1)))
                else:
                    backoff = min(25.0, RETRY_BASE_DELAY * (1.2 ** (attempt - 1)))

                backoff += random.uniform(0.5, 2.0)   # jitter
                logger.warning(f"Chunk {idx}: attempt {attempt} failed (rate={rate_limited} empty={empty}), sleeping {backoff:.1f}s")
                await asyncio.sleep(backoff)

            # All retries exhausted
            logger.error(f"Chunk {idx}: FAILED after {MAX_RETRIES} attempts — skipped.")
            async with lock:
                results[idx] = None
                done += 1

    tasks = [asyncio.create_task(worker(t, p, i)) for t, p, i in chunk_data]
    await asyncio.gather(*tasks)

    # Reconstruct in original index order, skipping failures
    ordered = []
    for _, mp3_path, idx in chunk_data:
        res = results.get(idx)
        if res and os.path.exists(res):
            ordered.append(res)
        else:
            logger.warning(f"Chunk {idx} missing from final merge — skipped.")

    return ordered


# ==========================================
# 🌐 COLAB KEEP-ALIVE SERVER
# ==========================================
_flask_app = Flask(__name__)

@_flask_app.route('/')
def _home():
    return "TTS UserBot is running!", 200

def run_keep_alive():
    try:
        from google.colab.output import eval_js
        url = eval_js('google.colab.kernel.proxyPort(8080)')
        print(f"🌐 Keep-alive URL: {url}")
    except Exception:
        print("ℹ️  Not in Colab — keep-alive skipped.")
        return
    import logging as _lg
    _lg.getLogger('werkzeug').setLevel(_lg.ERROR)
    _flask_app.run(host='0.0.0.0', port=8080)


# ==========================================
# ⬆️ UPLOAD PROGRESS
# ==========================================
class UploadProgress:
    def __init__(self):
        self.last_update = 0.0

    async def __call__(self, current: int, total: int, status_msg, prefix: str):
        now = time.time()
        if now - self.last_update >= PROGRESS_INTERVAL:
            try:
                pct      = round(current * 100 / total, 1)
                mb_cur   = round(current / 1048576, 2)
                mb_total = round(total   / 1048576, 2)
                filled   = int(pct / 5)
                bar      = "█" * filled + "░" * (20 - filled)
                await status_msg.edit_text(
                    f"{prefix}\n`{bar}` {pct}%\n⬆️ {mb_cur} MB / {mb_total} MB"
                )
                self.last_update = now
            except Exception:
                pass


# ==========================================
# 🤖 PYROGRAM USERBOT
# ==========================================
tg = Client(
    "tts_userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    in_memory=True,
)


@tg.on_message(filters.command("start") & filters.me & filters.chat("me"))
async def start_cmd(client, message):
    await message.reply_text(
        "👋 TTS UserBot is active!\n"
        "Send any `.txt` Hindi file to Saved Messages to convert it to audio."
    )


@tg.on_message(filters.document & filters.me & filters.chat("me"))
async def handle_document(client, message):
    doc = message.document
    if not doc or not doc.file_name.lower().endswith('.txt'):
        return

    status_msg = await message.reply_text("📥 Downloading file…")
    progress   = ProgressTracker(status_msg)
    temp_dir   = tempfile.mkdtemp()

    try:
        # ── Download ──────────────────────────────────────────────────────
        file_path = await message.download()
        try:
            with open(file_path, 'r', encoding='utf-8') as fh:
                raw_text = fh.read()
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='latin-1') as fh:
                raw_text = fh.read()
        finally:
            os.remove(file_path)

        # ── Clean ─────────────────────────────────────────────────────────
        await progress.update("🧹 Cleaning text…", force=True)
        cleaned = clean_text(raw_text)

        if not cleaned.strip():
            await status_msg.edit_text("❌ File is empty after cleaning.")
            return

        # ── Split into episodes ────────────────────────────────────────────
        episodes  = smart_split(cleaned, EPISODE_SIZE)
        total_ep  = len(episodes)
        await progress.update(
            f"📚 {total_ep} episode(s) detected.\n⏳ Starting TTS…", force=True
        )

        for ep_idx, ep_text in enumerate(episodes):
            ep_num = ep_idx + 1

            # Split episode into TTS chunks
            chunks = smart_split(ep_text, CHUNK_SIZE)

            # Sanity-check: edge-tts hard limit is ~4990 chars
            safe_chunks = []
            for c in chunks:
                if len(c) <= 4900:
                    safe_chunks.append(c)
                else:
                    # Force-split oversized chunk by words (last resort)
                    sub = smart_split(c, 4900)
                    safe_chunks.extend(sub)

            chunk_data = [
                (txt, os.path.join(temp_dir, f"e{ep_num}_c{i+1:04d}.mp3"), i + 1)
                for i, txt in enumerate(safe_chunks)
            ]

            # ── Generate audio ────────────────────────────────────────────
            successful_files = await process_episode(chunk_data, progress, ep_num, total_ep)

            failed = len([c for c in safe_chunks if c.strip()]) - len(successful_files)
            if failed > 0:
                await message.reply_text(
                    f"⚠️ Episode {ep_num}: {failed} chunk(s) failed and were skipped."
                )

            if not successful_files:
                await message.reply_text(
                    f"❌ Episode {ep_num}: No audio generated — skipping."
                )
                continue

            # ── Merge with ffmpeg (stream copy — no re-encoding) ──────────
            list_file = os.path.join(temp_dir, f"list_{ep_num}.txt")
            final_mp3 = os.path.join(temp_dir, f"Episode_{ep_num}.mp3")

            with open(list_file, "w", encoding='utf-8') as lf:
                for f in successful_files:
                    # Escape backslashes and single quotes for ffmpeg concat
                    safe_path = f.replace("\\", "/").replace("'", r"'\''")
                    lf.write(f"file '{safe_path}'\n")

            await progress.update(f"🔗 Merging Episode {ep_num}/{total_ep}…", force=True)

            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                "-f", "concat", "-safe", "0",
                "-i", list_file,
                "-c", "copy",           # Direct stream copy — fastest, no quality loss
                final_mp3,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, ffmpeg_err = await proc.communicate()

            if not os.path.exists(final_mp3) or os.path.getsize(final_mp3) < 1024:
                err_snippet = ffmpeg_err.decode(errors='ignore')[-400:] if ffmpeg_err else "unknown"
                await message.reply_text(
                    f"❌ ffmpeg failed for Episode {ep_num}:\n<code>{err_snippet}</code>",
                    parse_mode="html",
                )
                continue

            # ── Upload ────────────────────────────────────────────────────
            await progress.update(f"⬆️ Uploading Episode {ep_num}/{total_ep}…", force=True)

            upload_cb = UploadProgress()
            await message.reply_audio(
                audio=final_mp3,
                title=f"Episode {ep_num} of {total_ep}",
                performer=doc.file_name,
                progress=upload_cb,
                progress_args=(status_msg, f"🎙️ Episode {ep_num}/{total_ep}"),
            )

            # Free chunk files immediately to save disk
            for _, mp3_path, _ in chunk_data:
                try:
                    os.remove(mp3_path)
                except OSError:
                    pass

        await status_msg.edit_text(f"✅ Done! All {total_ep} episode(s) sent.")

    except Exception as exc:
        logger.exception("handle_document: unhandled error")
        try:
            await status_msg.edit_text(f"❌ Error:\n{exc}")
        except Exception:
            pass

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# ==========================================
# 🏁 ENTRY POINT
# ==========================================
async def main():
    Thread(target=run_keep_alive, daemon=True).start()
    print("🤖 TTS UserBot starting…")
    await tg.start()
    me = await tg.get_me()
    print(f"✅ Logged in as @{me.username} ({me.first_name})")
    print("📁 Send a .txt file to your Saved Messages to begin.")
    await idle()
    await tg.stop()


if __name__ == "__main__":
    if not SESSION_STRING:
        print("❌ SESSION_STRING is empty. Generate one and paste it in the config.")
        sys.exit(1)

    try:
        if "ipykernel" in sys.modules:     # Jupyter / Colab
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main())
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")
