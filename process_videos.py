# process_videos.py
import os
import subprocess
import json
import time
import google.generativeai as genai
import whisper
from tqdm import tqdm
import logging

# --- CẤU HÌNH --- 
# Đường dẫn đến các thư mục (SỬA LẠI CHO PHÙ HỢP VỚI MÁY BẠN NẾU CẦN)
BASE_DIR = r"C:\Users\DELL\Project_Tiktok_knowledge\data\Favorites"
VIDEO_DIR = os.path.join(BASE_DIR, "videos")
AUDIO_DIR = os.path.join(BASE_DIR, "audio_files")
TRANSCRIPT_DIR = os.path.join(BASE_DIR, "transcripts")
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis_results")
LOG_FILE = os.path.join(BASE_DIR, "processing.log")

# API Key của Google Gemini (THAY BẰNG KEY CỦA BẠN)
GEMINI_API_KEY = "AIzaSyDyLqLIygimttHVU_EVbcOswMCiVn6w0Gk"

# Cấu hình Whisper
WHISPER_MODEL_SIZE = "base"  # Chọn model: tiny, base, small, medium, large
WHISPER_LANGUAGE = "vi"      # Ngôn ngữ video (ví dụ: "vi" cho tiếng Việt, để None nếu muốn tự động phát hiện)

# Cấu hình Gemini
GEMINI_MODEL_NAME = "gemini-1.5-flash" # Hoặc gemini-pro
GEMINI_MAX_OUTPUT_TOKENS = 4096
GEMINI_TEMPERATURE = 0.2 # Giảm để kết quả nhất quán hơn

# --- KẾT THÚC CẤU HÌNH ---

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler() # Hiển thị log ra console
    ]
)

# Khởi tạo Gemini
try:
    genai.configure(api_key=GEMINI_API_KEY)
    gemini_model = genai.GenerativeModel(
        model_name=GEMINI_MODEL_NAME,
        generation_config={
            "temperature": GEMINI_TEMPERATURE,
            "max_output_tokens": GEMINI_MAX_OUTPUT_TOKENS
        }
    )
    logging.info("Đã khởi tạo Google Gemini thành công.")
except Exception as e:
    logging.error(f"Lỗi khởi tạo Google Gemini: {e}")
    gemini_model = None

# Tải mô hình Whisper (chỉ tải một lần)
try:
    whisper_model = whisper.load_model(WHISPER_MODEL_SIZE)
    logging.info(f"Đã tải mô hình Whisper '{WHISPER_MODEL_SIZE}' thành công.")
except Exception as e:
    logging.error(f"Lỗi tải mô hình Whisper: {e}")
    whisper_model = None

def get_file_paths(video_filename):
    """Tạo đường dẫn cho các file audio, transcript, analysis từ tên file video."""
    base_name = os.path.splitext(video_filename)[0]
    audio_path = os.path.join(AUDIO_DIR, f"{base_name}.mp3")
    transcript_path = os.path.join(TRANSCRIPT_DIR, f"{base_name}.txt")
    analysis_path = os.path.join(ANALYSIS_DIR, f"{base_name}.json")
    return audio_path, transcript_path, analysis_path

def extract_audio(video_path, audio_path):
    """Trích xuất audio từ video bằng ffmpeg."""
    if os.path.exists(audio_path):
        logging.info(f"File audio đã tồn tại: {os.path.basename(audio_path)}")
        return True
    try:
        logging.info(f"Đang trích xuất audio từ: {os.path.basename(video_path)}")
        # Lệnh ffmpeg: -i input -vn (không video) -acodec libmp3lame (codec mp3) -q:a 2 (chất lượng) output
        command = [
            'ffmpeg',
            '-i', video_path,
            '-vn',
            '-acodec', 'libmp3lame',
            '-q:a', '2',
            audio_path
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"Đã trích xuất audio thành công: {os.path.basename(audio_path)}")
        return True
    except FileNotFoundError:
        logging.error("Lỗi: Lệnh 'ffmpeg' không tìm thấy. Hãy đảm bảo ffmpeg đã được cài đặt và thêm vào PATH.")
        return False
    except subprocess.CalledProcessError as e:
        logging.error(f"Lỗi ffmpeg khi xử lý {os.path.basename(video_path)}: {e.stderr}")
        # Xóa file audio có thể bị lỗi
        if os.path.exists(audio_path):
            os.remove(audio_path)
        return False
    except Exception as e:
        logging.error(f"Lỗi không xác định khi trích xuất audio {os.path.basename(video_path)}: {e}")
        return False

def transcribe_audio(audio_path, transcript_path):
    """Chuyển đổi audio thành văn bản bằng Whisper."""
    if os.path.exists(transcript_path):
        logging.info(f"File transcript đã tồn tại: {os.path.basename(transcript_path)}")
        return True
    if not whisper_model:
        logging.error("Mô hình Whisper chưa được tải. Bỏ qua phiên âm.")
        return False
    try:
        logging.info(f"Đang phiên âm: {os.path.basename(audio_path)}")
        options = {}
        if WHISPER_LANGUAGE:
            options["language"] = WHISPER_LANGUAGE
        
        result = whisper_model.transcribe(audio_path, **options)
        transcript_text = result["text"]
        
        with open(transcript_path, 'w', encoding='utf-8') as f:
            f.write(transcript_text)
        logging.info(f"Đã phiên âm thành công: {os.path.basename(transcript_path)}")
        return True
    except Exception as e:
        logging.error(f"Lỗi Whisper khi xử lý {os.path.basename(audio_path)}: {e}")
        # Xóa file transcript có thể bị lỗi
        if os.path.exists(transcript_path):
            os.remove(transcript_path)
        return False

def analyze_transcript(transcript_path, analysis_path):
    """Phân tích nội dung transcript bằng Gemini API."""
    if os.path.exists(analysis_path):
        logging.info(f"File analysis đã tồn tại: {os.path.basename(analysis_path)}")
        return True
    if not gemini_model:
        logging.error("Mô hình Gemini chưa được khởi tạo. Bỏ qua phân tích.")
        return False
    try:
        logging.info(f"Đang phân tích: {os.path.basename(transcript_path)}")
        with open(transcript_path, 'r', encoding='utf-8') as f:
            transcript_text = f.read()

        if not transcript_text.strip():
            logging.warning(f"File transcript rỗng: {os.path.basename(transcript_path)}. Bỏ qua phân tích.")
            # Tạo file JSON rỗng để đánh dấu đã xử lý
            with open(analysis_path, 'w', encoding='utf-8') as f:
                json.dump({"error": "Transcript is empty"}, f, ensure_ascii=False, indent=2)
            return True

        # Tạo prompt theo yêu cầu của bạn
        prompt = f"""
Dựa trên nội dung video được phiên âm dưới đây, hãy phân tích và trích xuất các thông tin sau dưới dạng JSON:
1.  `topic`: Chủ đề chính của video (một câu ngắn gọn).
2.  `key_points`: Các điểm kiến thức quan trọng nhất (danh sách 3-5 chuỗi).
3.  `keywords`: Các từ khóa chính (danh sách 3-5 chuỗi).
4.  `level`: Mức độ chuyên sâu (chọn một trong: "Cơ bản", "Trung bình", "Nâng cao").
5.  `mentioned_resources`: Tên các tài liệu hoặc công cụ cụ thể được nhắc đến trong video (danh sách chuỗi, để trống nếu không có).
6.  `full_transcript`: Toàn bộ nội dung phiên âm đã cung cấp.

Nội dung phiên âm:
---
{transcript_text}
---

Chỉ trả về đối tượng JSON hợp lệ:
"""

        # Gọi Gemini API
        response = gemini_model.generate_content(prompt)
        analysis_result = {}

        # Xử lý response
        if response.text:
            try:
                # Cố gắng tìm và parse JSON
                text = response.text
                start_idx = text.find('{')
                end_idx = text.rfind('}')
                if start_idx != -1 and end_idx != -1:
                    json_str = text[start_idx:end_idx+1]
                    analysis_result = json.loads(json_str)
                    # Đảm bảo có trường full_transcript
                    if 'full_transcript' not in analysis_result:
                         analysis_result['full_transcript'] = transcript_text
                else:
                    logging.warning(f"Không tìm thấy JSON hợp lệ trong phản hồi Gemini cho {os.path.basename(transcript_path)}. Lưu text gốc.")
                    analysis_result = {"error": "Invalid JSON response", "raw_response": text, "full_transcript": transcript_text}
            except json.JSONDecodeError as json_err:
                logging.warning(f"Lỗi parse JSON từ Gemini cho {os.path.basename(transcript_path)}: {json_err}. Lưu text gốc.")
                analysis_result = {"error": "JSON Decode Error", "raw_response": response.text, "full_transcript": transcript_text}
        else:
             logging.warning(f"Phản hồi rỗng từ Gemini cho {os.path.basename(transcript_path)}.")
             analysis_result = {"error": "Empty response from Gemini", "full_transcript": transcript_text}

        # Lưu kết quả analysis vào file JSON
        with open(analysis_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_result, f, ensure_ascii=False, indent=2)
        logging.info(f"Đã phân tích thành công: {os.path.basename(analysis_path)}")
        
        # Thêm độ trễ nhỏ để tránh rate limit
        time.sleep(1.5) # Chờ 1.5 giây
        return True

    except Exception as e:
        logging.error(f"Lỗi Gemini API khi xử lý {os.path.basename(transcript_path)}: {e}")
        # Không tạo file analysis lỗi để script có thể thử lại lần sau
        return False

def main():
    logging.info("--- BẮT ĐẦU QUÁ TRÌNH XỬ LÝ VIDEO ---")
    
    # Lấy danh sách file video
    try:
        video_files = [f for f in os.listdir(VIDEO_DIR) if f.lower().endswith('.mp4')]
        logging.info(f"Tìm thấy {len(video_files)} file video MP4 trong thư mục: {VIDEO_DIR}")
    except FileNotFoundError:
        logging.error(f"Lỗi: Không tìm thấy thư mục video: {VIDEO_DIR}")
        return
    except Exception as e:
        logging.error(f"Lỗi khi đọc thư mục video: {e}")
        return

    if not video_files:
        logging.warning("Không tìm thấy file video nào để xử lý.")
        return

    # Xử lý từng video
    processed_count = 0
    for video_filename in tqdm(video_files, desc="Đang xử lý video"):
        video_path = os.path.join(VIDEO_DIR, video_filename)
        audio_path, transcript_path, analysis_path = get_file_paths(video_filename)

        logging.info(f"--- Bắt đầu xử lý: {video_filename} ---")

        # Bước 1: Trích xuất audio
        audio_ok = extract_audio(video_path, audio_path)
        if not audio_ok:
            logging.warning(f"Bỏ qua các bước sau cho {video_filename} do lỗi trích xuất audio.")
            continue # Chuyển sang video tiếp theo

        # Bước 2: Phiên âm audio
        transcript_ok = transcribe_audio(audio_path, transcript_path)
        if not transcript_ok:
            logging.warning(f"Bỏ qua các bước sau cho {video_filename} do lỗi phiên âm.")
            continue # Chuyển sang video tiếp theo

        # Bước 3: Phân tích transcript
        analysis_ok = analyze_transcript(transcript_path, analysis_path)
        if not analysis_ok:
             logging.warning(f"Lỗi phân tích AI cho {video_filename}. Sẽ thử lại lần chạy sau.")
             # Không tăng processed_count nếu analysis lỗi để thử lại
             continue

        processed_count += 1
        logging.info(f"--- Hoàn thành xử lý: {video_filename} ---")

    logging.info(f"--- KẾT THÚC QUÁ TRÌNH XỬ LÝ --- ")
    logging.info(f"Đã kiểm tra {len(video_files)} video.")
    # Lưu ý: processed_count chỉ đếm các video hoàn thành cả 3 bước trong lần chạy này.

if __name__ == "__main__":
    # Đảm bảo các thư mục output tồn tại
    os.makedirs(AUDIO_DIR, exist_ok=True)
    os.makedirs(TRANSCRIPT_DIR, exist_ok=True)
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    main()

