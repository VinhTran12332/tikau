# aggregate_results.py
import os
import json
import pandas as pd
from tqdm import tqdm
import logging

# --- CẤU HÌNH ---
# Đường dẫn đến các thư mục (SỬA LẠI CHO PHÙ HỢP VỚI MÁY BẠN NẾU CẦN)
BASE_DIR = r"C:\Users\DELL\Project_Tiktok_knowledge\data\Favorites"
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis_results")
TRANSCRIPT_DIR = os.path.join(BASE_DIR, "transcripts") # Cần để lấy transcript nếu JSON thiếu
EXCEL_OUTPUT_FILE = os.path.join(BASE_DIR, "tiktok_learnings.xlsx")
LOG_FILE = os.path.join(BASE_DIR, "aggregation.log") # File log riêng cho việc tổng hợp

# --- KẾT THÚC CẤU HÌNH ---

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def read_json_safe(file_path):
    """Đọc file JSON một cách an toàn, trả về None nếu lỗi."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"Lỗi giải mã JSON trong file {os.path.basename(file_path)}: {e}")
        return None
    except FileNotFoundError:
        logging.error(f"Không tìm thấy file JSON: {os.path.basename(file_path)}")
        return None
    except Exception as e:
        logging.error(f"Lỗi không xác định khi đọc file JSON {os.path.basename(file_path)}: {e}")
        return None

def read_transcript_safe(transcript_file_path):
    """Đọc file transcript một cách an toàn."""
    try:
        if not os.path.exists(transcript_file_path):
            return "Transcript file not found."
        with open(transcript_file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Lỗi khi đọc file transcript {os.path.basename(transcript_file_path)}: {e}")
        return "Error reading transcript."

def main():
    logging.info("--- BẮT ĐẦU TỔNG HỢP KẾT QUẢ VÀO EXCEL ---")

    # Lấy danh sách các file JSON kết quả phân tích
    try:
        analysis_files = [f for f in os.listdir(ANALYSIS_DIR) if f.lower().endswith('.json')]
        logging.info(f"Tìm thấy {len(analysis_files)} file JSON phân tích trong thư mục: {ANALYSIS_DIR}")
    except FileNotFoundError:
        logging.error(f"Lỗi: Không tìm thấy thư mục analysis_results: {ANALYSIS_DIR}")
        return
    except Exception as e:
        logging.error(f"Lỗi khi đọc thư mục analysis_results: {e}")
        return

    if not analysis_files:
        logging.warning("Không tìm thấy file JSON phân tích nào để tổng hợp.")
        return

    all_data = []
    for json_filename in tqdm(analysis_files, desc="Đang đọc file JSON phân tích"):
        json_path = os.path.join(ANALYSIS_DIR, json_filename)
        analysis_data = read_json_safe(json_path)

        if analysis_data is None:
            logging.warning(f"Bỏ qua file JSON lỗi: {json_filename}")
            continue # Bỏ qua file lỗi

        # Lấy tên file gốc (bỏ phần .json)
        base_name = os.path.splitext(json_filename)[0]
        video_filename = f"{base_name}.mp4" # Giả định tên file video gốc

        # Lấy thông tin từ JSON
        topic = analysis_data.get('topic', 'N/A')
        key_points_list = analysis_data.get('key_points', [])
        keywords_list = analysis_data.get('keywords', [])
        level = analysis_data.get('level', 'N/A')
        resources_list = analysis_data.get('mentioned_resources', [])
        full_transcript = analysis_data.get('full_transcript')
        raw_response = analysis_data.get('raw_response') # Lấy raw response nếu có lỗi
        error_msg = analysis_data.get('error')

        # Định dạng lại các trường dạng list thành chuỗi, mỗi item một dòng
        key_points_str = "\n".join(f"- {p}" for p in key_points_list) if isinstance(key_points_list, list) else str(key_points_list)
        keywords_str = ", ".join(keywords_list) if isinstance(keywords_list, list) else str(keywords_list)
        resources_str = "\n".join(f"- {r}" for r in resources_list) if isinstance(resources_list, list) else str(resources_list)

        # Nếu thiếu transcript trong JSON (do lỗi hoặc phiên bản cũ), thử đọc từ file .txt
        if not full_transcript and not raw_response and not error_msg:
            transcript_file_path = os.path.join(TRANSCRIPT_DIR, f"{base_name}.txt")
            logging.warning(f"Transcript bị thiếu trong file {json_filename}, đang thử đọc từ {os.path.basename(transcript_file_path)}...")
            full_transcript = read_transcript_safe(transcript_file_path)
        elif raw_response: # Nếu có raw_response (do lỗi JSON), hiển thị nó thay transcript
             full_transcript = f"ERROR - RAW RESPONSE: {raw_response}"
        elif error_msg: # Nếu có lỗi khác
             full_transcript = f"ERROR: {error_msg}"
        elif not full_transcript: # Trường hợp không có lỗi nhưng transcript vẫn rỗng
             full_transcript = "(Transcript not available)"


        all_data.append({
            'Video Filename': video_filename,
            'Topic': topic,
            'Key Points': key_points_str,
            'Keywords': keywords_str,
            'Level': level,
            'Mentioned Resources': resources_str,
            'Full Transcript': full_transcript
        })

    if not all_data:
        logging.warning("Không có dữ liệu hợp lệ nào để ghi vào Excel.")
        return

    # Tạo DataFrame
    df = pd.DataFrame(all_data)

    # Ghi ra file Excel
    try:
        logging.info(f"Đang ghi {len(df)} hàng vào file Excel: {EXCEL_OUTPUT_FILE}")

                # Xử lý các ký tự đặc biệt trong DataFrame trước khi ghi vào Excel
        for col in df.columns:
            # Chuyển đổi tất cả các giá trị trong cột thành chuỗi và loại bỏ ký tự không hợp lệ
            df[col] = df[col].astype(str).apply(lambda x: ''.join(char for char in x if ord(char) < 65536))

        # Sử dụng engine='openpyxl' để hỗ trợ định dạng tốt hơn
        try:
            df.to_excel(EXCEL_OUTPUT_FILE, index=False, engine='openpyxl')
            logging.info("Ghi file Excel thành công!")
        except Exception as e:
            logging.error(f"Lỗi khi ghi file Excel: {e}")
            
            # Thử phương án dự phòng: ghi ra CSV
            csv_output_file = os.path.join(BASE_DIR, "tiktok_learnings.csv")
            logging.info(f"Đang thử ghi ra file CSV: {csv_output_file}")
            try:
                df.to_csv(csv_output_file, index=False, encoding='utf-8-sig')
                logging.info(f"Đã ghi thành công ra file CSV: {csv_output_file}")
            except Exception as csv_error:
                logging.error(f"Lỗi khi ghi file CSV: {csv_error}")

        logging.info("Ghi file Excel thành công!")
    except Exception as e:
        logging.error(f"Lỗi khi ghi file Excel: {e}")

    logging.info("--- KẾT THÚC QUÁ TRÌNH TỔNG HỢP --- ")

if __name__ == "__main__":
    main()