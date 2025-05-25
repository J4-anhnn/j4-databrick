# j4-databrick

## Overview
Pipeline Data Engineering với cả batch & streaming, bảo mật dữ liệu, DLT pipeline, auto-deploy bằng Databricks Asset Bundle & GitHub Actions.

## Flow sử dụng
1. **Batch ingest:** Chạy notebook `01_ingest_batch.py` lấy dữ liệu từ `data_sample.csv` vào Delta Lake staging.
2. **Streaming ingest:** Chạy `02_stream_pusher.py` để mô phỏng ghi data từng dòng/nhóm dòng vào folder `/tmp/j4_databrick_streaming`.
3. **DLT pipeline:** Triển khai pipeline qua asset bundle, gồm batch và streaming, enrich, privacy masking, partition tối ưu.
4. **CI/CD:** Deploy tự động mỗi lần push mã nguồn.

## Thư mục
- `src/notebooks/`: ingest batch, tạo streaming input
- `src/pipelines/`: DLT pipeline gồm batch và streaming
- `deployment/`: asset bundles config
- `tests/`: unit test logic masking
- `.github/workflows/`: CI/CD workflow
- `data_sample.csv`: dữ liệu mẫu

## Chạy thử:
- B1: Run ingest batch hoặc push stream input.
- B2: Deploy pipeline (CLI hoặc CI/CD).
- B3: Kiểm tra các bảng `opportunity_final_*`.

## Author & License
by J4-anhnn - MIT License.
