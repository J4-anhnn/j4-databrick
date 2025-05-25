import pandas as pd
import time

csv_path = "../../sales_data.csv"
df = pd.read_csv(csv_path)

# Đẩy data thành file con vào DBFS streaming
stream_dir = "/dbfs/tmp/j4_databrick_streaming/"
dbutils.fs.mkdirs("/tmp/j4_databrick_streaming")

for i, row in df.iterrows():
    temp = row.to_frame().transpose()
    temp.to_csv(stream_dir + f"part_{i}.csv", header=(i==0), index=False, mode='w')
    time.sleep(2)  # mỗi 2s push 1 row, mô phỏng streaming thực
