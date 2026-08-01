import pytest
import os
import sys

# Lấy đường dẫn thư mục chứa file runner này
dir_root = os.path.dirname(os.path.realpath(__file__))

# Chuyển hướng làm việc về thư mục gốc
os.chdir(dir_root)

# Bỏ qua việc tạo file bytecode (.pyc) trên cluster
sys.dont_write_bytecode = True

# Chạy pytest với các tham số được truyền từ VS Code
retcode = pytest.main(sys.argv[1:])