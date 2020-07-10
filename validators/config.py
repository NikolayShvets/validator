"""
Файл настроек для валидаторов входа и выхода
"""

import os
import pandas as pd

ALLOWED_EXCEL_EXTENSIONS = [
    ".XLSX",
    ".XLSM",
    ".XLSB",
    ".XLTX",
    ".XLS"
]
ALLOWED_MIMETYPES = {
    "application/vnd.ms-excel": True
}
# Список недопустимых строк
trash_content = ["", " "]
MAX_FILE_SIZE = 1024 * 1024 * 30 + 1
UPLOAD_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), "upload")



