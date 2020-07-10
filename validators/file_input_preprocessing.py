from werkzeug.datastructures import FileStorage
from abc import ABC, abstractmethod
from flask import send_file
from .config import *
from .dataframe_cleaner import Cleaner
from app.logger.logger import Logger
from datetime import datetime
import os
import io
import openpyxl
import uuid
import pandas as pd


# TODO: перестроить архитетуру в соответствии с паттерном "Стретегия" или "Шаблонный метод"


class AbstractInputValidator(ABC, Cleaner):
    @Logger.info_log()
    def __init__(self, fs: FileStorage) -> None:
        """
        Конструктор
        :param fs: бинарный файл
        """

        self.target_file = fs
        self.local_file_name = ""

    @Logger.info_log()
    def _get_file_extension(self) -> str:
        """
        Возвращет расширение файла (.xlsx, .csv, ...)
        :return:
        """

        return os.path.splitext(self.target_file.filename)[1]

    @abstractmethod
    def allowed_file(self) -> bool:
        pass

    @Logger.info_log()
    def save_file(self) -> None:
        """
        Собриает локальное имя файла и путь до него. По полученному пути сохраняет файл.
        :return:
        """

        ext = self._get_file_extension()

        file_uuid = str(uuid.uuid4().hex)

        if not os.path.exists(UPLOAD_FOLDER):
            os.makedirs(UPLOAD_FOLDER)

        self.local_file_name = f"{UPLOAD_FOLDER}/{file_uuid}{ext}"
        self.target_file.save(self.local_file_name, MAX_FILE_SIZE)
        print(self.local_file_name)

    @abstractmethod
    def _is_password_protected(self):
        """
        Абстрактный метод проверки наличия пароля у файла
        :return:
        """
        pass

    @abstractmethod
    def _get_dataframe(self):
        """
        Абстрактный метод получения <pandas.DataFrame> из целевого файла
        :return:
        """
        pass

    @Logger.info_log(session=None)
    def convert_file_to_df(self) -> pd.DataFrame:
        """
        Конверитрует файл в <pandas.DataFrame>
        Очищает полученный DataFrame и возвращает его
        :return: pandas.DataFrame
        """

        if self.allowed_file() == True:
            self.save_file()
            if self._is_password_protected() == False:
                df = self._get_dataframe()
                df = df.copy()
                df = self.drop_duplicate_cols_by_name(df)
                df = self.drop_unnamed_columns(df)
                df = self.drop_thrash_columns(df, trash_content, drop_nul_cols=True, drop_nul_rows=True)
                return df
            else:
                raise RuntimeError("The file is password protected!")
        else:
            raise RuntimeError


class ExcelInputValidator(AbstractInputValidator):
    @Logger.info_log()
    def __init__(self, fs: FileStorage) -> None:
        super().__init__(fs)

    @Logger.info_log()
    def _is_password_protected(self) -> bool:
        """
        Проверяет наличие пароля у excel файла
        :return:
        """

        try:
            wb = openpyxl.load_workbook(filename=self.local_file_name)
            return wb.security.lockStructure is not None
        except:
            raise RuntimeError("Не удалось проверить файл на наличие пароля!")

    @Logger.info_log()
    def _get_dataframe(self) -> pd.DataFrame:
        return pd.read_excel(self.local_file_name)

    @Logger.info_log()
    def allowed_file(self) -> bool:
        """
        Определяет, является ли имя файла допустимым.
        :return: True или Exception
        """

        if self.target_file.filename is None or self.target_file.filename == "":
            raise ValueError("File name is empty!")

        if not "." in self.target_file.filename:
            raise ValueError("File extension is missing!")

        ext = self._get_file_extension()

        if ext.upper() in ALLOWED_EXCEL_EXTENSIONS:
            return True
        else:
            raise ValueError("File is not allowed!")


class CSVInputValidator(AbstractInputValidator):
    """
    Вариант валидатора на вход для .csv файлов
    """

    @Logger.info_log()
    def __init__(self, fs: FileStorage, sep: str = ",") -> None:
        super().__init__(fs)
        self.sep = sep

    @Logger.info_log()
    def _is_password_protected(self) -> bool:
        """
        Файлы .csv не имеют пароля, всегда возвращает False
        :return: False
        """
        return False

    @Logger.info_log()
    def _get_dataframe(self) -> pd.DataFrame:
        """
        Метод, собирающий pandas.DataFrame из .csv файла
        :return:
        """
        return pd.read_csv(self.local_file_name, sep=self.sep)

    @Logger.info_log()
    def allowed_file(self) -> bool:
        """
        Определяет, является ли имя файла допустимым.
        :return: True или Exception
        """

        if self.target_file.filename is None or self.target_file.filename == "":
            raise ValueError("File name is empty!")

        if not "." in self.target_file.filename:
            raise ValueError("File extension is missing!")

        ext = self._get_file_extension()

        if ext.upper() == ".CSV":
            return True
        else:
            raise ValueError("File is not allowed!")


@Logger.info_log()
def convert(validator: AbstractInputValidator) -> pd.DataFrame:
    """
    Функция, вызываемая клиентом. Вызывает шаблонный метод convert_file_to_df()
    :param validator: Один из вариантов валидатора в соответствии с форматом файла
    :return: pandas.DataFrame
    """
    return validator.convert_file_to_df()


class OutputValidator:
    # TODO: конвертурует датафрейм в определенный формат файла в соответсвтии с переданным флагом
    """
    Класс, позволяющий производить валидацию <pandas.DataFrame>,
    записывать владиный <pandas.DataFrame> в файл и отправлять его клиенту.
    """

    @Logger.info_log()
    def __init__(self, df: pd.DataFrame) -> None:
        """
        Конструктор
        :param df: целевой датафрейм
        """

        self.target_df = df.copy()

    @Logger.info_log()
    def _validate(self) -> bool:
        """
        Метод, позволяющий проверить валидность целевого датаврейма.
        :return: None
        """

        if self.target_df is None:
            return False

        if not isinstance(self.target_df, pd.DataFrame):
            return False
        else:
            if len(self.target_df.index) == 0:
                return False
            else:
                return True

    @Logger.info_log()
    def write_file_to_df(self, mimetype: str):
        """
        Метод, позволяющий записать целевой датайрейм в файл
        :param mimetype: тип данных в соответствии со стандартом MIME
        :return: ?
        """

        if self._validate():
            towrite = io.BytesIO()
            self.target_df.to_excel(towrite, engine="openpyxl")
            towrite.seek(0)
            dt = str(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
            file_uuid = uuid.uuid4().hex
            # TODO: проверять, что переданный mimetype - допустимый. Например, перечислить допустимые варианты в словаре
            if mimetype in ALLOWED_MIMETYPES.keys():
                return send_file(
                        towrite,
                        as_attachment=True,
                        attachment_filename=f"{file_uuid}({dt}).xlsx",
                        mimetype=mimetype
                    )
            else:
                raise TypeError("The file's mime type isn't allowed!")
