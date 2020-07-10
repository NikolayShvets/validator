import pandas as pd
import numpy as np
from typing import List


class Cleaner:
    """
    Статический класс, содержащий методы по очистке pandas.DataFrame
    """

    @staticmethod
    def drop_duplicate_rows(df: pd.DataFrame, subset: List[str] = None) -> pd.DataFrame:
        """
        Метод, позволяющий удалять повторяющиеся строки целевого датафрейма, кроме первого вхождения
        :param df: целевой датафрейм
        :param subset: перечисление имен столбцов, по которым необходимо произвести сравнение. По умолчанию
                        сравнение производнится по всем столбцам
        :return: целевой датафрейм с уникальными строками по указанным столбцам
        """

        df = df.copy()
        df.drop_duplicates(subset=subset, inplace=True)
        return df

    @staticmethod
    def drop_duplicate_cols_by_name(df: pd.DataFrame) -> pd.DataFrame:
        """
        Метод, позволяющий удалять колонки с повторяющимися именами, кроме первого вхождения
        :param df: целевой датафрейм
        :return: датафрейм с уникальными колонками
        """

        df = df.copy()
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    @staticmethod
    def drop_thrash_columns(df: pd.DataFrame, trash_set: list, drop_nul_cols=False, drop_nul_rows=False) -> pd.DataFrame:
        """
        Метод, позволяющий заменить все значения целевого датафрейма, входящие в список <trash_set>, на numpy.NaN
        :param drop_nul_rows: флаг удаления строк, состоящих только из np.NaN
        :param drop_nul_cols: флаг удаления колонок, стостоящих только из np.NaN
        :param trash_set: список недопустимых значений
        :param df: целевой датафрейм
        :return:
        """

        df = df.copy()
        df.replace(to_replace="\s{2,}", value=np.NaN, regex=True, inplace=True)
        value = [np.NaN for i in range(len(trash_set))]
        df.replace(trash_set, value, inplace=True)
        if drop_nul_rows:
            df.dropna(how="all", inplace=True)
            df.reset_index(drop=True, inplace=True)
        if drop_nul_cols:
            df.dropna(axis=1, how="all", inplace=True)
            df.reset_index(drop=True, inplace=True)
        return df

    @staticmethod
    def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Метод, позволяющий удалить все колонки целевого датафрейма, не имеющие имени
        :param df: целевой датафрейм
        :return: датафрейм без непроименованных колонок
        """

        df = df.copy()
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        return df

    # TODO: вынести удаление нулевых строк и столбцов в отдельную функцию?
    @staticmethod
    def drop_nulls(df: pd.DataFrame):
        df = df.copy()
