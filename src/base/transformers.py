from itertools import repeat
from dataclasses import dataclass
from typing import Callable, Union, List, Dict
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


def get_current_time_str():
    return str(datetime.now()).split('T')[0]


@dataclass
class BaseSingleTransformer:
    """
    Базовый класс трансформера для колонок датафрейма

    Constructor:
        :param name - наименование трансформера
        :param input_col - наименование преобразуемой колонки
        :param output_col - наименование колонки, куда пишется результат
    """
    name: str = None
    input_col: str = None
    output_col: str = None

    def __repr__(self):
        return '; '.join(
            [
                self.name,
                self.input_col,
                self.output_col,
            ]
        )


@dataclass
class BaseMultipleTransformer:
    """
        Базовый класс трансформера для преобразования списка колонок датафрейма

        Constructor:
            :param name - наименование трансформера
            :param input_columns - список преобразуемых колонок
            :param output_columns - список колонок с результатом преобразования
        """
    name: str = None
    input_columns: List[str] = None
    output_columns: List[str] = None

    def __repr__(self):
        return '; '.join(
            [
                self.name,
                ', '.join(self.input_columns),
                ', '.join(self.output_columns),
            ]
        )


class ApplyTransformer(BaseSingleTransformer):
    def __init__(self, name: str, input_col: str, output_col: str, apply_func: Callable):
        """
        Трансформер для применения функции преобразования к колонке табличных данных
        :param name: str - имя трансформера
        :param input_col: str - имя преобразуемой колонки
        :param output_col: str - имя колонки с результатом
        :param apply_func: Callable Method - функция преобразования

        Example Function:

        def apply_func(data: DataFrame, *args) -> DataFrame:
            return result
        """
        super().__init__(name, input_col, output_col)
        self.apply_func = apply_func

    def transform(self, data: pd.DataFrame) -> pd.Series:
        res = self.apply_func(data, self.input_col, self.output_col)
        print(
            get_current_time_str(),
            f'Transformer: {self.name} finished! Output columns: {self.output_col} '
        )
        return res


class MultipleTransformer(BaseMultipleTransformer):
    def __init__(self, name: str, input_columns: List[str], output_columns: List[str], apply_func: Callable):
        """
        Трансформер для применения функции преобразования к колонкам табличных данных
        :param name: str - имя трансформера
        :param input_columns: str - имя преобразуемых колонок
        :param output_columns: str - имя колонок с результатами
        :param apply_func: Callable Method - функция преобразования

        Example Function:

        def apply_func(data: DataFrame, *args) -> DataFrame:
            return result
        """
        super().__init__(name, input_columns, output_columns)
        self.apply_func = apply_func

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        res = self.apply_func(data, self.input_columns, self.output_columns)
        print(
            get_current_time_str(),
            f'Transformer: {self.name} finished! Output columns: {self.output_columns} '
        )
        return res


class TransformersPipelane:
    """
    Класс расширяет функционал применения трансформеров до пайплайна.

    Поддерживается 2-а формата расчета:

    - синхронный: последовательное применение трансформеров;
    - асинхронный: параллельное применение трансформеров;

    Управляется параметром: mcalc_type -> dict:
        - single
        - multiple

        default: mcalc_type = {
                'type': 'single',
                'num_executors': 1,
                'chunk_size': 1,
                }

    """

    def __init__(
            self,
            transformers,
            mcalc_type=None):

        self.SINGLE = 'single'
        self.MULTIPLE = 'multiple'

        if mcalc_type is None:
            mcalc_type = {
                'type': self.SINGLE,
                'num_executors': 1,
                'chunk_size': 1,
            }
        elif not isinstance(mcalc_type, dict):
            raise TypeError(mcalc_type)
        elif len(mcalc_type) != 3:
            raise ValueError(mcalc_type)
        elif 'type' not in list(mcalc_type.keys()) \
                or 'num_executors' not in list(mcalc_type.keys()) \
                or 'chunk_size' not in list(mcalc_type.keys()):
            raise ValueError(mcalc_type)
        # elif self.SINGLE in mcalc_type['type'] or self.MULTIPLE in mcalc_type['type']:
        #     raise ValueError(mcalc_type['type'])
        elif not isinstance(mcalc_type['num_executors'], int):
            raise ValueError(mcalc_type['num_executors'])
        # TODO: добавить проверку на float значение параметра chunk_size - от 0 до 1
        elif not isinstance(mcalc_type['chunk_size'], int):
            raise ValueError(mcalc_type['chunk_size'])

        self.transformers = transformers

        self.calculation_type = mcalc_type['type']

        self.threads_count = mcalc_type['num_executors']

    @staticmethod
    def multiple_run(data: pd.DataFrame, transformer, *args):
        return transformer.transform(data)

    def transform(self, data: pd.DataFrame):

        if self.calculation_type == self.SINGLE:
            res = [data.reset_index(drop=True)]
            for transformer in self.transformers:
                res.append(transformer.transform(data))
            return pd.concat(res, axis=1)

        if self.calculation_type == self.MULTIPLE:
            with ThreadPoolExecutor(max_workers=self.threads_count) as executor:
                res = executor.map(self.multiple_run, repeat(data), self.transformers)

            result_data_list = list(res)

            return pd.concat([data.reset_index(drop=True)] + result_data_list, axis=1)
