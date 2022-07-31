# API предоставляющий методы для работы с датасетами pandas

## Описание классов

### ApplyTransformer(BaseSingleTransformer)

Трансформер для применения функции преобразования к колонке табличных данных
        
- :param name: str - имя трансформера
        
- :param input_col: str - имя преобразуемой колонки
        
- :param output_col: str - имя колонки с результатом
        
- :param apply_func: Callable Method - функция преобразования

        Example Function:

        def apply_func(data: DataFrame, *args) -> DataFrame:
            return result

### MultipleTransformer(BaseMultipleTransformer)

Трансформер для применения функции преобразования к колонкам табличных данных
        
- :param name: str - имя трансформера
        
- :param input_columns: str - имя преобразуемых колонок
        
- :param output_columns: str - имя колонок с результатами
        
- :param apply_func: Callable Method - функция преобразования

        Example Function:

        def apply_func(data: DataFrame, *args) -> DataFrame:
            return result

### TransformersPipelane

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

В качестве результата возвращает DataFrame - полученный конкатенацией по оси 1 результатов применения всех трансформеров.

Функционал классов трансформеров расширяется путем использования процедур, изменяющих подаваемый датасет:

    def some_procedure(df: pandas.DataFrame, *args):
        df.some_function(*args, inplace=True)

В данном случае, такие трансформеры требуется выносить в отдельный пайплайн.