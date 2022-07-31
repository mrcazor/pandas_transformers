from functools import partial
import pandas as pd

from src.base.transformers import ApplyTransformer, TransformersPipelane
from src.utils.functions import benchmark


def power_column(df, input_col, output_col, deg):
    res = df[input_col].apply(lambda x: pow(x, deg))
    res.name = output_col
    return res


transformers_list = []
for i in range(100):
    transformers_list.append(
        ApplyTransformer(
            name=f'deg{i}_transformer',
            input_col='A',
            output_col=f'A_{i}',
            apply_func=partial(power_column, deg=i//10),
        )
    )

@benchmark
def run(type, num_executors, chunk_size):
    mcalc = {
        'type': type,
        'num_executors': num_executors,
        'chunk_size': chunk_size,
    }

    df = pd.read_csv('./data/example.csv')

    transformers = TransformersPipelane(transformers_list, mcalc_type=mcalc)

    res = transformers.transform(df)

    res.to_csv('./result/res.csv')


if __name__ == '__main__':
    run('multiple', 100, 100)
