import logging
import os
import pandas as pd
import re

ROOT_DATA_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'data')
INPUT_DATA_PATH = os.path.join(ROOT_DATA_PATH, 'input')
OUTPUT_DATA_PATH = os.path.join(ROOT_DATA_PATH, 'output')

logger = logging.getLogger(__name__)

def load_crimes_data():
    """Loads and transforms crimes data"""
    read_csv_common_kwargs = {
        'filepath_or_buffer': os.path.join(INPUT_DATA_PATH, 'delitos_por_municipio.csv'),
        'sep': ';',
        'index_col': 0,
        'skiprows': 4,
        'encoding': 'latin-1'
    }

    df_crimes_raw = pd.read_csv(**read_csv_common_kwargs, header = [0, 1], skipfooter = 7, engine = 'python')

    # drop null cols
    cols_bool_mask = df_crimes_raw.isna().all(axis = 0)
    df_crimes = df_crimes_raw.drop(columns = df_crimes_raw.columns[cols_bool_mask])

    # set columns as multi-index
    headers = pd.read_csv(**read_csv_common_kwargs, header = None, nrows = 2, keep_default_na = True)
    header_levels = [row[row.notna()].unique() for _, row in headers.iterrows()]
    df_crimes.columns = pd.MultiIndex.from_product(header_levels)

    # prettify column names
    df_crimes.rename(columns = lambda x: re.sub('(\d+\.)+-', '', x), inplace = True)

    # simplify municipality name
    df_crimes.index = df_crimes.index.str.replace('- Municipio de | \(Las\)', '', regex = True)

    return df_crimes

def load_income_data():
    """Loads and transforms income data"""
    df_income_raw = pd.read_csv(
        os.path.join(INPUT_DATA_PATH, 'renta_por_hogar.csv'), 
        sep = ';'
    )
    df_income_raw['Total'] = pd.to_numeric(df_income_raw['Total'], errors = 'coerce')

    # pivot data
    df_income = df_income_raw.pivot_table(index = 'Municipios', columns = ['Indicadores de renta media y mediana', 'Periodo'], values = 'Total')

    # note: before computing zip code and municipality name we could flatten the column multi-index
    df_income[['CP','Municipio']] = df_income.index.str.extract('(\d{5}) ([\w\s]+)(?:,\w+)?', expand = True).values.tolist()
    df_income['CP'] = pd.to_numeric(df_income['CP'])

    # remove column index names and drop municipality (row) index
    df_income.columns.names = (None, None)
    df_income = df_income.reset_index(drop = True).reindex(columns = df_income.columns[-2:].tolist() + df_income.columns[:-2].tolist())

    return df_income

def load_call_center_data():
    """Loads and transforms call center data"""
    df_cc_raw = pd.read_csv(
        os.path.join(INPUT_DATA_PATH, 'contac_center_data.csv'),
        sep = ';'
    )

    df_cc = df_cc_raw.groupby(
        by = ['sessionID', 'DNI', 'Telef', 'CP', 'duration_call_mins'],
        as_index = False,
        dropna = True
    ).agg(
        funnel_steps = ('funnel_Q', list),
        product = ('Producto', lambda x: pd.unique(x)[0])
    )

    # add a dummy multi-index to simplify the juntion with the other data
    df_cc.columns = pd.MultiIndex.from_tuples([(col, '') for col in df_cc.columns])

    return df_cc

def main():
    """Loads all data, transforms (joins) it and stores a single file as output"""
    logger.info('Loading crimes data')
    df_crimes = load_crimes_data()
    logger.info('Loading income data')
    df_income = load_income_data()
    logger.info('Loading call center data')
    df_cc = load_call_center_data()

    logger.info('Transforming (joining) data')
    df_tmp = df_income.merge(df_crimes, how = 'inner', left_on = 'Municipio', right_index = True)
    df_all = df_cc.merge(df_tmp.set_index('CP'), how = 'inner', left_on = 'CP', right_index = True)
    
    logger.info('Writing output file')
    df_all.reset_index(drop = True).to_csv(
        os.path.join(OUTPUT_DATA_PATH, 'out_data.csv'), 
        sep = ';'
    )

if __name__ == '__main__':
    logging.basicConfig(
        level = logging.INFO,
        format = '[%(asctime)s] {%(filename)s} %(levelname)s: %(message)s'
    )
    main()
