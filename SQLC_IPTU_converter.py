# %%
import pandas as pd

# %%
%pip install fsspec

# %%


IPTU_2022 = pd.read_csv('c://Users/americod/Downloads/sendgb-X8QAGTrsugR/IPTU_2022/IPTU_2022.csv', encoding='latin1', sep = ';')

# %%
IPTU_2022.head()

# %%
# Setor e Quadra
IPTU_2022['SETOR'] = IPTU_2022['NUMERO DO CONTRIBUINTE'].str.slice(0,3)
IPTU_2022['QUADRA'] = IPTU_2022['NUMERO DO CONTRIBUINTE'].str.slice(3,6)

# %%
IPTU_2022.fillna(value='00-0', inplace=True)
IPTU_2022.fillna(value=1., inplace=True)
IPTU_2022.fillna(value=0., inplace=True)


# %%
IPTU_2022['SQLC'] = IPTU_2022.apply(
    lambda row: row['NUMERO DO CONTRIBUINTE'][:10] + '00' if row['NUMERO DO CONDOMINIO'] == '00-0'
    else row['NUMERO DO CONTRIBUINTE'][:6] + '0000' + row['NUMERO DO CONDOMINIO'][:2],
    axis=1
)


# %%
IPTU_2022.columns

# %%
# Cálculo do CA
IPTU_2022['CA'] = (IPTU_2022['AREA CONSTRUIDA'] / IPTU_2022['FRACAO IDEAL']) / IPTU_2022['AREA DO TERRENO']

# %%
IPTU_2022['PROJECAO CONSTRUCAO LOTE'] = IPTU_2022['AREA OCUPADA'] / IPTU_2022['AREA DO TERRENO']

# %%
IPTU_2022['VALOR TERRENO UNITARIO'] = IPTU_2022['FRACAO IDEAL'] * IPTU_2022['VALOR DO M2 DO TERRENO'] * IPTU_2022['AREA DO TERRENO']
IPTU_2022['VALOR CONSTRUCAO UNITARIO'] = IPTU_2022['VALOR DO M2 DE CONSTRUCAO'] * IPTU_2022['AREA CONSTRUIDA']

# %%
IPTU_2022.columns

# %%
columns_to_keep = [ 'NUMERO DO CONTRIBUINTE', 'SETOR','QUADRA', 'SQLC', 'TIPO DE PADRAO DA CONSTRUCAO', 'TIPO DE USO DO IMOVEL', 
                    'AREA DO TERRENO', 'AREA CONSTRUIDA', 'AREA OCUPADA','FRACAO IDEAL','VALOR DO M2 DO TERRENO', 'VALOR DO M2 DE CONSTRUCAO', 'CA', 'PROJECAO CONSTRUCAO LOTE',
       'VALOR TERRENO UNITARIO', 'VALOR CONSTRUCAO UNITARIO']

# %%
df = IPTU_2022[columns_to_keep].copy()

# %%
# Importing the excel file

Terminais = pd.read_excel('c://Users/americod/Desktop/Lotes_piu_Terminal.xlsx', dtype=str)

# %%
Terminais

# %%
Terminais.rename(columns={'sqlc': 'SQLC'}, inplace=True)


# %%
Terminais['SQLC_Terminais'] = Terminais['SQLC'].copy()

# %%
Terminais

# %%
df['SQLC_IPTU'] = df['SQLC'].copy()

# %%
merged_data = pd.merge(Terminais, df, on='SQLC', how='inner')


# %%
merged_data.to_csv('Terminais_IPTU2022.csv',encoding='latin1', index = False)

# %%
merged_data = pd.merge(Terminais, df, how='inner', on='SQLC', suffixes=('_Terminais', '_IPTU'))


# %%
merged_data

# %%
merged_data['SQLC'].duplicated().any()

# %%
Eixos['SQLC'].duplicated().any()

# %%
import numpy as np

# %%
df_length = len(merged_data)

# %%
split_length = df_length // 5

# %%
splits = np.array_split(merged_data, 5)

# %%
for i, split in enumerate(splits, start=1):
    file_name = f'IPTU_merged_eixos{i}.csv'
    split.to_csv(file_name, index=False)

# %%
merged_data.to_csv('IPTU_merged_eixos_full.csv', sep = ';', index = False)

# %%
data =  pd.read_csv('Eixos.csv', dtype=str)

# %%
filtered_line = data.loc[data['SQLC'] == '16047002600']



# %%
import pandas as pd
Eixos = pd.read_csv('Eixos.csv', dtype = str)

# %%
# Specify the column names
column_sqlc = 'sqlc'
column_zepec = 'ZEPEC'


# Create a duplicate of the DataFrame
df_eixos = Eixos.copy()

# Remove duplicates in 'sqlc' column and drop rows without 'BIR' in 'ZEPEC' column
df_eixos = df_eixos[df_eixos[column_zepec].str.contains('BIR', na=False) | ~df_eixos.duplicated(subset=column_sqlc)]

# Reset the index of the duplicate DataFrame
df_eixos.reset_index(drop=True, inplace=True)





# %%
df_iptu = pd.read_csv('IPTU_2022_custom_full.csv', sep = ";", dtype=str)



# %%
df_iptu.columns

# %%
df_iptu["AREA DO TERRENO"] = pd.to_numeric(df_iptu["AREA DO TERRENO"], errors='coerce')
df_iptu["FRACAO IDEAL"] = pd.to_numeric(df_iptu["FRACAO IDEAL"], errors='coerce')

# Multiply "AREA DO TERRENO" and "FRACAO IDEAL" columns
df_iptu["AREA x FRACAO IDEAL"] = df_iptu["AREA DO TERRENO"] * df_iptu["FRACAO IDEAL"]


# %%
# Specify the column names
group_columns = ['SQLC', 'TIPO DE PADRAO DA CONSTRUCAO']
area_column = 'AREA DO TERRENO'
value_terreno_column = 'VALOR TERRENO UNITARIO'
value_construcao_column = 'VALOR CONSTRUCAO UNITARIO'

# Convert 'VALOR TERRENO UNITARIO' and 'VALOR CONSTRUCAO UNITARIO' columns to numeric
df_iptu[value_terreno_column] = pd.to_numeric(df_iptu[value_terreno_column], errors='coerce')
df_iptu[value_construcao_column] = pd.to_numeric(df_iptu[value_construcao_column], errors='coerce')

# Group by 'SQLC' and 'TIPO DE PADRAO DA CONSTRUCAO', keeping 'AREA DO TERRENO' and calculating the sum of values
grouped_df = df_iptu.groupby(group_columns).agg({area_column: 'first', value_terreno_column: lambda x: round(x.sum(), 2), value_construcao_column: lambda x: round(x.sum(), 2)}).reset_index()

# Display the grouped DataFrame
print(grouped_df)

# %%
df_eixos = df_eixos.rename(columns={"sqlc": "SQLC"})

# %%
merged_df = pd.merge(df_iptu,df_eixos, on='SQLC', how='inner')

# Display the merged DataFrame
print(merged_df)

# %%
output_filename = 'Eixos_x_IPTU22.csv'

# Set the formatting parameters
formatting_params = {
    'sep': ';',
    'decimal': ',',
    'float_format': '%.2f',
}

# Save the DataFrame to CSV with the specified formatting
merged_df.to_csv(output_filename, index=False, **formatting_params)


