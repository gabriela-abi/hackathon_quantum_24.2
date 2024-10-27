import pandas as pd
import os

folder_path = "file/path"

# Lista todos os arquivos CSV na pasta
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Inicializa um DataFrame vazio para armazenar os dados combinados
merged_df = pd.DataFrame()

# Percorre os arquivos CSV e os adiciona ao DataFrame combinado
for csv_file in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Lê o arquivo CSV com os tipos de dados especificados
    df = pd.read_csv(file_path, dtype=str)
    
    # Adiciona os dados ao DataFrame combinado
    merged_df = pd.concat([merged_df, df], ignore_index=True)

# Reinicializa o índice para otimizar o uso de memória
merged_df.reset_index(drop=True, inplace=True)

# Remove colunas desnecessárias para otimização e simplificação dos dados
df = df.drop(columns=["Citi Code", "Fund Code", "Dynamic Currency", 
                      "Amount of Shares Redeemed Fund", "Amount of Shares Redeemed Share Class", 
                      "Amount of Shares Subscribed Fund", "Amount of Shares Subscribed Share Class", 
                      "Number of Shares Redeemed Fund", "Number of Shares Redeemed Share Class", 
                      "Number of Shares Subscribed Fund", "Number of Shares Subscribed Share Class"])

# Seleciona as colunas de valor, excluindo datas e códigos ISIN
value_columns = [col for col in df.columns if 'Date' not in col and 'ISIN' not in col]
value_columns = sorted(value_columns)

# Seleciona as colunas de data para análise temporal
date_columns = [col for col in df.columns if 'Date' in col]
date_columns = sorted(date_columns)

# Inicializa uma lista vazia para armazenar as mudanças
changes = []

# Verifica se os pares estão corretos
if len(value_columns) == len(date_columns):
    print("Número de colunas de valor e data estão corretos.")

    # Itera pelo DataFrame agrupando por ISIN
    for current_id, sub_df in df.groupby('ISIN'):
        print(f"Processando ISIN: {current_id}")

        # Itera pelas colunas de valor e data emparelhadas
        for value_col, date_col in zip(value_columns, date_columns):

            # Cria uma máscara booleana para detectar mudanças
            changed_mask = sub_df[value_col] != sub_df[value_col].shift()

            # Filtra as linhas onde mudanças ocorreram
            changes_detected = sub_df[changed_mask]

            # Adiciona informações de mudança à lista
            for index in changes_detected.index:
                try:
                    # Obtém a localização do índice atual
                    current_loc = sub_df.index.get_loc(index)
                    
                    # Se o índice atual não for o primeiro, obtenha o valor antigo
                    if current_loc > 0:
                        previous_index = sub_df.index[current_loc - 1]
                        old_value = sub_df.loc[previous_index, value_col]
                    else:
                        old_value = None
                        
                    changes.append({
                        'ISIN': current_id,
                        'changed_column': value_col,
                        'old_value': old_value,
                        'new_value': sub_df.loc[index, value_col],
                        'date_changed': sub_df.loc[index, date_col],
                    })
                except KeyError as e:
                    print(f"Erro ao acessar o índice anterior: {e}")

# Cria um DataFrame a partir da lista de mudanças
changes_df = pd.DataFrame(changes)

# Exibir resumo das mudanças detectadas
print(f"Número total de mudanças detectadas: {len(changes_df)}")
if not changes_df.empty:
    print("Mudanças registradas:")
    print(changes_df)
else:
    print("Nenhuma mudança detectada.")
    
    
# Cria uma cópia do DataFrame de mudanças para preservar o original
editando_df = changes_df.copy()

# Filtra para manter apenas as linhas onde a coluna 'date_changed' não está vazia
editando_df = editando_df[editando_df['date_changed'].notna()]

# Ordena o DataFrame pela coluna 'changed_column' em ordem decrescente
editando_df = editando_df.sort_values(['changed_column'], ascending=[False])

# Ordena o DataFrame pela coluna 'old_value', colocando valores nulos no início
editando_df = editando_df.sort_values(by='old_value', na_position='first')

def remove_first_occurrence_group(editando_df, id_column, value_column):
    def process_group(group):
        # Encontra os índices da primeira ocorrência de cada valor único dentro do grupo
        first_occurrences = group.drop_duplicates(subset=[value_column], keep='first').index
        # Remove essas primeiras ocorrências
        return group.drop(index=first_occurrences)

    # Aplica a função em cada grupo de ID
    editando_df = editando_df.groupby(id_column, group_keys=False).apply(process_group)
    
    return editando_df

# Aplicando a função ao DataFrame
editando_df = remove_first_occurrence_group(editando_df, 'ISIN', 'changed_column')

# Salvando o df final
editando_df.to_csv('quanti_valores_alterados.csv', sep=',', index=False)