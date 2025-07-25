# src/b3_analyzer/dictionary_builder.py

import pandas as pd
from pathlib import Path
import time

def create_code_dictionaries(output_path: Path):
    """Gera e salva os dicionários de mapeamento para CODBDI e TPMERC."""
    print("\n--- Gerando Dicionários de Códigos (CODBDI e TPMERC) ---")
    try:
        map_codbdi = {
            '02': 'LOTE PADRÃO', '05': 'SANCIONADAS', '06': 'CONCORDATÁRIAS',
            '07': 'RECUPERAÇÃO EXTRAJUDICIAL', '08': 'RECUPERAÇÃO JUDICIAL',
            '09': 'REGIME DE ADMINISTRAÇÃO ESPECIAL TEMPORÁRIA', '10': 'DIREITOS E RECIBOS',
            '11': 'INTERVENÇÃO', '12': 'FUNDOS IMOBILIÁRIOS', '14': 'CERTIFICADOS DE INVESTIMENTO',
            '18': 'OBRIGAÇÕES', '22': 'BÔNUS (PRIVADOS)', '26': 'APÓLICES/BÔNUS/TÍTULOS (PÚBLICOS)',
            '32': 'EXERCÍCIO DE OPÇÕES DE COMPRA DE ÍNDICES', '33': 'EXERCÍCIO DE OPÇÕES DE VENDA DE ÍNDICES',
            '38': 'EXERCÍCIO DE OPÇÕES DE COMPRA', '42': 'EXERCÍCIO DE OPÇÕES DE VENDA',
            '46': 'LEILÃO DE AÇÕES EM MORA', '48': 'LEILÃO DE AÇÕES (ART. 49)',
            '49': 'LEILÃO DE AÇÕES', '50': 'LEILÃO DE AÇÕES', '51': 'LEILÃO DE AÇÕES',
            '52': 'LEILÃO DE AÇÕES', '53': 'LEILÃO DE AÇÕES', '54': 'LEILÃO DE AÇÕES',
            '56': 'LEILão DE AÇÕES', '58': 'LEILÃO', '60': 'LEILÃO', '61': 'LEILÃO',
            '62': 'LEILÃO', '66': 'DEBÊNTURES COM DATA DE VENCIMENTO ATÉ 3 ANOS',
            '68': 'DEBÊNTURES COM DATA DE VENCIMENTO MAIOR QUE 3 ANOS',
            '70': 'FUTURO COM RETENÇÃO DE GANHOS', '71': 'FUTURO COM MOVIMENTAÇÃO DIÁRIA',
            '74': 'OPÇÕES DE COMPRA DE ÍNDICES', '75': 'OPÇÕES DE VENDA DE ÍNDICES',
            '78': 'OPÇÕES DE COMPRA', '82': 'OPÇÕES DE VENDA', '83': 'BOVESPAFIX',
            '84': 'SOMA FIX', '90': 'TERMO', '96': 'FRACIONÁRIO', '99': 'TOTAL'
        }
        df_dict_codbdi = pd.DataFrame(list(map_codbdi.items()), columns=['CODBDI', 'DESCRICAO_CODBDI'])
        df_dict_codbdi['CODBDI'] = pd.to_numeric(df_dict_codbdi['CODBDI'])
        
        map_tpmerc = {
            '010': 'VISTA', '012': 'EXERCÍCIO DE OPÇÕES DE COMPRA',
            '013': 'EXERCÍCIO DE OPÇÕES DE VENDA', '017': 'LEILÃO',
            '020': 'FRACIONÁRIO', '030': 'TERMO', '050': 'FUTURO COM RETENÇÃO DE GANHO',
            '060': 'FUTURO COM MOVIMENTAÇÃO DIÁRIA', '070': 'OPÇÕES DE COMPRA',
            '080': 'OPÇÕES DE VENDA'
        }
        df_dict_tpmerc = pd.DataFrame(list(map_tpmerc.items()), columns=['TPMERC', 'DESCRICAO_TPMERC'])
        df_dict_tpmerc['TPMERC'] = pd.to_numeric(df_dict_tpmerc['TPMERC'])

        path_codbdi = output_path / 'dicionario_codbdi.xlsx'
        path_tpmerc = output_path / 'dicionario_tpmerc.xlsx'
        
        df_dict_codbdi.to_excel(path_codbdi, index=False)
        df_dict_tpmerc.to_excel(path_tpmerc, index=False)
        
        print(f" -> [SUCESSO] Dicionário de CODBDI salvo em: {path_codbdi}")
        print(f" -> [SUCESSO] Dicionário de TPMERC salvo em: {path_tpmerc}")

    except Exception as e:
        print(f" -> [ERRO] Falha ao gerar os Dicionários de Códigos: {e}")

def create_security_master(processed_path: Path, output_path: Path):
    """Gera o Dicionário Master de Ativos (Security Master)."""
    print("\n--- Gerando Dicionário Master de Ativos (Security Master) ---")
    
    parquet_file = processed_path / 'dados_b3.parquet'
    if not parquet_file.exists():
        print(f" -> [ERRO FATAL] O arquivo de dados principal '{parquet_file}' não foi encontrado.")
        return

    try:
        start_time = time.time()
        print(" -> Lendo colunas necessárias do dataset principal...")
        df_full = pd.read_parquet(parquet_file, columns=['DATA_PREGAO', 'CODISI', 'CODNEG', 'NOMRES', 'ESPECI'])
        print(f" -> Leitura concluída em {time.time() - start_time:.2f} segundos.")

        df_ativos = df_full.dropna(subset=['CODISI']).copy()
        
        print(" -> Identificando informações mais recentes de cada ativo...")
        df_base_ativos = (
            df_ativos.sort_values('DATA_PREGAO', ascending=False)
            .drop_duplicates(subset=['CODISI'], keep='first')
            .rename(columns={'CODNEG': 'ULTIMO_TICKER', 'NOMRES': 'ULTIMO_NOME', 'ESPECI': 'ULTIMA_ESPECIFICACAO'})
            .drop(columns='DATA_PREGAO')
        )

        print(" -> Compilando históricos de tickers...")
        df_variacoes_tickers = (
            df_ativos.groupby('CODISI')['CODNEG']
            .apply(lambda x: ' | '.join(sorted([str(item) for item in x.unique() if pd.notna(item)])))
            .reset_index()
            .rename(columns={'CODNEG': 'TICKERS_HISTORICOS'})
        )
        
        print(" -> Compilando históricos de nomes...")
        df_variacoes_nomes = (
            df_ativos.groupby('CODISI')['NOMRES']
            .apply(lambda x: ' | '.join(sorted([str(item) for item in x.unique() if pd.notna(item)])))
            .reset_index()
            .rename(columns={'NOMRES': 'NOMES_HISTORICOS'})
        )

        print(" -> Montando o dicionário final...")
        df_dicionario_ativos = pd.merge(df_base_ativos, df_variacoes_tickers, on='CODISI', how='left')
        df_dicionario_ativos = pd.merge(df_dicionario_ativos, df_variacoes_nomes, on='CODISI', how='left')

        cols_ordem = ['CODISI', 'ULTIMO_TICKER', 'ULTIMO_NOME', 'ULTIMA_ESPECIFICACAO', 'TICKERS_HISTORICOS', 'NOMES_HISTORICOS']
        df_dicionario_ativos = df_dicionario_ativos[cols_ordem].sort_values('ULTIMO_TICKER').reset_index(drop=True)

        output_path_parquet = output_path / 'dicionario_ativos.parquet'
        output_path_excel = output_path / 'dicionario_ativos.xlsx'
        
        df_dicionario_ativos.to_parquet(output_path_parquet, index=False)
        df_dicionario_ativos.to_excel(output_path_excel, index=False, engine='openpyxl')
        
        print(f" -> [SUCESSO] Dicionário com {len(df_dicionario_ativos)} ativos únicos gerado.")
        print(f"    -> Salvo em (Parquet): {output_path_parquet}")
        print(f"    -> Salvo em (Excel):   {output_path_excel}")

    except Exception as e:
        print(f" -> [ERRO] Falha ao gerar o Dicionário Master de Ativos: {e}")