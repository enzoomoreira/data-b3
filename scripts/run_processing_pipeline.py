# scripts/run_processing_pipeline.py

import sys
from pathlib import Path
import os

# --- AJUSTE DE PATH PARA IMPORTAÇÃO ---
# Adiciona o diretório 'src' ao path do sistema para encontrar nosso pacote
project_root = Path(__file__).resolve().parents[1]
src_path = project_root / 'src'
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

# --- IMPORTAÇÕES DOS NOSSOS MÓDULOS ---
# Importa as funções de cada etapa do pipeline
from b3_analyzer.raw_data_processor import extract_zip_files, process_text_to_parquet
from b3_analyzer.dictionary_builder import create_code_dictionaries, create_security_master

# --- CONFIGURAÇÃO DOS CAMINHOS DO PROJETO ---
DATA_PATH = project_root / 'data'
RAW_PATH = DATA_PATH / 'raw'
TEXTS_PATH = DATA_PATH / 'texts'
PROCESSED_PATH = DATA_PATH / 'processed'
OUTPUTS_PATH = DATA_PATH / 'outputs'

def run_full_pipeline():
    """
    Orquestra o pipeline completo de processamento de dados da B3.

    Fluxo de Execução:
    1. Extrai os arquivos .zip da pasta 'raw' para a pasta 'texts'.
    2. Converte e consolida os arquivos .txt em um único arquivo Parquet otimizado
       na pasta 'processed'.
    3. Gera os dicionários de códigos (CODBDI, TPMERC) na pasta 'outputs'.
    4. Gera o dicionário master de ativos (security master) na pasta 'outputs'.
    """
    print("="*60)
    print("--- INICIANDO PIPELINE COMPLETO DE PROCESSAMENTO DE DADOS B3 ---")
    print("="*60)
    
    # Garante que os diretórios de saída existam
    os.makedirs(TEXTS_PATH, exist_ok=True)
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    os.makedirs(OUTPUTS_PATH, exist_ok=True)

    # --- ETAPA 1: Extração ---
    extract_zip_files(RAW_PATH, TEXTS_PATH)
    
    # --- ETAPA 2: Processamento para Parquet ---
    process_text_to_parquet(TEXTS_PATH, PROCESSED_PATH)
    
    # --- ETAPA 3: Geração dos Dicionários ---
    # As funções de dicionário usam o Parquet gerado na etapa anterior
    create_code_dictionaries(OUTPUTS_PATH)
    create_security_master(PROCESSED_PATH, OUTPUTS_PATH)
    
    print("\n--- PIPELINE COMPLETO CONCLUÍDO COM SUCESSO ---")
    print("Os dados estão prontos para serem consultados com o módulo `b3_analyzer.analyzer`.")

if __name__ == '__main__':
    # Permite que o script seja executado diretamente do terminal
    run_full_pipeline()