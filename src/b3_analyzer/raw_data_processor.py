# src/b3_analyzer/raw_data_processor.py

import os
import zipfile
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tqdm import tqdm
from io import StringIO
import gc

# --- Constantes de Layout e Limpeza ---
COTAHIST_LAYOUT = {
    'TIPREG': (1, 2), 'DATA_PREGAO': (3, 10), 'CODBDI': (11, 12), 'CODNEG': (13, 24),
    'TPMERC': (25, 27), 'NOMRES': (28, 39), 'ESPECI': (40, 49), 'PRAZOT': (50, 52),
    'MODREF': (53, 56), 'PREABE': (57, 69), 'PREMAX': (70, 82), 'PREMIN': (83, 95),
    'PREMED': (96, 108), 'PREULT': (109, 121), 'PREOFC': (122, 134), 'PREOFV': (135, 147),
    'TOTNEG': (148, 152), 'QUATOT': (153, 170), 'VOLTOT': (171, 188), 'PREEXE': (189, 201),
    'INDOPC': (202, 202), 'DATVEN': (203, 210), 'FATCOT': (211, 217), 'PTOEXE': (218, 230),
    'CODISI': (231, 242), 'DISMES': (243, 245),
}
COLSPECS = [(v[0] - 1, v[1]) for k, v in COTAHIST_LAYOUT.items()]
NAMES = list(COTAHIST_LAYOUT.keys())
DATE_COLS = ['DATA_PREGAO', 'DATVEN']
PRICE_COLS = ['PREABE', 'PREMAX', 'PREMIN', 'PREMED', 'PREULT', 'PREOFC', 'PREOFV', 'PREEXE', 'VOLTOT']
INT_COLS = ['TIPREG', 'CODBDI', 'TPMERC', 'TOTNEG', 'QUATOT', 'FATCOT', 'INDOPC', 'DISMES', 'PRAZOT', 'PTOEXE']


def extract_zip_files(raw_path: Path, texts_path: Path):
    """
    Extrai arquivos .zip de um diretório de origem para um de destino,
    garantindo que todos os arquivos de saída tenham a extensão .txt.
    """
    print("\n--- Etapa 1: Extraindo arquivos ZIP ---")
    os.makedirs(texts_path, exist_ok=True)
    zip_files = [f for f in os.listdir(raw_path) if f.lower().endswith('.zip')]
    if not zip_files:
        print(" -> Nenhum arquivo .zip encontrado em data/raw.")
        return
        
    print(f" -> Encontrados {len(zip_files)} arquivos ZIP para extrair.")
    extracted_count = 0
    for filename in tqdm(zip_files, desc="Extraindo arquivos ZIP"):
        zip_path = raw_path / filename
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    base_name = os.path.basename(member)
                    
                    # --- LÓGICA ORIGINAL DO NOTEBOOK RESTAURADA ---
                    # Garante que arquivos sem extensão recebam .TXT
                    if not base_name.lower().endswith('.txt'):
                        output_filename = texts_path / (base_name + '.TXT')
                    else:
                        output_filename = texts_path / base_name
                    # --- FIM DA LÓGICA RESTAURADA ---

                    with zip_ref.open(member) as source, open(output_filename, 'wb') as target:
                        shutil.copyfileobj(source, target)
                    extracted_count += 1

        except zipfile.BadZipFile:
            print(f" -> AVISO: O arquivo '{filename}' não é um ZIP válido. Pulando.")
        except Exception as e:
            print(f" -> ERRO ao extrair '{filename}': {e}")
            
    print(f" -> Total de {extracted_count} arquivos extraídos para a pasta 'texts'.")


def process_text_to_parquet(texts_path: Path, processed_path: Path):
    """
    Processa arquivos de texto e consolida em um único Parquet otimizado.
    """
    print("\n--- Etapa 2: Processando arquivos TXT para Parquet ---")
    os.makedirs(processed_path, exist_ok=True)
    
    files_to_process = sorted([f for f in os.listdir(texts_path) if f.lower().endswith('.txt')])
    if not files_to_process:
        print(" -> Nenhum arquivo .txt encontrado em data/texts para processar.")
        return

    print(f" -> Encontrados {len(files_to_process)} arquivos de texto para consolidar.")
    
    FINAL_PARQUET_PATH = processed_path / 'dados_b3.parquet'
    BATCH_SIZE = 500_000
    writer = None
    final_schema = None

    if os.path.exists(FINAL_PARQUET_PATH):
        os.remove(FINAL_PARQUET_PATH)
        print(f" -> Arquivo parquet antigo '{FINAL_PARQUET_PATH}' removido.")

    for filename in tqdm(files_to_process, desc="Processando Arquivos"):
        file_path = texts_path / filename
        try:
            with open(file_path, 'r', encoding='latin-1') as f:
                lines = f.readlines()

            content_lines = lines[1:-1]
            if not content_lines: continue

            for i in range(0, len(content_lines), BATCH_SIZE):
                batch_lines = content_lines[i:i + BATCH_SIZE]
                data_io = StringIO("".join(batch_lines))
                
                df_batch = pd.read_fwf(data_io, colspecs=COLSPECS, names=NAMES, header=None)
                if df_batch.empty: continue

                # Limpeza e conversão de tipos
                for col in df_batch.select_dtypes(['object']).columns:
                    df_batch[col] = df_batch[col].str.strip()
                for col in DATE_COLS:
                    df_batch[col] = pd.to_datetime(df_batch[col], format='%Y%m%d', errors='coerce')
                for col in PRICE_COLS:
                    df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce') / 100
                for col in INT_COLS:
                    df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce').astype('Int64')

                table = pa.Table.from_pandas(df_batch, preserve_index=False)
                
                if writer is None:
                    final_schema = table.schema
                    writer = pq.ParquetWriter(FINAL_PARQUET_PATH, final_schema, compression='snappy')
                
                table = table.cast(final_schema)
                writer.write_table(table)
        except Exception as e:
            print(f" -> ERRO CRÍTICO ao processar '{filename}': {e}. Pulando.")
        
        gc.collect()

    if writer:
        writer.close()
        print(f"\n -> [SUCESSO] Todos os arquivos foram consolidados em: {FINAL_PARQUET_PATH}")
    else:
        print("\n -> Nenhum dado foi processado para o arquivo Parquet.")