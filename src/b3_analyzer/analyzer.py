# b3_analyzer.py
#
# Um módulo Python para consulta e análise eficiente de dados históricos de
# cotações da B3, armazenados em formato Parquet.

import pandas as pd
from pathlib import Path
from typing import List, Optional, Union
import re

class B3Data:
    """
    Uma classe para carregar e analisar dados históricos de cotações da B3
    de forma eficiente a partir de um arquivo Parquet.

    Esta classe é projetada para lidar com datasets de grande volume (multi-GB)
    sem a necessidade de carregar todo o arquivo na memória para cada consulta.
    Ela utiliza a técnica de "predicate pushdown" do PyArrow para ler do disco
    apenas os dados que correspondem aos filtros solicitados.

    A API foi desenhada com duas camadas de uso:
    1. Acesso Direto: Para usuários que sabem exatamente quais tickers e
       dados desejam, oferecendo a máxima velocidade.
    2. Acesso Exploratório: Para usuários que desejam descobrir ativos por nome
       de empresa ou classe de ativo, oferecendo uma interface mais intuitiva.
    """
    def __init__(self, data_path: str = 'data/processed'):
        """
        Inicializa o analisador B3Data.

        Este método configura os caminhos para os arquivos de dados e pré-carrega
        dicionários de mapeamento essenciais que são pequenos e usados com frequência.
        A carga do dataset principal (Parquet) NÃO ocorre aqui, garantindo uma
        inicialização rápida.

        Args:
            data_path (str): O caminho para o diretório 'processed', que deve
                             conter o arquivo 'dados_b3.parquet'. Espera-se que
                             um diretório irmão chamado 'outputs' contenha os
                             arquivos de dicionário em formato .xlsx e .parquet.
        
        Estrutura de diretórios esperada:
        - .../
          - data/
            - processed/
              - dados_b3.parquet
            - outputs/
              - dicionario_ativos.parquet
              - dicionario_codbdi.xlsx
              - dicionario_tpmerc.xlsx
        """
        print("Iniciando o Analisador B3Data...")
        self.base_path = Path(data_path)
        self.outputs_path = self.base_path.parent / 'outputs'
        self.full_data_path = self.base_path / 'dados_b3.parquet'
        
        if not self.full_data_path.exists():
            raise FileNotFoundError(f"Arquivo de dados principal não encontrado: {self.full_data_path}")

        try:
            # O dicionário de ativos é o "security master" do nosso sistema.
            # É pequeno o suficiente para ser carregado na memória.
            self.df_dicionario = pd.read_parquet(self.outputs_path / 'dicionario_ativos.parquet')
            print(f" -> Dicionário de {len(self.df_dicionario):,} ativos carregado.")
            
            # Mapeamentos para traduzir descrições amigáveis (ex: 'VISTA') para códigos numéricos.
            self.codbdi_map = pd.read_excel(self.outputs_path / 'dicionario_codbdi.xlsx').set_index('DESCRICAO_CODBDI')['CODBDI'].to_dict()
            self.tpmerc_map = pd.read_excel(self.outputs_path / 'dicionario_tpmerc.xlsx').set_index('DESCRICAO_TPMERC')['TPMERC'].to_dict()
            print(" -> Mapeamentos de CODBDI e TPMERC carregados.")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Erro ao carregar dicionário essencial: {e}. Execute o script de geração de dicionários.")
            
        print("Analisador B3Data pronto para uso.")

    def find_assets(self, query: str) -> pd.DataFrame:
        """
        Realiza uma busca universal por ativos no dicionário mestre.

        Este método é o principal ponto de entrada para a exploração de ativos.
        Ele procura por correspondências parciais no nome da empresa, em todos
        os tickers históricos e por correspondência exata no código ISIN.

        Args:
            query (str): O termo de busca. Pode ser um nome de empresa
                         (ex: 'Gerdau'), um ticker (ex: 'PETR4'), ou um
                         código ISIN (ex: 'BRGGBRACNPR8').

        Returns:
            pd.DataFrame: Um DataFrame contendo as informações cadastrais dos
                          ativos que correspondem à busca.
        """
        query_upper = query.upper()
        
        # Constrói máscaras booleanas para cada tipo de busca
        mask_ticker = self.df_dicionario['TICKERS_HISTORICOS'].str.contains(f"\\b{query_upper}\\b", regex=True, na=False)
        mask_name = self.df_dicionario['NOMES_HISTORICOS'].str.contains(query_upper, regex=False, na=False)
        mask_isin = self.df_dicionario['CODISI'] == query_upper
        
        # Combina as máscaras para encontrar qualquer correspondência
        combined_mask = mask_ticker | mask_name | mask_isin
        
        return self.df_dicionario[combined_mask].copy()

    def _build_parquet_filters(self, **kwargs) -> List[tuple]:
        """
        (Helper Interno) Constrói a lista de filtros de pré-leitura para o Parquet.

        Este método traduz os argumentos amigáveis do usuário em uma lista de tuplas
        no formato `(coluna, operador, valor)`, que é o formato exigido pelo
        `pd.read_parquet`. Apenas filtros seguros e universalmente suportados
        são construídos aqui. Filtros mais complexos (como por 'especificacao')
        são aplicados após a leitura.

        Returns:
            List[tuple]: Uma lista de filtros para o PyArrow.
        """
        filters = []
        
        # Camada de conveniência: traduz 'asset_class' para filtros de baixo nível
        asset_class = kwargs.get('asset_class')
        if asset_class:
            ac_lower = asset_class.lower()
            if ac_lower == 'equity': filters.extend([('CODBDI', 'in', [2, 96]), ('TPMERC', 'in', [10, 20])])
            elif ac_lower == 'fii': filters.append(('CODBDI', '==', 12))
            elif ac_lower == 'options': filters.extend([('TPMERC', 'in', [70, 80, 12, 13])])
            # BDR e 'especificacao' são tratados na pós-filtragem para máxima robustez.

        # Mapeia argumentos diretos para colunas e operadores
        filter_map = {
            'start_date': ('DATA_PREGAO', '>='), 'end_date': ('DATA_PREGAO', '<='),
            'vencimento_min': ('DATVEN', '>='), 'vencimento_max': ('DATVEN', '<='),
            'tickers': ('CODNEG', 'in'), 'codisi': ('CODISI', 'in'),
        }
        for arg, (col, op) in filter_map.items():
            value = kwargs.get(arg)
            if value is not None:
                if op == 'in' and not isinstance(value, list): value = [value]
                if arg == 'tickers': value = [str(v).upper() for v in value]
                if 'date' in arg or 'vencimento' in arg: value = pd.to_datetime(value)
                filters.append((col, op, value))
        
        # Traduz descrições de CODBDI e TPMERC para seus códigos numéricos
        for code_arg, code_map in [('codbdi', self.codbdi_map), ('tpmerc', self.tpmerc_map)]:
            value = kwargs.get(code_arg)
            if value is not None:
                if not isinstance(value, list): value = [value]
                codes_to_filter = [int(code_map.get(str(item).upper(), item)) for item in value]
                filters.append((code_arg.upper(), 'in', codes_to_filter))
                
        return filters

    def get_quotes(self, **kwargs) -> pd.DataFrame:
        """
        Busca cotações históricas com um sistema de filtros flexível e poderoso.

        Este é o método principal do analisador. Ele aceita uma variedade de
        argumentos para filtrar os dados, permitindo tanto buscas diretas
        (por ticker) quanto exploratórias (por entidade ou classe de ativo).

        Args:
            tickers (Union[str, List[str]], optional): Um ticker ou lista de tickers.
            entity (str, optional): Nome de uma empresa para buscar todos os seus ativos.
            asset_class (str, optional): Uma classe de ativo de alto nível.
                Opções: 'equity', 'fii', 'bdr', 'options'.
            ticker_root (str, optional): O radical de 4 letras de um ticker, usado
                principalmente para encontrar todas as opções de um ativo-objeto (ex: 'VALE').
            start_date (str, optional): Data de início no formato 'YYYY-MM-DD'.
            end_date (str, optional): Data de fim no formato 'YYYY-MM-DD'.
            columns (List[str], optional): Lista de colunas a serem retornadas.
                Se None, retorna um conjunto padrão.
            codbdi (Union[int, str, List], optional): Filtra pelo código BDI.
                Aceita código (ex: 12) ou descrição (ex: 'FUNDOS IMOBILIÁRIOS').
            tpmerc (Union[int, str, List], optional): Filtra pelo tipo de mercado.
                Aceita código (ex: 10) ou descrição (ex: 'VISTA').
            especificacao (Union[str, List[str]], optional): Filtra pela
                especificação do papel (ex: 'ON', 'PN').
            codisi (Union[str, List[str]], optional): Filtra pelo código ISIN.
            vencimento_min (str, optional): Data de vencimento mínima para derivativos.
            vencimento_max (str, optional): Data de vencimento máxima para derivativos.

        Returns:
            pd.DataFrame: Um DataFrame com os dados solicitados, ordenado por
                          ticker e data.
        """
        params = kwargs.copy()
        ticker_root_for_options = None
        
        # --- Camada de Inteligência: Lógica de Busca Exploratória ---
        entity_query = params.get('entity')
        if entity_query and params.get('asset_class') == 'options':
            # Caso especial: busca de opções por entidade. Requer lógica de radical.
            print(f"Modo de busca de opções para a entidade: '{entity_query}'...")
            asset_info = self.find_assets(entity_query)
            if not asset_info.empty:
                main_ticker = asset_info.iloc[0]['ULTIMO_TICKER']
                ticker_root_for_options = re.match("^[A-Z]{4}", main_ticker).group(0)
                print(f" -> Radical do ativo-objeto identificado: '{ticker_root_for_options}'")
                del params['entity'] # Evita que a busca por entidade gere um filtro de tickers
            else:
                return pd.DataFrame()
        elif entity_query and 'tickers' not in params:
            # Caso geral: busca por entidade para outras classes de ativos.
            asset_info = self.find_assets(entity_query)
            if not asset_info.empty:
                all_tickers_raw = asset_info['TICKERS_HISTORICOS'].str.split(' | ').explode().unique()
                # Filtra para manter apenas tickers com formato válido (ex: AAAA11)
                valid_tickers = [t for t in all_tickers_raw if t and re.match("^[A-Z]{4}[0-9]{1,2}$", t)]
                if params.get('asset_class') == 'equity':
                    # Refina a busca para tickers que são tipicamente ações/units.
                    params['tickers'] = [t for t in valid_tickers if t.endswith(('3', '4', '5', '6', '11'))]
                else:
                    params['tickers'] = valid_tickers
            else:
                return pd.DataFrame()
        
        # Validação para garantir que pelo menos um filtro principal foi fornecido
        valid_starters = ['tickers', 'entity', 'codisi', 'codbdi', 'tpmerc', 'asset_class', 'ticker_root']
        if not any(k in params for k in valid_starters):
             raise ValueError(f"Você deve fornecer um dos seguintes argumentos: {valid_starters}")
        
        # Constrói os filtros de pré-leitura
        filters = self._build_parquet_filters(**params)
        
        # Define quais colunas carregar
        user_columns = params.get('columns')
        key_columns = {'DATA_PREGAO', 'CODNEG', 'CODISI'}
        if user_columns is None:
            default_columns = {'PREABE', 'PREMAX', 'PREMIN', 'PREULT', 'VOLTOT', 'QUATOT'}
            columns_to_load = list(key_columns | default_columns)
        else:
            columns_to_load = list(key_columns | set(user_columns))
        
        # Garante que a coluna 'ESPECI' seja carregada se for necessária para a pós-filtragem
        if params.get('especificacao') or params.get('asset_class') == 'bdr' or params.get('ticker_root'):
            if 'ESPECI' not in columns_to_load:
                columns_to_load.append('ESPECI')
        
        try:
            # --- Leitura Otimizada do Parquet ---
            df = pd.read_parquet(self.full_data_path, columns=columns_to_load, filters=filters if filters else None)
            
            # --- Camada de Pós-Filtragem (para filtros complexos) ---
            if not df.empty:
                # Filtro por radical do ticker (usado para opções)
                ticker_root = params.get('ticker_root') or ticker_root_for_options
                if ticker_root:
                    df = df[df['CODNEG'].str.startswith(ticker_root.upper(), na=False)]
                
                # Filtro por classe de ativo 'bdr'
                asset_class_param = params.get('asset_class')
                if asset_class_param and asset_class_param.lower() == 'bdr':
                    df = df[df['ESPECI'].str.contains('DR', na=False, case=False)]
                
                # Filtro por 'especificacao'
                espec_value = params.get('especificacao')
                if espec_value is not None:
                    if not isinstance(espec_value, list): espec_value = [espec_value]
                    pattern = '|'.join(espec_value)
                    df = df[df['ESPECI'].str.contains(pattern, na=False, case=False, regex=True)]

            print(f" -> {len(df):,} registros carregados e filtrados.")
            if df.empty: return df
            return df.sort_values(by=['CODNEG', 'DATA_PREGAO']).reset_index(drop=True)
            
        except Exception as e:
            print(f"ERRO ao ler o arquivo Parquet ou ao filtrar: {e}")
            return pd.DataFrame()

    def list_tickers(self, asset_type: str = 'acoes') -> Optional[List[str]]:
        """Lista tickers únicos, baseado no dicionário de ativos."""
        if self.df_dicionario is None: return None
        print(f"Listando tickers para o tipo '{asset_type}' (baseado no dicionário)...")
        if asset_type == 'acoes':
            tickers = self.df_dicionario[
                self.df_dicionario['ULTIMA_ESPECIFICACAO'].str.contains('ACÕES', na=False)
            ]['ULTIMO_TICKER'].unique().tolist()
        else:
            tickers = self.df_dicionario['ULTIMO_TICKER'].unique().tolist()
        return sorted(tickers)

    def get_asset_info(self, ticker: str) -> Optional[pd.Series]:
        """Retorna informações cadastrais de um ativo, com busca robusta."""
        results = self.find_assets(ticker)
        if not results.empty:
            if len(results) > 1:
                print(f"AVISO: Múltiplas correspondências para '{ticker}'. Retornando a primeira.")
            return results.iloc[0]
        else:
            print(f"Ticker '{ticker}' não encontrado no dicionário.")
            return None