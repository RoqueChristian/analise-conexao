import pandas as pd
import numpy as np
import streamlit as st
import locale

# ==============================
# CONFIGURAÇÕES INICIAIS
# ==============================

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except locale.Error:
        locale.setlocale(locale.LC_ALL, '')

FILE_CONEXAO = 'dados_conexao_unificada.csv'
FILE_PEDIDOS = 'dados_pedidos.csv'
COLUNA_ESTADO = 'ESTADO' 

# ==============================
# FUNÇÕES DE SUPORTE
# ==============================

def limpar_cnpj(cnpj_series):

    cnpj_series = cnpj_series.astype(str).fillna('')
    cnpj_series = cnpj_series.str.replace(r'\.0$', '', regex=True)
    return cnpj_series.str.replace(r'\D', '', regex=True)


@st.cache_data
def carregar_dados_brutos():
    df_empty = pd.DataFrame()
    
    try:

        df_conexao = pd.read_csv(FILE_CONEXAO, sep=';', decimal=',', encoding='utf-8-sig')
        df_conexao.columns = df_conexao.columns.str.upper()

        df_conexao = df_conexao.rename(columns={
            'CLIENTE': 'CLIENTE_NOME_FATURADO',
            'CNPJ_CLIENTE': 'CLIENTE_CNPJ_BASE',
            'TOTAL_FATURADO': 'VALOR_FATURADO',
            'VALOR_DEVOLVIDO': 'VALOR_DEVOLVIDO',
            'FORNECEDOR': 'FORNECEDOR_NOME_FATURADO',
            'CNPJ_FORNECEDOR': 'FORNECEDOR_CNPJ_FATURADO', 
            'CODFILIAL': 'CODFILIAL_FATURAMENTO'
        }, errors='ignore')


        df_pedidos = pd.read_csv(FILE_PEDIDOS, sep=';', decimal='.', encoding='utf-8')
        df_pedidos.columns = df_pedidos.columns.str.upper()

        df_pedidos = df_pedidos.rename(columns={
            'CLIENTE_NOME': 'CLIENTE_NOME',
            'CLIENTE_CNPJ': 'CLIENTE_CNPJ_BASE',
            'TOTAL_VALOR_PEDIDO': 'VALOR_PEDIDO',
            'TOTAL_PEDIDOS_QTD': 'PEDIDOS_QTD',
            'FORNECEDOR_NOME': 'FORNECEDOR_NOME_PEDIDO',
            'FORNECEDOR_CNPJ': 'FORNECEDOR_CNPJ_PEDIDO', 
            'CODFILIAL': 'CODFILIAL_PEDIDO'
        }, errors='ignore')

    except FileNotFoundError:
        st.error(f"⚠️ Erro: Um ou ambos os arquivos ({FILE_CONEXAO}, {FILE_PEDIDOS}) não foram encontrados.")
        return df_empty, df_empty


    

    df_conexao['CLIENTE_CNPJ_LIMPO'] = limpar_cnpj(df_conexao['CLIENTE_CNPJ_BASE'])
    df_pedidos['CLIENTE_CNPJ_LIMPO'] = limpar_cnpj(df_pedidos['CLIENTE_CNPJ_BASE'])

    df_conexao['FORNECEDOR_CNPJ_LIMPO'] = limpar_cnpj(
        df_conexao.get('FORNECEDOR_CNPJ_FATURADO', df_conexao.get('FORNECEDOR_NOME_FATURADO'))
    )
    df_pedidos['FORNECEDOR_CNPJ_LIMPO'] = limpar_cnpj(
        df_pedidos.get('FORNECEDOR_CNPJ_PEDIDO', df_pedidos.get('FORNECEDOR_NOME_PEDIDO'))
    )
    

    if 'CODFILIAL_FATURAMENTO' not in df_conexao.columns and 'CODFILIAL' in df_conexao.columns:
        df_conexao['CODFILIAL_FATURAMENTO'] = df_conexao['CODFILIAL']
    elif 'CODFILIAL_FATURAMENTO' not in df_conexao.columns:
        df_conexao['CODFILIAL_FATURAMENTO'] = 'FILIAL_UNICA'
    
    return df_conexao, df_pedidos


def calcular_metricas_agregadas(df_conexao: pd.DataFrame, df_pedidos: pd.DataFrame, coluna_estado: str = 'ESTADO'):

    
    df_empty = pd.DataFrame()
    if df_conexao.empty and df_pedidos.empty:
        return df_empty, df_empty, df_empty, df_empty


    df_conexao_agg_cliente = df_conexao.groupby('CLIENTE_CNPJ_LIMPO').agg({
        'CLIENTE_NOME_FATURADO': 'first',
        'VALOR_FATURADO': 'sum',
        'VALOR_DEVOLVIDO': 'sum'
    }).reset_index()

    df_pedidos_agg_cliente = df_pedidos.groupby('CLIENTE_CNPJ_LIMPO').agg({
        'CLIENTE_NOME': 'first',
        'VALOR_PEDIDO': 'sum'
    }).reset_index()

    df_merged = pd.merge(df_pedidos_agg_cliente, df_conexao_agg_cliente, on='CLIENTE_CNPJ_LIMPO', how='outer')
    df_merged['VALOR_PEDIDO'] = df_merged['VALOR_PEDIDO'].fillna(0)
    df_merged['VALOR_FATURADO'] = df_merged['VALOR_FATURADO'].fillna(0)
    df_merged['VALOR_DEVOLVIDO'] = df_merged['VALOR_DEVOLVIDO'].fillna(0)
    df_merged['CLIENTE'] = df_merged['CLIENTE_NOME'].fillna(df_merged['CLIENTE_NOME_FATURADO'])
    df_merged['DIFERENCA_FLUXO'] = df_merged['VALOR_PEDIDO'] - df_merged['VALOR_FATURADO']
    df_merged['VALOR_LIQUIDO_FATURADO'] = df_merged['VALOR_FATURADO'] - df_merged['VALOR_DEVOLVIDO']
    df_analise_cliente = df_merged.copy()

    # --- Análise por Fornecedor ---
    df_pedidos_forn = df_pedidos.groupby('FORNECEDOR_CNPJ_LIMPO').agg(
        FORNECEDOR_NOME=('FORNECEDOR_NOME_PEDIDO', 'first'),
        VALOR_PEDIDO_TOTAL=('VALOR_PEDIDO', 'sum')
    ).reset_index().rename(columns={'FORNECEDOR_CNPJ_LIMPO': 'FORNECEDOR_CHAVE'})

    df_conexao_forn = df_conexao.groupby('FORNECEDOR_CNPJ_LIMPO').agg(
        FORNECEDOR_NOME=('FORNECEDOR_NOME_FATURADO', 'first'), 
        VALOR_FATURADO_TOTAL=('VALOR_FATURADO', 'sum'),
        VALOR_DEVOLVIDO_TOTAL=('VALOR_DEVOLVIDO', 'sum')
    ).reset_index().rename(columns={'FORNECEDOR_CNPJ_LIMPO': 'FORNECEDOR_CHAVE'})

    df_analise_fornecedor_agg = pd.merge(
        df_pedidos_forn, df_conexao_forn, on='FORNECEDOR_CHAVE', how='outer', suffixes=('_PEDIDO', '_FATURADO')
    ).fillna(0)
    
    df_analise_fornecedor_agg['FORNECEDOR'] = df_analise_fornecedor_agg['FORNECEDOR_NOME_PEDIDO'].replace(0, np.nan).fillna(df_analise_fornecedor_agg['FORNECEDOR_NOME_FATURADO'])
    df_analise_fornecedor_agg['FORNECEDOR'] = df_analise_fornecedor_agg['FORNECEDOR'].str.upper().str.strip()
    df_analise_fornecedor_agg['DIFERENCA_FLUXO'] = (
        df_analise_fornecedor_agg['VALOR_PEDIDO_TOTAL'] - df_analise_fornecedor_agg['VALOR_FATURADO_TOTAL']
    )
    df_analise_fornecedor = df_analise_fornecedor_agg.rename(columns={
        'VALOR_PEDIDO_TOTAL': 'VALOR_PEDIDO',
        'VALOR_FATURADO_TOTAL': 'VALOR_FATURADO',
        'VALOR_DEVOLVIDO_TOTAL': 'VALOR_DEVOLVIDO'
    })
    df_analise_fornecedor['FORNECEDOR_CNPJ_LIMPO'] = df_analise_fornecedor['FORNECEDOR_CHAVE']
    df_analise_fornecedor = df_analise_fornecedor[['FORNECEDOR', 'FORNECEDOR_CNPJ_LIMPO', 'VALOR_PEDIDO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'DIFERENCA_FLUXO']].copy()
    
    # --- Análise por Filial ---
    df_analise_filial = df_conexao.groupby('CODFILIAL_FATURAMENTO').agg(
        VALOR_FATURADO=('VALOR_FATURADO', 'sum'),
        VALOR_DEVOLVIDO=('VALOR_DEVOLVIDO', 'sum')
    ).reset_index().rename(columns={'CODFILIAL_FATURAMENTO': 'FILIAL'})

    df_analise_filial['VALOR_LIQUIDO_FATURADO'] = (
        df_analise_filial['VALOR_FATURADO'] - df_analise_filial['VALOR_DEVOLVIDO']
    )
    df_analise_filial['VALOR_PEDIDO'] = 0.0
    df_analise_filial['DIFERENCA_FLUXO'] = 0 - df_analise_filial['VALOR_FATURADO']
    
    # --- Análise por Estado ---
    if coluna_estado in df_conexao.columns:
        df_analise_estado = df_conexao.groupby(coluna_estado).agg(
            VALOR_FATURADO=('VALOR_FATURADO', 'sum'),
            VALOR_DEVOLVIDO=('VALOR_DEVOLVIDO', 'sum')
        ).reset_index().rename(columns={coluna_estado: 'ESTADO'})

        df_analise_estado['VALOR_LIQUIDO_FATURADO'] = (
            df_analise_estado['VALOR_FATURADO'] - df_analise_estado['VALOR_DEVOLVIDO']
        )
        df_analise_estado['VALOR_PEDIDO'] = 0.0
        df_analise_estado['DIFERENCA_FLUXO'] = 0 - df_analise_estado['VALOR_FATURADO']
    else:
        df_analise_estado = pd.DataFrame(columns=['ESTADO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'VALOR_LIQUIDO_FATURADO', 'VALOR_PEDIDO', 'DIFERENCA_FLUXO'])
    
    return df_analise_cliente, df_analise_fornecedor, df_analise_filial, df_analise_estado


def formatar_moeda(valor):
    try:
        return locale.currency(valor, grouping=True)
    except:
        return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


# ==============================
# INTERFACE STREAMLIT
# ==============================

def main():
    st.set_page_config(layout="wide")

    col_logo, col_titulo = st.columns([0.1, 0.9]) 
    with col_logo:
        st.image("logo_total.png", width=300) 
    with col_titulo:
        st.markdown("## Análise de Resultados do Evento Conexão")


    df_conexao_bruto, df_pedidos_bruto = carregar_dados_brutos()

    if df_conexao_bruto.empty:
        st.warning("Não há dados suficientes para análise. Verifique os arquivos CSV.")
        return


    
    df_conexao_filtrado = df_conexao_bruto.copy()
    df_pedidos_filtrado = df_pedidos_bruto.copy()
    estado_selecionado = 'Todos'
    
    if COLUNA_ESTADO in df_conexao_bruto.columns:
        lista_estados = ['Todos'] + sorted(df_conexao_bruto[COLUNA_ESTADO].unique().tolist())
        
        with st.sidebar:
             st.header("Filtros")
             estado_selecionado = st.selectbox(
                 'Selecione o Estado/UF:',
                 lista_estados
             )
             
        if estado_selecionado != 'Todos':
            df_conexao_filtrado = df_conexao_bruto[df_conexao_bruto[COLUNA_ESTADO] == estado_selecionado].copy()
            

            clientes_filtrados = df_conexao_filtrado['CLIENTE_CNPJ_LIMPO'].unique()
            df_pedidos_filtrado = df_pedidos_bruto[df_pedidos_bruto['CLIENTE_CNPJ_LIMPO'].isin(clientes_filtrados)].copy()
            
    # ==============================
    # CÁLCULO DAS MÉTRICAS 
    # ==============================
    df_analise_cliente, df_analise_fornecedor, df_analise_filial, df_analise_estado = \
        calcular_metricas_agregadas(df_conexao_filtrado, df_pedidos_filtrado, COLUNA_ESTADO)
    
    if df_analise_cliente.empty:
        st.info(f"Nenhum dado encontrado para o Estado: **{estado_selecionado}**.")
        return

    # ==============================
    # 1. VISÃO GERAL
    # ==============================
    if estado_selecionado == 'Todos':
        st.header(f"1. Visão Geral")
    else:
        st.header(f"1. Visão Geral: {estado_selecionado}")

    total_pedido = df_analise_cliente['VALOR_PEDIDO'].sum()
    total_faturado = df_analise_cliente['VALOR_FATURADO'].sum()
    total_devolvido = df_analise_cliente['VALOR_DEVOLVIDO'].sum()
    total_diferenca = df_analise_cliente['DIFERENCA_FLUXO'].sum()

    TOTAL_CLIENTES_PRESENTES = 500
    dados_clientes_cargos = {
        "Acompanhante": 229,
        "Balconista": 23,
        "Comprador": 337,
        "Proprietário": 641,
        "Outros": 397
    }

    main_kpi_col, main_kpi_tabela_col = st.columns([2, 1])

    with main_kpi_col:
        col_pedido, col_faturado, col_devolvido = st.columns(3)

        with col_pedido:
            st.metric("Valor Total de Pedidos", formatar_moeda(total_pedido))
        with col_faturado:
            st.metric("Valor Total Faturado", formatar_moeda(total_faturado))
        with col_devolvido:
            st.metric("Valor Total Devolvido", formatar_moeda(total_devolvido))

        st.markdown("##")

        col_diferenca, col_clientes = st.columns(2)
        with col_diferenca:
            delta_color = "normal" if total_diferenca < 0 else "inverse"
            st.metric(
                "Diferença (Pedido - Faturado)",
                formatar_moeda(total_diferenca),
                delta="Potencial ou Divergência Total",
                delta_color=delta_color
            )
        with col_clientes:
            
            st.metric("Clientes Únicos", TOTAL_CLIENTES_PRESENTES)

    with main_kpi_tabela_col:
        st.subheader("Cargos - Cliente")
        st.table(dados_clientes_cargos)

    # ==============================
    # 2. TOP PERFORMANCE
    # ==============================
    st.header("2. Análise de Top Performance (Evento)")

    col_top_clientes, col_top_fornecedores = st.columns(2)


    with col_top_clientes:
        st.subheader("Clientes - Top 10 por Receita Líquida")
        df_top_clientes = df_analise_cliente[df_analise_cliente['VALOR_LIQUIDO_FATURADO'] > 0] \
            .sort_values(by='VALOR_LIQUIDO_FATURADO', ascending=False).head(10).copy()

        if not df_top_clientes.empty:
            df_top_clientes_display = df_top_clientes[['CLIENTE', 'VALOR_LIQUIDO_FATURADO']].copy()
            df_top_clientes_display.columns = ['Cliente', 'Receita Líquida']
            df_top_clientes_display['Receita Líquida'] = df_top_clientes_display['Receita Líquida'].apply(formatar_moeda)
            st.dataframe(df_top_clientes_display, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum cliente com Receita Líquida positiva encontrado.")


    with col_top_fornecedores:
        st.subheader("Fornecedores - Top 10 por Faturamento")
        df_top_fornecedores = df_analise_fornecedor[df_analise_fornecedor['VALOR_FATURADO'] > 0] \
            .sort_values(by='VALOR_FATURADO', ascending=False).head(10).copy()

        if not df_top_fornecedores.empty:
            df_top_fornecedores_display = df_top_fornecedores[['FORNECEDOR', 'FORNECEDOR_CNPJ_LIMPO', 'VALOR_FATURADO']].copy()
            df_top_fornecedores_display.columns = ['Fornecedor', 'CNPJ', 'Valor Faturado']
            df_top_fornecedores_display['Valor Faturado'] = df_top_fornecedores_display['Valor Faturado'].apply(formatar_moeda)
            st.dataframe(df_top_fornecedores_display, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum fornecedor com Faturamento positivo encontrado.")

    # ==============================
    # 3. ANÁLISE POR FILIAL 
    # ==============================
    st.header("3. Análise de Desempenho por Filial")

    if not df_analise_filial.empty:
        df_display_filial = df_analise_filial.sort_values(by='VALOR_LIQUIDO_FATURADO', ascending=False).copy()
        cols_to_display_filial = ['FILIAL', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'VALOR_LIQUIDO_FATURADO']
        for col in cols_to_display_filial[1:]:
            df_display_filial[col] = df_display_filial[col].apply(formatar_moeda)
        st.dataframe(df_display_filial[cols_to_display_filial], use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma filial encontrada para análise.")

    #===============================
    # 4. ANÁLISE POR ESTADO 
    #===============================

    st.header('4. Análise por Estado')

    if not df_analise_estado.empty:
        df_display_estado = df_analise_estado.sort_values(by='VALOR_LIQUIDO_FATURADO', ascending=False).copy()
        cols_to_display_estado = ['ESTADO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'VALOR_LIQUIDO_FATURADO']
        for col in cols_to_display_estado[1:]:
            df_display_estado[col] = df_display_estado[col].apply(formatar_moeda)
        st.dataframe(df_display_estado[cols_to_display_estado], use_container_width=True, hide_index=True)
    else:
        st.info('Nenhuma Estado encontrado para análise.')

    # ==============================
    # 5. TABELA DETALHADA POR CLIENTE
    # ==============================
    st.header("5. Tabela Detalhada por Cliente")

    if not df_analise_cliente.empty:
        df_display_cliente = df_analise_cliente.sort_values(by='DIFERENCA_FLUXO', ascending=False).copy()
        cols = ['CLIENTE', 'CLIENTE_CNPJ_LIMPO', 'VALOR_PEDIDO', 'VALOR_FATURADO',
                'VALOR_DEVOLVIDO', 'DIFERENCA_FLUXO', 'VALOR_LIQUIDO_FATURADO']
        for col in cols[2:]:
            df_display_cliente[col] = df_display_cliente[col].apply(formatar_moeda)
        
        display_cols = dict(zip(cols, ['Cliente', 'CNPJ', 'Valor Pedido', 'Valor Faturado', 'Valor Devolvido', 'Diferença Fluxo', 'Receita Líquida']))
        df_display_cliente_final = df_display_cliente[cols].rename(columns=display_cols)
        st.dataframe(df_display_cliente_final, use_container_width=True)
    else:
        st.info("Nenhum cliente encontrado.")

    # ==============================
    # 6. TABELA DETALHADA POR FORNECEDOR 
    # ==============================
    st.header("6. Tabela Detalhada por Fornecedor")

    if not df_analise_fornecedor.empty:
        df_display_fornecedor = df_analise_fornecedor.sort_values(by='DIFERENCA_FLUXO', ascending=False).copy()
        cols = ['FORNECEDOR', 'FORNECEDOR_CNPJ_LIMPO', 'VALOR_PEDIDO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'DIFERENCA_FLUXO']
        for col in cols[2:]:
            df_display_fornecedor[col] = df_display_fornecedor[col].apply(formatar_moeda)
        
        display_cols_forn = dict(zip(cols, ['Fornecedor', 'CNPJ', 'Valor Pedido', 'Valor Faturado', 'Valor Devolvido', 'Diferença Fluxo']))
        df_display_fornecedor_final = df_display_fornecedor[cols].rename(columns=display_cols_forn)
        st.dataframe(df_display_fornecedor_final, use_container_width=True)
    else:
        st.info("Nenhum fornecedor encontrado.")

    # ==============================
    # 7. CONEXÃO 2025 - CLIENTES FATURADOS
    # ==============================
    st.header("7. Conexão 2025 - Clientes Faturados")

    arquivo_novo = 'conexao_2025_clientes fat.xlsx'

    try:
        df_novo = pd.read_excel(arquivo_novo)
        colunas_desejadas = ['COD', 'RAZÃO', 'TOTAL_GASTO', 'TOTAL FATURADO']

        # Verifica se as colunas existem na planilha antes de exibir
        if set(colunas_desejadas).issubset(df_novo.columns):
            df_exibicao = df_novo[colunas_desejadas].copy()
            
            for col in ['TOTAL_GASTO', 'TOTAL FATURADO']:
                df_exibicao[col] = df_exibicao[col].apply(formatar_moeda)

            st.dataframe(df_exibicao, use_container_width=True, hide_index=True)
        else:
            st.warning(f"As colunas {colunas_desejadas} não foram encontradas no arquivo.")
            st.write("Colunas disponíveis:", list(df_novo.columns))

    except FileNotFoundError:
        st.info(f"Arquivo '{arquivo_novo}' não encontrado. Adicione-o à pasta para visualizar.")
    except Exception as e:
        st.error(f"Erro ao ler o arquivo '{arquivo_novo}': {e}")


if __name__ == "__main__":
    main()