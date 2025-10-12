import pandas as pd
import numpy as np
import streamlit as st
import locale

# ==============================
# CONFIGURAÇÕES INICIAIS
# ==============================

# Configuração do locale para formatação de moeda em Português do Brasil
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except locale.Error:
        locale.setlocale(locale.LC_ALL, '')

FILE_CONEXAO = 'dados_conexao_unificada.csv'
FILE_PEDIDOS = 'dados_pedidos.csv'

# ==============================
# FUNÇÕES DE SUPORTE
# ==============================

def limpar_cnpj(cnpj_series):
    """Remove caracteres não-numéricos do CNPJ/CPF e trata valores ausentes (NaN)."""
    cnpj_series = cnpj_series.astype(str).fillna('')
    cnpj_series = cnpj_series.str.replace(r'\.0$', '', regex=True)
    return cnpj_series.str.replace(r'\D', '', regex=True)


@st.cache_data
def carregar_e_cruzar_dados():
    """Carrega, pré-processa e cruza os DataFrames por Cliente, Fornecedor e Filial (somente Faturamento/Devolução)."""

    try:
        # --- Carrega o arquivo de faturamento ---
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

        # --- Carrega o arquivo de pedidos ---
        df_pedidos = pd.read_csv(FILE_PEDIDOS, sep=';', decimal='.', encoding='utf-8')
        df_pedidos.columns = df_pedidos.columns.str.upper()

        df_pedidos = df_pedidos.rename(columns={
            'CLIENTE_NOME': 'CLIENTE_NOME',
            'CLIENTE_CNPJ': 'CLIENTE_CNPJ_BASE',
            'TOTAL_VALOR_PEDIDO': 'VALOR_PEDIDO',
            'TOTAL_PEDIDOS_QTD': 'PEDIDOS_QTD',
            'FORNECEDOR_NOME': 'FORNECEDOR_NOME_PEDIDO',
            'CODFILIAL': 'CODFILIAL_PEDIDO'
        }, errors='ignore')

    except FileNotFoundError:
        st.error(f"⚠️ Erro: Um ou ambos os arquivos ({FILE_CONEXAO}, {FILE_PEDIDOS}) não foram encontrados.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # --- Garantia das colunas essenciais ---
    if 'CODFILIAL_FATURAMENTO' not in df_conexao.columns and 'CODFILIAL' in df_conexao.columns:
        df_conexao['CODFILIAL_FATURAMENTO'] = df_conexao['CODFILIAL']
    elif 'CODFILIAL_FATURAMENTO' not in df_conexao.columns:
        st.warning("Coluna 'CODFILIAL' não encontrada no arquivo de faturamento. Usando 'FILIAL_UNICA'.")
        df_conexao['CODFILIAL_FATURAMENTO'] = 'FILIAL_UNICA'

    # --- Agregações ---
    df_conexao['CLIENTE_CNPJ_LIMPO'] = limpar_cnpj(df_conexao['CLIENTE_CNPJ_BASE'])
    df_pedidos['CLIENTE_CNPJ_LIMPO'] = limpar_cnpj(df_pedidos['CLIENTE_CNPJ_BASE'])

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

    # --- Agregação por fornecedor ---
    df_pedidos['FORNECEDOR_NOME_LIMPO'] = df_pedidos['FORNECEDOR_NOME_PEDIDO'].str.upper().str.strip()
    df_conexao['FORNECEDOR_NOME_LIMPO'] = df_conexao['FORNECEDOR_NOME_FATURADO'].str.upper().str.strip()

    df_pedidos_forn = df_pedidos.groupby('FORNECEDOR_NOME_LIMPO').agg(
        VALOR_PEDIDO_TOTAL=('VALOR_PEDIDO', 'sum')
    ).reset_index().rename(columns={'FORNECEDOR_NOME_LIMPO': 'FORNECEDOR'})

    df_conexao_forn = df_conexao.groupby('FORNECEDOR_NOME_LIMPO').agg(
        VALOR_FATURADO_TOTAL=('VALOR_FATURADO', 'sum'),
        VALOR_DEVOLVIDO_TOTAL=('VALOR_DEVOLVIDO', 'sum')
    ).reset_index().rename(columns={'FORNECEDOR_NOME_LIMPO': 'FORNECEDOR'})

    df_analise_fornecedor_agg = pd.merge(
        df_pedidos_forn, df_conexao_forn, on='FORNECEDOR', how='outer'
    ).fillna(0)

    df_analise_fornecedor_agg['DIFERENCA_FLUXO'] = (
        df_analise_fornecedor_agg['VALOR_PEDIDO_TOTAL'] - df_analise_fornecedor_agg['VALOR_FATURADO_TOTAL']
    )

    df_analise_fornecedor_agg = df_analise_fornecedor_agg.rename(columns={
        'VALOR_PEDIDO_TOTAL': 'VALOR_PEDIDO',
        'VALOR_FATURADO_TOTAL': 'VALOR_FATURADO',
        'VALOR_DEVOLVIDO_TOTAL': 'VALOR_DEVOLVIDO'
    })

    # --- Agregação por filial ---
    df_analise_filial = df_conexao.groupby('CODFILIAL_FATURAMENTO').agg(
        VALOR_FATURADO=('VALOR_FATURADO', 'sum'),
        VALOR_DEVOLVIDO=('VALOR_DEVOLVIDO', 'sum')
    ).reset_index().rename(columns={'CODFILIAL_FATURAMENTO': 'FILIAL'})

    df_analise_filial['VALOR_LIQUIDO_FATURADO'] = (
        df_analise_filial['VALOR_FATURADO'] - df_analise_filial['VALOR_DEVOLVIDO']
    )
    df_analise_filial['VALOR_PEDIDO'] = 0.0
    df_analise_filial['DIFERENCA_FLUXO'] = 0 - df_analise_filial['VALOR_FATURADO']

    return df_analise_cliente, df_analise_fornecedor_agg, df_analise_filial


def formatar_moeda(valor):
    """Formata um valor numérico para o formato de moeda R$ (pt-BR)."""
    try:
        return locale.currency(valor, grouping=True)
    except:
        return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


# ==============================
# INTERFACE STREAMLIT
# ==============================

def main():
    st.set_page_config(layout="wide")

    col_logo, col_titulo = st.columns([0.01, 0.10]) 
    with col_logo:
        st.image("logo_total.png", width=300)
    with col_titulo:
        st.markdown("## Análise de Resultados do Evento Conexão")

    # --- Carregamento de dados ---
    df_analise_cliente, df_analise_fornecedor, df_analise_filial = carregar_e_cruzar_dados()

    if df_analise_cliente.empty and df_analise_fornecedor.empty and df_analise_filial.empty:
        st.warning("Não há dados suficientes para análise. Verifique os arquivos CSV.")
        return

    # ==============================
    # 1. VISÃO GERAL
    # ==============================
    st.header("1. Visão Geral")

    total_pedido = df_analise_cliente['VALOR_PEDIDO'].sum()
    total_faturado = df_analise_cliente['VALOR_FATURADO'].sum()
    total_devolvido = df_analise_cliente['VALOR_DEVOLVIDO'].sum()
    total_diferenca = df_analise_cliente['DIFERENCA_FLUXO'].sum()

    TOTAL_CLIENTES_PRESENTES = 449
    dados_clientes_cargos = {
        "Acompanhante": 125,
        "Balconista": 14,
        "Comprador": 184,
        "Proprietário": 351
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
            st.metric("Total de Clientes", TOTAL_CLIENTES_PRESENTES)

    with main_kpi_tabela_col:
        st.subheader("Cargos - Cliente")
        st.table(dados_clientes_cargos)

    # ==============================
    # 2. TOP PERFORMANCE
    # ==============================
    st.header("2. Análise de Top Performance (Evento)")

    col_top_clientes, col_top_fornecedores = st.columns(2)

    # Top Clientes
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

    # Top Fornecedores
    with col_top_fornecedores:
        st.subheader("Fornecedores - Top 10 por Faturamento")
        df_top_fornecedores = df_analise_fornecedor[df_analise_fornecedor['VALOR_FATURADO'] > 0] \
            .sort_values(by='VALOR_FATURADO', ascending=False).head(10).copy()

        if not df_top_fornecedores.empty:
            df_top_fornecedores_display = df_top_fornecedores[['FORNECEDOR', 'VALOR_FATURADO']].copy()
            df_top_fornecedores_display.columns = ['Fornecedor', 'Valor Faturado']
            df_top_fornecedores_display['Valor Faturado'] = df_top_fornecedores_display['Valor Faturado'].apply(formatar_moeda)
            st.dataframe(df_top_fornecedores_display, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum fornecedor com Faturamento positivo encontrado.")

    # ==============================
    # 3. ANÁLISE POR FILIAL
    # ==============================
    st.header("3. Análise de Desempenho por Filial")
    st.markdown("Mostrando apenas Faturamento, Devolução e Receita Líquida por Filial.")

    if not df_analise_filial.empty:
        df_display_filial = df_analise_filial.sort_values(by='VALOR_LIQUIDO_FATURADO', ascending=False).copy()
        cols_to_display_filial = ['FILIAL', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'VALOR_LIQUIDO_FATURADO']
        for col in cols_to_display_filial[1:]:
            df_display_filial[col] = df_display_filial[col].apply(formatar_moeda)
        st.dataframe(df_display_filial[cols_to_display_filial], use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma filial encontrada para análise.")

    # ==============================
    # 4. ANÁLISE POR CLIENTE
    # ==============================
    st.header("4. Tabela Detalhada por Cliente")
    st.markdown("Clientes com maior potencial de venda a ser faturado ou maior faturamento excedente.")

    if not df_analise_cliente.empty:
        df_display_cliente = df_analise_cliente.sort_values(by='DIFERENCA_FLUXO', ascending=False).copy()
        cols = ['CLIENTE', 'CLIENTE_CNPJ_LIMPO', 'VALOR_PEDIDO', 'VALOR_FATURADO',
                'VALOR_DEVOLVIDO', 'DIFERENCA_FLUXO', 'VALOR_LIQUIDO_FATURADO']
        for col in cols[2:]:
            df_display_cliente[col] = df_display_cliente[col].apply(formatar_moeda)
        st.dataframe(df_display_cliente[cols], use_container_width=True)
    else:
        st.info("Nenhum cliente encontrado.")

    # ==============================
    # 5. ANÁLISE POR FORNECEDOR
    # ==============================
    st.header("5. Tabela Detalhada por Fornecedor")
    st.markdown("Fornecedores com maior potencial de faturamento ou maior volume já faturado.")

    if not df_analise_fornecedor.empty:
        df_display_fornecedor = df_analise_fornecedor.sort_values(by='DIFERENCA_FLUXO', ascending=False).copy()
        cols = ['FORNECEDOR', 'VALOR_PEDIDO', 'VALOR_FATURADO', 'VALOR_DEVOLVIDO', 'DIFERENCA_FLUXO']
        for col in cols[1:]:
            df_display_fornecedor[col] = df_display_fornecedor[col].apply(formatar_moeda)
        st.dataframe(df_display_fornecedor[cols], use_container_width=True)
    else:
        st.info("Nenhum fornecedor encontrado.")


if __name__ == "__main__":
    main()
