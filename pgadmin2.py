import psycopg2
import csv
from dotenv import load_dotenv
import os 
# Corrigido: As importações de dotenv e os agora estão separadas corretamente

# 1. CARREGA AS VARIÁVEIS DE AMBIENTE (do arquivo .env)
load_dotenv()

# 2. DEFINIÇÃO DA CONSULTA SQL
sql_d_conexao = """
    SELECT
        f.nome_fantasia AS fornecedor_nome,
        f.id AS fornecedor_cnpj,
        c.nome_fantasia AS cliente_nome,
        c.cod_cli_princ AS cliente_cnpj,
        c.estado as estado,

        SUM(p.valor_total) AS total_valor_pedido,
        COUNT(p.num_ped) AS total_pedidos_qtd

    FROM 
        pedido p
    INNER JOIN 
        fornecedor f ON p.fornecedor_id = f.id
    INNER JOIN 
        cliente c ON c.id = p.cliente_id
    GROUP BY
        f.nome_fantasia,
        f.id,
        c.nome_fantasia,
        c.cod_cli_princ,
        c.estado
"""

csv_pedidos_compra = 'dados_pedidos.csv'

# 3. LEITURA E CONFIGURAÇÃO DA CONEXÃO DO POSTGRES
# Estas variáveis foram lidas após o load_dotenv() e antes da conexão
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Definição do Dicionário de Configuração (DB_CONFIG corrigido)
DB_CONFIG = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "host": POSTGRES_HOST,
    "database": POSTGRES_DB
}

conn = None

def exportar_para_csv(cursor, sql_query, csv_filename):
    """Executa uma consulta SQL e salva os resultados em um arquivo CSV."""
    try:
        # 1. Executa a consulta
        cursor.execute(sql_query)
        records = cursor.fetchall()
        
        # Se não houver registros, imprime uma mensagem e retorna
        if not records and cursor.description is None:
            print(f"Aviso: A consulta para '{csv_filename}' não retornou resultados ou estrutura.")
            return

        # 2. Pega os nomes das colunas
        # Usamos o MAX para evitar um erro caso o cursor não tenha descrição
        column_names = [desc[0] for desc in cursor.description]

        # 3. Escreve no arquivo CSV
        print(f"Escrevendo {len(records)} registro(s) no arquivo '{csv_filename}'...")
        # Usa 'w' para sobrescrever, 'newline='' para evitar linhas em branco no Windows
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Usa ';' como delimitador
            csv_writer = csv.writer(csvfile, delimiter=';') 
            
            # Escreve o cabeçalho
            csv_writer.writerow(column_names)
            
            # Escreve os dados
            csv_writer.writerows(records)

        print(f"Sucesso! Dados exportados para '{csv_filename}'.")

    except Exception as e:
        print(f"Ocorreu um erro ao exportar para '{csv_filename}': {e}")
        # Re-lança o erro para ser capturado no bloco principal, se necessário
        raise

try:
    # 1. Estabelece a conexão usando o DB_CONFIG
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Conexão com o banco de dados estabelecida.")
    print("-" * 30)
    
    # 2. Exporta os resultados das consultas
    exportar_para_csv(cursor, sql_d_conexao, csv_pedidos_compra)
    print("-" * 30)


except psycopg2.OperationalError as e:
    print(f"\n--- Erro Crítico ---")
    print(f"Erro de Conexão. Verifique as credenciais ou se o PostgreSQL está rodando: {e}")
    print(f"--------------------")
except Exception as e:
    print(f"\n--- Erro Crítico ---")
    print(f"Ocorreu um erro geral: {e}")
    print(f"--------------------")

finally:
    # 3. Fecha a conexão
    if 'cursor' in locals() and cursor:
        cursor.close()
    if conn:
        conn.close()
        print("Conexão com o banco de dados fechada.")