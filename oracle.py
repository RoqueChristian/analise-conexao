import pandas as pd
import oracledb
from dotenv import load_dotenv
import os

try:
    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_9")
except Exception as e:
    print(f"Aviso: Não foi possível inicializar o cliente Oracle: {e}")


load_dotenv()
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
dsn = '192.168.0.1/WINT'

# --- Consultas SQL  ---

# valor dos pedidos por cliente (ol)
sql_conexao_clientes_ol = """
    WITH fornecedor_pedido AS (
        SELECT
            m.numped,
            m.codfornec as cgc_fornecedor,
            f.fornecedor as nome_fornecedor
        FROM pcmov m
        LEFT JOIN pcfornec f ON f.codfornec = m.codfornec
        WHERE m.numped <> 0 AND m.codfornec <> 0
        GROUP BY m.numped, m.codfornec, f.fornecedor
    )
    SELECT
        p.codfilial,
        p.numped,
        fp.cgc_fornecedor AS cnpj_fornecedor,
        fp.nome_fornecedor AS fornecedor,
        c.codcliprinc as cnpj_cliente,
        c.cliente,
        c.estent as estado,
        round(p.vlatend, 2) as total_faturado,
        round(d.valor_devolvido, 2) as valor_devolvido
    FROM pcpedc p
    LEFT JOIN pcclient c ON c.codcli = p.codcli
    LEFT JOIN fornecedor_pedido fp ON fp.numped = p.numped
    LEFT JOIN (
        SELECT
            m.numped,
            SUM(m.punit * qt) AS valor_devolvido
        FROM pcmov m
        WHERE m.codoper = 'ED' AND m.numped <> 0 AND m.codfornec <> 0
        GROUP BY m.numped
    ) d ON d.numped = p.numped
    WHERE p.data BETWEEN TO_DATE('12/04/2025', 'DD/MM/YYYY') AND TO_DATE('21/04/2025', 'DD/MM/YYYY')
    AND p.posicao IN ('F')
    AND p.tipofv IN ( 'OL')
    AND p.origemped IN ('F')
    AND p.codemitente IN (8888)
    ORDER BY p.numped
"""
# valor dos pedidos por cliente (pela condição da promoção)
sql_conexao_clientes_condicao = """
    WITH fornecedor_pedido AS (
        SELECT
            m.numped,
            m.codfornec as cgc_fornecedor,
            f.fornecedor as nome_fornecedor
        FROM pcmov m
        LEFT JOIN pcfornec f ON f.codfornec = m.codfornec
        WHERE m.numped <> 0 AND m.codfornec <> 0
        GROUP BY m.numped, m.codfornec, f.fornecedor
    )
    SELECT
        p.codfilial,
        p.numped,
        fp.cgc_fornecedor AS cnpj_fornecedor,
        fp.nome_fornecedor AS fornecedor,
        c.codcliprinc as cnpj_cliente,
        c.cliente,
        c.estent as estado,
        round(p.vlatend, 2) as total_faturado,
        round(d.valor_devolvido, 2) as valor_devolvido
    FROM pcpedc p
    LEFT JOIN pcclient c ON c.codcli = p.codcli
    LEFT JOIN fornecedor_pedido fp ON fp.numped = p.numped
    LEFT JOIN (
        SELECT
            m.numped,
            SUM(m.punit * qt) AS valor_devolvido
        FROM pcmov m
        WHERE m.codoper = 'ED' AND m.numped <> 0 AND m.codfornec <> 0
        GROUP BY m.numped
    ) d ON d.numped = p.numped
    where
        p.posicao IN ('F')
        and p.codpromocaomed in (126106,125480,125703,125726,125748,125744,129930,125936,
        125928,125701,125669,125531,125401,125927,125416,125717,125716,125916,125733,125742,
        125516,125585,125520,125517,125525,125524,125505,125503,125732,125731,126263,125591,
        125576,125575,126535,125760,125358,125363,125364,125365,125366,125375,125377,125379,
        125380,125381,125382,125384,125387,125389,125411,125682,125408,125386,125485,126283,125519,
        125413,125801,125800,125518,125528,125477,125984,126217,126215,125462,125816,125938,125799,
        125450,126829,125784,125770,125771,125808,126809,125779,125773,125807,125782,126536,126241,125840,
        125967,125918,125588,125590,125592,125580,125583,125577,125586,125584,125581,125587,126001,125833,
        125934,125968,125743,125973,125713,125444,127023,127025,125777,125774,125976,125522,126161,125506,
        125508,125718,126191,125529,125908,125513,125367,125368,125369,125370,125371,125394,125396,125398,
        125399,125400,125402,125403,125405,125407,125940,125825,125872,125851,126141,125393,125785,126049,
        126680,126681,126013,127001,125917,126925,126575,125820,125942,126659,126664,126993,125515,125746,
        125730,126596,125998,126002,126052,125650,125921,126497,125641,126220,125993,126119,125999,126016,
        125980,126518,126519,125794,125870,125836,125786,126133,125787,125790,125792,125849,125793,125791,
        125795,125796,125797,125788,125798,126169,125910,125417,126521,126573,126574,125455,125530,125376,
        125372,125868,126276,125864,125395,126603,126538,125666,125668,126602,126258,125498,125412,125409,
        126599,126050,125467,126103,125494,126091,126092,126101,126076,126102,126099,126098,125493,126100,
        125490,125491,126085,126086,126088,126087,125486,125484,125487,126389,126081,126083,126578,126579,
        126525,125436,126075,125642,125634,125499,126313,125463,126418,125558,125482,125709,125710,125711,
        125689,125712,125811,125694,125697,125737,125478,125479,125404,125419,125492,125420,125500,125383,
        125941,125970,125414,125535,125406,125648,125649,125754,125753,125752,125656,125657,125751,125750,
        125905,125823,125909,125911,125740,125727,125725,125724,125722,125721,125523,125391,125390,125680,
        125678,126148,125736,125734,126115,125527,125739,125442,125447,125448,125451,125452,125458,125955,
        125966,125719,125723,125958,125839,125861,125512,125738,125871,126041,125526,125766,125767,125768,
        125772,125776,125778,125422,125423,125424,125728,125729,125356,125639,125735,125637,125636,125633,
        125630,125429,125426,125427,125428,125900,125901,125902,125904,125745,125741,125430,125431,125432,
        125433,125437,125438,125440,125441,125804,125803,125802,125805,125434,125435,125822,125439,125824,
        125826,125443,125445,125449,125453,125456,126244,125385,125761,125764,125749,125747,125652,125653,
        125651,125755,126200,125827,125828,125829,125830,125831,125832,125835,125617,125626,125627,125629,
        126307,126314,125853,125854,125855,125856,125567,125862,125857,125863,125865,125866,126284,126286,
        125425,126054,125552,125972,125548,125971,126310,126308,125708,126337,126082,125931,126479,126495,
        125817,126260,126170,126287,125935,126255,126172,125533,125532,125534,125536,125537,125538,125539,
        125547,125553,125555,125560,125562,125565,125568,125569,125570,125571,126042,126114,125495,125496,
        125497,126319,126251,125488,126045,125476,125687,125674,125932,126318,126334,126117,126293,126282,
        126158,126107,126079,126150,126078,126093,126095,126096,126097,126281,126000,125977,126015,126073,
        126074,125655,126039,126014,126104,125373,125374,125350,125662,125464,125472,125349,125352,125351,
        125359,125357,125362,125361,125465,125469,125965,125874,125875,125876,125877,125878,125879,125880,
        125881,125882,125883,125884,125885,125886,125887,125888,125889,125890,125891,125892,125893,125894,
        125895,125896,125982,125964,125897,125929,125937,125985,125996,124884,125397,125765,125084,125593,
        125564,125541,125566,125589,125594,125596,125597,125598,125599,125601,125602,125605,125606,125608,
        125611,125543,125550,125544,125545,125554,125557,125559,125471,125809)
"""



try:
    with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn) as connection:
        print("Conexão com o banco de dados Oracle estabelecida com sucesso.")


        print("Executando consulta de pedidos 'OL'...")
        df_ol = pd.read_sql(sql_conexao_clientes_ol, con=connection)
        print(f"{len(df_ol)} linhas retornadas para 'OL'.")


        print("Executando consulta de pedidos por 'CONDIÇÃO'...")
        df_condicao = pd.read_sql(sql_conexao_clientes_condicao, con=connection)
        print(f"{len(df_condicao)} linhas retornadas para 'CONDIÇÃO'.")


        df_completo = pd.concat([df_ol, df_condicao], ignore_index=True)
        print(f"Total de linhas após concatenação: {len(df_completo)}")

        colunas_chave = ['CODFILIAL', 'NUMPED', 'CNPJ_CLIENTE', 'CNPJ_FORNECEDOR']
        df_final = df_completo.drop_duplicates(subset=colunas_chave, keep='first')

        print(f"Total de pedidos únicos após remover duplicatas: {len(df_final)}")

        df_final.to_csv('dados_conexao_unificada.csv', index=False, sep=';', encoding='utf-8-sig', decimal=',')

        print("\n✅ Dados UNIFICADOS e exportados com sucesso para 'dados_conexao_unificada.csv' 🎉")


except oracledb.Error as e:
    error_obj = e.args[0]
    print(f'\n❌ Erro ao se conectar ou executar a query no banco Oracle: {error_obj.code}: {error_obj.message}')
    print("Verifique as credenciais, a DSN e o status do servidor.")

except Exception as e:
    print(f'\n⚠️ Ocorreu um erro inesperado: {e}')