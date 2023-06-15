import streamlit as st

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def create_database():
    # Connect to the database
    conn = sqlite3.connect('TP2.db')
    cur = conn.cursor()

    # Read the CSV files using pandas
    df_NCM = pd.read_csv('https://raw.githubusercontent.com/milbravo/IBD/main/Tabela_NCM', sep=';', encoding='ISO-8859-1')
    df_Dados = pd.read_csv('https://raw.githubusercontent.com/milbravo/IBD/main/Dados', sep=';', encoding='ISO-8859-1')


    # Write the data to the database
    df_NCM.to_sql('Tabela_NCM', conn, if_exists='replace')
    df_Dados.to_sql('Dados', conn, if_exists='replace')

    #region Creating tables

    query1 = """
    CREATE TABLE IF NOT EXISTS UME (
        id integer PRIMARY KEY,
        medida text
    );
    """

    cur.execute(query1)

    query2 = """
    CREATE TABLE IF NOT EXISTS NCM (
        numero text PRIMARY KEY,
        descricao text,
    descricao_concatenada text,
    ume integer
    );
    """

    cur.execute(query2)

    query3 = """
    CREATE TABLE IF NOT EXISTS PAIS (
        id integer PRIMARY KEY,
        pais_nome text
    );
    """
    cur.execute(query3)

    query4 = """
    CREATE TABLE IF NOT EXISTS PAIS_NCM (
        pais integer,
        ncm text,
    PRIMARY KEY (pais, ncm)
        FOREIGN KEY (pais) REFERENCES PAIS(id)
    FOREIGN KEY (ncm) REFERENCES NCM(numero)
    );
    """

    cur.execute(query4)

    query5 = """
    CREATE TABLE IF NOT EXISTS FOB_KG (
        id integer PRIMARY KEY,
        minimo real,
        maximo real,
    media real,
    mediana real,
    desvio_padrao real,
    quartil1 real,
    quartil3 real
    );
    """

    cur.execute(query5)

    query6 = """
    CREATE TABLE IF NOT EXISTS FOB_UME (
        id integer PRIMARY KEY,
        minimo real,
        maximo real,
    media real,
    mediana real,
    desvio_padrao real,
    quartil1 real,
    quartil3 real
    );
    """

    cur.execute(query6)

    query7 = """
    CREATE TABLE IF NOT EXISTS FRETE (
        id integer PRIMARY KEY,
        minimo real,
        maximo real,
    media real,
    mediana real,
    desvio_padrao real
    );
    """

    cur.execute(query7)

    query8 = """
    CREATE TABLE IF NOT EXISTS SEGURO (
        id integer PRIMARY KEY,
        minimo real,
        maximo real,
    media real,
    mediana real,
    desvio_padrao real
    );
    """

    cur.execute(query8)

    query9 = """
    CREATE TABLE IF NOT EXISTS IMPORTACAO (
        pais integer,
        ncm text,
        fob_kg integer,
        fob_ume integer,
    frete integer,
    seguro integer,
        vmle_soma real,
    peso_liq_soma real,
    qtd_ume_soma real,
    PRIMARY KEY (pais, ncm)
    FOREIGN KEY (pais) REFERENCES PAIS (id),
    FOREIGN KEY (ncm) REFERENCES NCM (numero),
    FOREIGN KEY (fob_kg) REFERENCES FOB_KG (id),
    FOREIGN KEY (fob_ume) REFERENCES FOB_UME (id),
    FOREIGN KEY (frete) REFERENCES FRETE (id)
    FOREIGN KEY (seguro) REFERENCES SEGURO (id)
    );
    """
    cur.execute(query9)

    #endregion

    #region Inserting data

    query10 = """
    INSERT INTO PAIS (pais_nome)
    SELECT DISTINCT pais_origem
    FROM
        Dados
    """

    cur.execute(query10)

    query11 = """
    INSERT INTO UME (medida)
    SELECT DISTINCT ume
    FROM
        Dados
    """

    cur.execute(query11)

    query12 = """
    INSERT INTO NCM (numero)
        SELECT DISTINCT ncm
    FROM
        Dados
    """

    cur.execute(query12)

    query13 = """
    INSERT INTO PAIS_NCM (pais, ncm)
        SELECT pais_origem, ncm
    FROM
        Dados
    """

    cur.execute(query13)

    query14 = """
    INSERT INTO
    FOB_KG(id, minimo, maximo, media, mediana, desvio_padrao, quartil1, quartil3)
        SELECT "index", fob_kg_minimo_dolar_truncado, fob_kg_maximo_dolar_truncado,
    fob_kg_media_dolar_truncado, fob_kg_mediana_dolar_truncado,
    fob_kg_desvio_padrao_dolar_truncado, fob_kg_quartil1_truncado,
    fob_kg_quartil3_truncado
    FROM
        Dados
    """

    cur.execute(query14)

    query15 = """
    INSERT INTO
    FOB_UME(id,	minimo, maximo, media, mediana, desvio_padrao, quartil1, quartil3)
        SELECT "index", fob_ume_minimo_dolar_truncado, fob_ume_maximo_dolar_truncado,
    fob_ume_media_dolar_truncado, fob_ume_mediana_dolar_truncado,
    fob_ume_desvio_padrao_dolar_truncado, fob_ume_quartil1_truncado,
    fob_ume_quartil3_truncado
    FROM
        Dados
    """

    cur.execute(query15)

    query16 = """
    INSERT INTO FRETE (id,	minimo, maximo, media, mediana, desvio_padrao)
        SELECT "index", frete_minimo_dolar_truncado, frete_media_dolar_truncado,
    frete_mediana_dolar_truncado, frete_maximo_dolar_truncado,
    frete_desvio_padrao_dolar_truncado
    FROM
        Dados
    """

    cur.execute(query16)

    query17 = """
    INSERT INTO SEGURO (id,	minimo, maximo, media, mediana, desvio_padrao)
        SELECT "index", seguro_minimo_dolar_truncado, seguro_media_dolar_truncado,
    seguro_mediana_dolar_truncado, seguro_maximo_dolar_truncado,
    seguro_desvio_padrao_dolar_truncado
    FROM
        Dados
    """

    cur.execute(query17)

    query18 = """
    INSERT INTO
    IMPORTACAO(pais,ncm, fob_kg, fob_ume, frete, seguro, vmle_soma, peso_liq_soma,
    qtd_ume_soma)
        SELECT pais_origem, ncm, "index","index", "index", "index",
    vmle_soma_dolar_truncado, peso_liq_soma_truncado, qtd_ume_soma_truncado
    FROM
        Dados
    """

    cur.execute(query18)

    #endregion

    #region Updating data

    query19 = """
    UPDATE PAIS_NCM
    SET pais = (
        SELECT P.id
        FROM PAIS AS P
        WHERE P.pais_nome IS PAIS_NCM.pais
    )
    """

    cur.execute(query19)

    query20 = """
    UPDATE NCM
    SET descricao = (
        SELECT T.Descricao
        FROM Tabela_NCM AS T
        WHERE T.Codigo IS NCM.numero
    )
    """

    cur.execute(query20)

    query21 = """
    UPDATE NCM
    SET descricao_concatenada = (
        SELECT T.Descricao_Concatenada
        FROM Tabela_NCM AS T
        WHERE T.Codigo IS NCM.numero
    )
    """

    cur.execute(query21)

    query22 = """
    UPDATE NCM
    SET ume = (
        SELECT D.ume
        FROM Dados AS D
        WHERE D.ncm IS NCM.numero
    )
    """

    cur.execute(query22)

    query23 = """
    UPDATE NCM
    SET ume = (
        SELECT U.id
        FROM UME AS U
        WHERE U.medida IS NCM.ume
    )
    """

    cur.execute(query23)

    query24 = """
    UPDATE IMPORTACAO
    SET pais = (
        SELECT P.id
        FROM PAIS AS P
        WHERE P.pais_nome IS IMPORTACAO.pais
    )
    """

    cur.execute(query24)

    #endregion

    conn.commit()
    conn.close()

# Run the function to create the database
create_database()

########################################################################################
# Connect to the existing database
conn = sqlite3.connect('TP2.db')

# Create a cursor object to execute SQL queries
c = conn.cursor()
key=0

#region generating tables

st.text('UNIVERSIDADE FEDERAL DE MINAS GERAIS - INTRODUÇÃO A BANCO DE DADOS')
st.text('ATIVIDADE EXTRA: streamlit')
st.text('Grupo: \nAndre Luís da Costa - 2018020352\nFelipe da Cruz Basilio - 2018020530\nMilton Pereira Bravo Neto - 2018072549\nEduardo Gabriel Carvalho Marinho - 2018020549')
st.title('Análise de dados\n')
st.write('Nesse projeto vamos analisaras consultas realizadas no trabalho pelo grupo de forma dinâmica. A intenção não é variar todos os parâmetros de todas as pesquisas para poder fazer qualquer pesquisa e sim variar os campos utilizados nas clausulas WHERE propostas no trabalho')

## filtros para a tabela

# função para selecionar a quantidade de linhas do dataframe
def mostra_qntd_linhas(dataframe, keyParam):
    qntd_linhas = st.sidebar.slider('Selecione a quantidade de linhas que deseja mostrar na tabela', min_value = 1, max_value = len(dataframe)+1, step = 1, key=keyParam)

    st.write(dataframe.head(qntd_linhas))

#region checkbox consulta 6.1.1

selectImportacao = """

SELECT *
FROM IMPORTACAO AS I

"""

df_importacao = pd.read_sql_query(selectImportacao, conn)

checkbox_mostrar_consulta_6_1_1= st.sidebar.checkbox('Mostrar consulta 6.1.1: Importações vindas do país com código 1.')

if checkbox_mostrar_consulta_6_1_1:
    key+=1
    st.title('IMPORTACAO\n')

    st.sidebar.markdown('## Filtro para a tabela Importacao')

    paises = list(df_importacao['pais'].unique())
    paises.append('Todos')

    pais_selecionado = st.sidebar.selectbox('Selecione o pais para apresentar na tabela', options = paises, index=0, key=key)
    key+=1

    if pais_selecionado != 'Todos':
        df_importacao_filtrado = df_importacao.query('pais == @pais_selecionado')
        mostra_qntd_linhas(df_importacao_filtrado, keyParam=key)      
    else:
        mostra_qntd_linhas(df_importacao, keyParam=key)
#endregion

#region checkbox consulta 6.1.2

selectImportacao = """

SELECT *
FROM IMPORTACAO AS I

"""

df_importacao2 = pd.read_sql_query(selectImportacao, conn)

checkbox_mostrar_consulta_6_1_2 = st.sidebar.checkbox('Mostrar consulta 6.1.2: Importações vindas do país com código 93 e com peso superior a 5 toneladas.')

if checkbox_mostrar_consulta_6_1_2:
    key+=1
    st.title('IMPORTACAO\n')

    st.sidebar.markdown('## Filtro para a tabela Importacao')

    #1o filtro
    paises = list(df_importacao2['pais'].unique())
    paises.append('Todos')

    pais_selecionado = st.sidebar.selectbox('Selecione o pais para apresentar na tabela', options = paises, index=92, key=key)

    if pais_selecionado != 'Todos':
        df_importacao_filtrado1 = df_importacao2.query('pais == @pais_selecionado')
    else:
        df_importacao_filtrado1 = df_importacao2

    #2o filtro
    key+=1
    pesos = list(df_importacao_filtrado1['peso_liq_soma'].unique())
    pesos.append('Todos')

    peso_selecionado = st.sidebar.selectbox('Selecione o peso para filtrar tabela', options = pesos, index=0, key=key)
    key+=1

    if peso_selecionado != 'Todos':
        df_importacao_filtrado2 = df_importacao_filtrado1.query('peso_liq_soma == @peso_selecionado')
        mostra_qntd_linhas(df_importacao_filtrado2, keyParam=key)      
    else:
        mostra_qntd_linhas(df_importacao_filtrado1, keyParam=key)
#endregion

#region checkbox consulta 6.2.1

selectImportacao = """

SELECT
  P.pais_nome, F.maximo
FROM
  IMPORTACAO AS I
  INNER JOIN FRETE as F ON I.frete = F.id
  INNER JOIN PAIS as P ON I.pais = P.id
ORDER BY F.maximo ASC

"""

df_importacao3 = pd.read_sql_query(selectImportacao, conn)

checkbox_mostrar_consulta_6_2_1= st.sidebar.checkbox('Mostrar consulta 6.2.1: Nome dos 10 primeiros países que tem frete máximo menor que 500 dólares.')

if checkbox_mostrar_consulta_6_2_1:
    key+=1
    st.title('IMPORTACAO\n')

    st.sidebar.markdown('## Filtro para a tabela Importacao com seus joins, exibindo somente os nomes dos paises')

    fretes_maximos = list(df_importacao3['maximo'].unique())
    fretes_maximos.append('Todos')

    frete_max_selecionado = st.sidebar.selectbox('Selecione o frete máximo para filtrar a tabela', options = fretes_maximos, index=0, key=key)
    key+=1

    if frete_max_selecionado != 'Todos':
        df_importacao_filtrado = df_importacao3.query('maximo <= @frete_max_selecionado')
        mostra_qntd_linhas(df_importacao_filtrado, keyParam=key)      
    else:
        mostra_qntd_linhas(df_importacao3, keyParam=key)
#endregion

#region checkbox consulta 6.2.2

selectImportacao = """

SELECT
  I.ncm, I.pais, S.media, P.pais_nome
FROM
  IMPORTACAO AS I
  INNER JOIN SEGURO as S ON I.seguro = S.id
  INNER JOIN PAIS as P ON I.pais = P.id

"""

df_importacao4 = pd.read_sql_query(selectImportacao, conn)

checkbox_mostrar_consulta_6_2_2= st.sidebar.checkbox('Mostrar consulta 6.2.2: Importação com seguro medio de 500 dólares e vindo dos ESTADOS UNIDOS.')

if checkbox_mostrar_consulta_6_2_2:
    key+=1
    st.title('IMPORTACAO\n')

    st.sidebar.markdown('## Filtro para a tabela Importacao com seus joins, exibindo somente os nomes dos paises e os respectivos ncms')

    #1o filtro
    paises = list(df_importacao4['pais_nome'].unique())
    paises.append('Todos')

    pais_selecionado = st.sidebar.selectbox('Selecione o pais para apresentar na tabela', options = paises, index=0, key=key)

    if pais_selecionado != 'Todos':
        df_importacao_filtrado3 = df_importacao4.query('pais_nome == @pais_selecionado')
    else:
        df_importacao_filtrado3 = df_importacao4

    #2o filtro
    key+=1
    valores_medios_seguro = list(df_importacao_filtrado3['media'].unique())
    valores_medios_seguro.append('Todos')

    valor_medio_selecionado = st.sidebar.selectbox('Selecione o peso para filtrar tabela', options = valores_medios_seguro, index=0, key=key)
    key+=1

    if valor_medio_selecionado != 'Todos':
        df_importacao_filtrado4 = df_importacao_filtrado3.query('media == @valor_medio_selecionado')
        mostra_qntd_linhas(df_importacao_filtrado4, keyParam=key)      
    else:
        mostra_qntd_linhas(df_importacao_filtrado3, keyParam=key)
#endregion

#region checkbox consulta 6.2.3

selectImportacao = """

SELECT
  I.ncm, I.pais, F.mediana, P.pais_nome
FROM
  IMPORTACAO AS I
  INNER JOIN FOB_KG as F ON I.fob_kg = F.id
  INNER JOIN PAIS as P ON I.pais = P.id

"""

df_importacao5 = pd.read_sql_query(selectImportacao, conn)

checkbox_mostrar_consulta_6_2_3= st.sidebar.checkbox('Mostrar consulta 6.2.3: Importação com FOB_KG com mediana de 10 e vindo da ITALIA.')

if checkbox_mostrar_consulta_6_2_3:
    key+=1
    st.title('IMPORTACAO\n')

    st.sidebar.markdown('## Filtro para a tabela Importacao com seus joins, exibindo somente os nomes dos paises e os respectivos ncms, além de mediana e nome do país')

    #1o filtro
    paises = list(df_importacao5['pais_nome'].unique())
    paises.append('Todos')

    pais_selecionado = st.sidebar.selectbox('Selecione o pais para apresentar na tabela', options = paises, index=0, key=key)

    if pais_selecionado != 'Todos':
        df_importacao_filtrado5 = df_importacao5.query('pais_nome == @pais_selecionado')
    else:
        df_importacao_filtrado5 = df_importacao5

    #2o filtro
    key+=1
    valores_mediana = list(df_importacao_filtrado5['mediana'].unique())
    valores_mediana.append('Todos')

    valor_mediana_selecionado = st.sidebar.selectbox('Selecione a mediana para filtrar tabela', options = valores_mediana, index=0, key=key)
    key+=1

    if valor_mediana_selecionado != 'Todos':
        df_importacao_filtrado6 = df_importacao_filtrado5.query('mediana == @valor_mediana_selecionado')
        mostra_qntd_linhas(df_importacao_filtrado6, keyParam=key)      
    else:
        mostra_qntd_linhas(df_importacao_filtrado5, keyParam=key)
#endregion


#endregion


