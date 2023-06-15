import streamlit as st

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

subprocess.run(['python3', 'database_generator.py'])

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


