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