from flask_db2 import DB2
from flask import Flask, render_template, request, redirect, session, flash, url_for, send_from_directory

app = Flask(__name__)
app.config['DB2_DATABASE'] = 
app.config['DB2_HOSTNAME'] = 
app.config['DB2_PORT'] = 
app.config['DB2_PROTOCOL'] = 
app.config['DB2_USER'] = 
app.config['DB2_PASSWORD'] = ''
app.config['DB2_POOL_CONNECTIONS'] = True
db = DB2(app)

@app.route('/')
def index():
    return render_template("index.html", titulo="MIGRAR INSTRUMENTOS DATA")

def max_id():
    cur = db.connection.cursor()
    sql = "SELECT COALESCE ( MAX(ID), 0) MAX_ID FROM MERCADO_FINANCEIRO_TESTE.ATRIBUTO";
    cur.execute(sql)
    row = cur.fetchone()
    return int(row[0])


def listarAtributos():
    cur = db.connection.cursor()
    cur.execute("SELECT ID, NOME FROM MERCADO_FINANCEIRO_TESTE.ATRIBUTO")
    json_instrumento_acao_data = monta_estrutura_colunas(cur)['dados']
    nd = {}
    for key in json_instrumento_acao_data:
        nd[key['NOME']] = key['ID']
    return nd


def monta_estrutura_colunas(cur):
    rows = cur.fetchall()
    json_data = []
    row_headers = [x[0] for x in cur.description]
    for result in rows:
        json_data.append(dict(zip(row_headers, result)))
    return {'dados': json_data}

def atributos_AcaoOuData(tb):
    cur = db.connection.cursor()
    cond = ''
    if tb == 'INSTRUMENTO_ACAO_DATA':
        cond = ", 'PU_ABERTURA', 'PU_FECHAMENTO'"

    sql = f" SELECT NAME FROM Sysibm.syscolumns \
            WHERE tbname = '{tb}' \
            AND TBCREATOR = 'MERCADO_FINANCEIRO' \
            AND NAME NOT IN ('ID', 'DATA_MOVIMENTO', 'ID_INSTRUMENTO' {cond})";
    cur.execute(sql)
    return monta_estrutura_colunas(cur)['dados']

def novosAtributos():
    cur = db.connection.cursor()
    maxId = max_id()
    sql = " SELECT NAME, COLTYPE \
            FROM Sysibm.syscolumns \
            WHERE tbname = 'INSTRUMENTO_ACAO_DATA' \
            AND TBCREATOR = 'MERCADO_FINANCEIRO' \
            AND NAME NOT IN ('ID', 'DATA_MOVIMENTO', 'ID_INSTRUMENTO') AND NAME NOT IN ( SELECT NOME FROM MERCADO_FINANCEIRO_TESTE.ATRIBUTO ) \
            UNION \
            SELECT NAME, COLTYPE \
            FROM Sysibm.syscolumns \
            WHERE tbname = 'INSTRUMENTO_DATA' \
            AND TBCREATOR = 'MERCADO_FINANCEIRO' \
            AND NAME NOT IN ('ID', 'DATA_MOVIMENTO', 'ID_INSTRUMENTO') AND NAME NOT IN ( SELECT NOME FROM MERCADO_FINANCEIRO_TESTE.ATRIBUTO ) ";
    cur.execute(sql)
    rows = cur.fetchall()
    for r in rows:
        maxId = maxId + 1;
        cur.execute(
            f"INSERT INTO MERCADO_FINANCEIRO_TESTE.ATRIBUTO (ID, NOME, TIPO) VALUES ({maxId}, '{r[0]}', '{r[1]}')")
    return {'QTD_ATRIBUTOS_NOVOS': len(rows)}


#-----------------------------------------------------------INSTRUMENTO_ACAO------------------------------------------------------------#
def migra_instrumento_acao():
    sql = f" INSERT INTO MERCADO_FINANCEIRO_TESTE.INSTRUMENTO_ACAO ( ID, CODIGO_ISIN, TICKER, NOME, CODIGO_B3, CODIGO_BLOOMBERG, TIPO, SETOR_ECONOMICO, SUB_SETOR, SEGMENTO ) \
             SELECT IA.ID, I.CODIGO_ISIN, IA.TICKER, EF.NOME, IND.CODIGO_B3, IA.CODIGO_BLOOMBERG, IA.TIPO, IA.SETOR_ECONOMICO, IA.SUB_SETOR, IA.SEGMENTO \
             FROM MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA \
             LEFT JOIN MERCADO_FINANCEIRO.INSTRUMENTO I ON I.ID=IA.ID \
             LEFT JOIN MERCADO_FINANCEIRO.ELEMENTO_FINANCEIRO EF ON EF.ID=I.ID \
             LEFT JOIN MERCADO_FINANCEIRO.INSTRUMENTO_FUTURO_ACAO IFA ON IFA.ID_INSTRUMENTO_ACAO=IA.ID \
             LEFT JOIN MERCADO_FINANCEIRO.INSTRUMENTO_FUTURO IFU ON IFU.ID=IFA.ID \
             LEFT JOIN MERCADO_FINANCEIRO.INSTRUMENTO_FUTURO_INDICE IFI ON IFI.ID=IFA.ID \
             LEFT JOIN MERCADO_FINANCEIRO.INDICE IND ON IND.ID=IFI.ID_INDICE \
             WHERE IA.ID NOT IN ( SELECT ID FROM MERCADO_FINANCEIRO_TESTE.INSTRUMENTO_ACAO ) \
             GROUP BY IA.ID, I.CODIGO_ISIN, IA.TICKER, EF.NOME, IND.CODIGO_B3,IA.CODIGO_BLOOMBERG, IA.TIPO, IA.SETOR_ECONOMICO, IA.SUB_SETOR, IA.SEGMENTO"
    cur = db.connection.cursor()
    cur.execute(sql)
    return {'TOT':1}

def insert_instrumentos_acao(json_instrumento_acao):

    dados_insert = ""
    tot_insert = 0
    for jd in json_instrumento_acao:
        nome = ( jd['NOME'] ).replace("'","''")
        dados_insert = f"{dados_insert}   ( {jd['ID']}, '{jd['CODIGO_ISIN']}', '{jd['TICKER']}', '{nome}', '{jd['CODIGO_B3']}', '{jd['CODIGO_BLOOMBERG']}', '{jd['TIPO']}','{jd['SETOR_ECONOMICO']}','{jd['SUB_SETOR']}','{jd['SEGMENTO']}' ),"
        tot_insert += 1

        if (tot_insert % 10000) == 0:
            dados_insert = dados_insert.rstrip(dados_insert[-1])
            commit_instrumentos_data(dados_insert)
            dados_insert = "";


    if tot_insert > 0 and dados_insert != "":
        dados_insert = dados_insert.rstrip(dados_insert[-1])
        sql = f"INSERT INTO MERCADO_FINANCEIRO_TESTE.INSTRUMENTO_ACAO ( ID, CODIGO_ISIN, TICKER, NOME, CODIGO_B3, CODIGO_BLOOMBERG, TIPO, SETOR_ECONOMICO, SUB_SETOR, SEGMENTO ) VALUES {dados_insert}"
        cur = db.connection.cursor()
        cur.execute(sql)
    return {'TOT': tot_insert}

def deleta_instrumentos_acao(dataMovimentoIni, dataMovimentoFim):
    sql = f"DELETE FROM MERCADO_FINANCEIRO_TESTE.INSTRUMENTO_ACAO \
            WHERE ID IN ( \
                SELECT ID_INSTRUMENTO \
                FROM MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA \
                INNER JOIN  MERCADO_FINANCEIRO.INSTRUMENTO_ACAO_DATA IAD  ON IA.ID=IAD.ID_INSTRUMENTO \
                WHERE IAD.DATA_MOVIMENTO BETWEEN '{dataMovimentoIni}' AND '{dataMovimentoFim}' \
            UNION \
                SELECT ID_INSTRUMENTO \
                FROM MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA \
                INNER JOIN  MERCADO_FINANCEIRO.INSTRUMENTO_DATA IDT  ON IA.ID=IDT.ID_INSTRUMENTO \
                WHERE IDT.DATA_MOVIMENTO BETWEEN '{dataMovimentoIni}' AND '{dataMovimentoFim}' \
            ) ";
    cur = db.connection.cursor()
    cur.execute(sql)
#-----------------------------------------------------------INSTRUMENTO_DATA------------------------------------------------------------#
def lista_instrumentos_acao_data(dataMovimentoIni, dataMovimentoFim):
    #atributos = atributos_AcaoOuData('INSTRUMENTO_ACAO_DATA')
    #cond_or = "";
    #for atributo in atributos:
    #    cond_or = f"{cond_or} {atributo['NAME']} IS NOT NULL OR"
    #cond_or = cond_or.rstrip(cond_or[-1])
    #cond_or = cond_or.rstrip(cond_or[-1])
    sql = f"SELECT IAD.* FROM MERCADO_FINANCEIRO.INSTRUMENTO_ACAO_DATA IAD \
            INNER JOIN MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA ON IA.ID=IAD.ID_INSTRUMENTO \
            WHERE IAD.DATA_MOVIMENTO BETWEEN '{dataMovimentoIni}' AND '{dataMovimentoFim}'"

    cur = db.connection.cursor()
    cur.execute(sql)

    json_instrumento_acao_data = monta_estrutura_colunas(cur)
    return json_instrumento_acao_data

def lista_instrumentos_data(dataMovimentoIni, dataMovimentoFim):
    sql = f"SELECT IDT.* FROM MERCADO_FINANCEIRO.INSTRUMENTO_DATA IDT \
            INNER JOIN MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA ON IA.ID=IDT.ID_INSTRUMENTO \
            WHERE IDT.DATA_MOVIMENTO BETWEEN '{dataMovimentoIni}' AND '{dataMovimentoFim}'";
    cur = db.connection.cursor()
    cur.execute(sql)

    json_instrumento_acao_data = monta_estrutura_colunas(cur)
    return json_instrumento_acao_data

def insert_instrumentos_data(json_instrumento_acao_data, tb):
    json_atributos = listarAtributos()
    dados_insert = ""
    tot_insert = 0
    for jd in json_instrumento_acao_data:
        if tb == 'IAD':  #REMOVE OS CAMPOS SEGUINTES, PARA N INSERIR NO INSTRUMENTO_DATA CASO A TABELA ORIGEM SEJA INSTR ACAO DATA
            jd.pop("PU_ABERTURA")
            jd.pop("PU_FECHAMENTO")
        for key in jd:
            if jd[key] is not None and key not in ['ID_INSTRUMENTO', 'DATA_MOVIMENTO']:
                id_atributo = json_atributos[key]
                dados_insert = dados_insert + f"\n( {jd['ID_INSTRUMENTO']}, {id_atributo}, '{jd['DATA_MOVIMENTO']}', '{jd[key]}' ),"
                tot_insert += 1
                if (tot_insert % 5000) == 0:
                    dados_insert = dados_insert.rstrip(dados_insert[-1])
                    commit_instrumentos_data(dados_insert)
                    dados_insert = "";

    if tot_insert > 0 and dados_insert != "":
        dados_insert = dados_insert.rstrip(dados_insert[-1])
        commit_instrumentos_data(dados_insert)

    return {'TOT': tot_insert}

def commit_instrumentos_data(dados_insert):
    sql = f"INSERT INTO MERCADO_FINANCEIRO_TESTE.INSTRUMENTO_DATA ( ID_INSTRUMENTO, ID_ATRIBUTO, DATA_MOVIMENTO, VALOR) VALUES {dados_insert}";
    cur = db.connection.cursor()
    cur.execute(sql)

def deleta_instrumentos_data(dataMovimentoIni, dataMovimentoFim):
    sql = f"    DELETE FROM MERCADO_FINANCEIRO_TESTE.INSTRUMENTO_DATA \
                WHERE ID_INSTRUMENTO IN ( \
                        SELECT ID_INSTRUMENTO \
                        FROM MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA \
                        INNER JOIN  MERCADO_FINANCEIRO.INSTRUMENTO_ACAO_DATA IAD  ON IA.ID=IAD.ID_INSTRUMENTO \
                        WHERE IAD.DATA_MOVIMENTO BETWEEN '{dataMovimentoIni}' AND '{dataMovimentoFim}' \
                    UNION \
                        SELECT ID_INSTRUMENTO \
                        FROM MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA \
                        INNER JOIN  MERCADO_FINANCEIRO.INSTRUMENTO_DATA IDT  ON IA.ID=IDT.ID_INSTRUMENTO \
                        WHERE IDT.DATA_MOVIMENTO BETWEEN '{dataMovimentoIni}' AND '{dataMovimentoFim}' \
                )";
    cur = db.connection.cursor()
    cur.execute(sql)

#----------------------------------NOVAS DATAS----------------
def lista_registros_novos(tb):

    atributos = atributos_AcaoOuData(tb)
    cond_or = "";
    for atributo in atributos:
        cond_or = f"{cond_or} {atributo['NAME']} IS NOT NULL OR"
    cond_or = cond_or.rstrip(cond_or[-1])
    cond_or = cond_or.rstrip(cond_or[-1])

    sql = f"WITH NE AS \
            ( \
                SELECT IAD.* \
                FROM MERCADO_FINANCEIRO.{tb} IAD \
                INNER JOIN MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA ON IA.ID=IAD.ID_INSTRUMENTO \
                WHERE NOT EXISTS ( \
                    SELECT 1 \
                    FROM MERCADO_FINANCEIRO_TESTE.INSTRUMENTO_DATA ID2 \
                    WHERE IAD.ID_INSTRUMENTO=ID2.ID_INSTRUMENTO AND IAD.DATA_MOVIMENTO=ID2.DATA_MOVIMENTO \
                ) \
            ) \
            SELECT * \
            FROM NE \
            WHERE ( {cond_or} )"
    cur = db.connection.cursor()
    cur.execute(sql)

    json_instrumento_acao_data = monta_estrutura_colunas(cur)
    return json_instrumento_acao_data

@app.route('/processa_periodo', methods=['POST', ])
def processa_periodo():
    dtI = request.form['data_movimento_inicial']
    dtF = request.form['data_movimento_final']

    novosAtributos()#BUSCA AS COLUNAS NOVAS E INSERE NA TABELA ATRIBUTO
    #tot_inserido_ia = migra_instrumento_acao()

    #====================================DELETA OS INSTRUMENTOS_DATA DE UM PERIODO=====================================
    #deleta_instrumentos_data(dtI, dtF)
    #deleta_instrumentos_acao(dtI, dtF)

    # ====================================INSERE OS INSTRUMENTOS_DATA DE UM PERIODO=====================================
    #rows_iad = lista_instrumentos_acao_data(dtI, dtF)
    #tot_inserido_ad = insert_instrumentos_data(rows_iad['dados'], 'IAD')

    #rows_idt = lista_instrumentos_data(dtI, dtF)
    #tot_inserido_idt = insert_instrumentos_data(rows_idt['dados'], 'IDT')

    # ====================================INSERE OS INSTRUMENTOS_DATA QUE AINDA NAO FORAM MIGRADOS PARA A BASE TESTE=====
    #novos_iad = lista_registros_novos('INSTRUMENTO_ACAO_DATA')
    #tot_inserido_ad = insert_instrumentos_data(novos_iad['dados'], 'IAD')

    novos_id = lista_registros_novos('INSTRUMENTO_DATA')
    tot_inserido_idt = insert_instrumentos_data(novos_id['dados'], 'IDT')

    #total_instrumento_data = tot_inserido_ad['TOT'] + tot_inserido_idt['TOT']

    return {"TOT_INSTRUMENTO_ACAO": 0, "TOT_INSTRUMENTO_DATA": 1}


if __name__ == '__main__':
    app.run(debug=True)