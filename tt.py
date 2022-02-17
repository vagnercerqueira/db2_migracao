from flask_db2 import DB2
from flask import Flask, render_template, request, redirect, session, flash, url_for, send_from_directory

app = Flask(__name__)
app.config['DB2_DATABASE'] = 'D1GRS02'
app.config['DB2_HOSTNAME'] = 'bdhdb2-d2con.servicos.bb.com.br'
app.config['DB2_PORT'] = 50000
app.config['DB2_PROTOCOL'] = 'TCPIP'
app.config['DB2_USER'] = 'usrgrs03'
app.config['DB2_PASSWORD'] = 'usrgrs03'
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
    json_instrumento_acao_data = monta_estrutura_colunas(cur)
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
    return json_data
#-----------------------------ATRIBUTOS--------------------------------------------------------
def atributos_AcaoOuData(tb):
    cur = db.connection.cursor()
    sql = f" SELECT NAME FROM Sysibm.syscolumns \
            WHERE tbname = '{tb}' \
            AND TBCREATOR = 'MERCADO_FINANCEIRO' \
            AND NAME NOT IN ('ID', 'DATA_MOVIMENTO', 'ID_INSTRUMENTO')";
    cur.execute(sql)
    return monta_estrutura_colunas(cur)

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
    return monta_estrutura_colunas(cur)

#---------------------------------INSTRUMENTO ACAO--------------------------------------------
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

def migra_instrumento_acao_data(tb):
    atributos = listarAtributos()
    atributos_data = atributos_AcaoOuData(tb)

    sql = ""
    for atributo in atributos_data:
        if tb == 'INSTRUMENTO_ACAO_DATA' and atributo['NAME'] in ['PU_ABERTURA', 'PU_FECHAMENTO']: continue
        sql = f"{sql} \n \
                { ' UNION ' if sql != '' else '' } \
                SELECT IA.ID, {atributos[atributo['NAME']]} ID_ATRIBUTO, IAD.DATA_MOVIMENTO, IAD.{atributo['NAME']} VALOR \
                FROM MERCADO_FINANCEIRO.{tb} IAD \
                INNER JOIN MERCADO_FINANCEIRO.INSTRUMENTO_ACAO IA ON IA.ID=IAD.ID_INSTRUMENTO \
                WHERE IAD.{atributo['NAME']} IS NOT NULL "

    if(sql != ""):
        sql = f"INSERT INTO MERCADO_FINANCEIRO_TESTE.INSTRUMENTO_DATA ( ID_INSTRUMENTO, ID_ATRIBUTO, DATA_MOVIMENTO, VALOR) \n {sql}"
        print(sql)
       # cur = db.connection.cursor()
       # cur.execute(sql)


@app.route('/processa_periodo2', methods=['POST', ])
def processa_periodo():
    dtI = request.form['data_movimento_inicial']
    dtF = request.form['data_movimento_final']

    migra_instrumento_acao_data('INSTRUMENTO_ACAO_DATA')

    return {"TOT_INSTRUMENTO_ACAO": 1}


if __name__ == '__main__':
    app.run(debug=True)