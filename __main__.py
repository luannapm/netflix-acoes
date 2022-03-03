#Importação das bibliotecas
import datetime
import pandas as pd
from connector import Interface_mysql

#extração dos dados
df = pd.read_csv(r'C:\netlix_acoes\NFLX.csv')
#print(df)

#Conectando ao mysql
if __name__ == "__main__":
    try:
        
        banco = Interface_mysql("root", "1234", "localhost", "netflix")
        
        #CRIANDO TABELA 'NETFLIX_ACOES_2002-2022'
        query = f"""CREATE TABLE netflix_acoes(
            DATA_ABERTURA DATE,
            VALOR_ABERTURA FLOAT,
            ALTA_VALOR FLOAT,
            BAIXA_VALOR FLOAT,
            VALOR_ENCERRAMENTO FLOAT,
            AJUSTE_ENCERRAMENTO FLOAT,
            VOLUME_VENDAS INT
        )"""
        banco.inserir(query)

        print("Criação confirmada!")
        
        #PERCORRENDO O DATAFRAME E INSERINDO SEUS VALORES NA TABELA CRIADA
        for col in range(len(df)):
            data = df.loc[col, 'Date']
            abertura = df.loc[col, 'Open']
            alta = df.loc[col, 'High']
            baixa = df.loc[col, 'Low']
            encerramento = df.loc[col, 'Close']
            ajustamento = df.loc[col, 'Adj Close']
            volume = df.loc[col, 'Volume']

            query = f'''INSERT INTO netflix_acoes(DATA_ABERTURA, VALOR_ABERTURA, ALTA_VALOR, BAIXA_VALOR, VALOR_ENCERRAMENTO, AJUSTE_ENCERRAMENTO, VOLUME_VENDAS) 
                        VALUES{data, abertura, alta, baixa, encerramento, ajustamento, volume}'''

            banco.inserir(query)
    
        print("\nInserção confirmada!\n")
       
        
    except Exception as e:
        print(str(datetime.datetime.now()) + " ERRO: netflix: main: " + str(e))    