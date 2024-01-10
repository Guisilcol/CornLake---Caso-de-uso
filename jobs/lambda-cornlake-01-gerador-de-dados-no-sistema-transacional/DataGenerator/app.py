from dotenv import load_dotenv
from logger import info, configure_logger
from http_utils import Response
from generator import field, generate_dataframe

import database as db
import pandas as pd
import numpy as np

from sqlalchemy import Connection


def update_existing_data(connection: Connection):
    sql = \
    """
        UPDATE svc.pedidos
        SET 
            status_pedido = 'Entregue',
            data_alteracao_registro = CURRENT_TIMESTAMP
        WHERE 
            data_pedido <= NOW() - INTERVAL '2 day';
    """
    
    connection.exec_driver_sql(sql)
    
    info('Existing data updated successfully')

def generate_new_data(connection: Connection):
    """Generate new data for the SCP and SVP database"""
        
    info("Generating scp.categorias_produto data")
    
    
    categorias_produto = generate_dataframe([
        field(name = 'nome_categoria', type = 'string', quantity = 3, can_generate_null=False, possible_random_values=['Tipo Alimento 1', 'Tipo Alimento 2', 'Tipo Alimento 3']),
    ])

    categorias_produto = categorias_produto.drop_duplicates()
    
    categorias_produto = db.upsert_data(categorias_produto, 'scp', 'categorias_produto', 'categoria_id', connection, ['nome_categoria'], 'full')
    
    info(f'{len(categorias_produto)} rows inserted or updated in scp.categorias_produto')
    
    info("Generating scp.produtos data")
    
    produtos_row_count = 2
    
    produtos = generate_dataframe([
        field(name = 'uuid_externo', type = 'uuid', quantity = produtos_row_count, can_generate_null=False),
        field(name = 'nome_produto', type = 'product', quantity = produtos_row_count, can_generate_null=False),
        field(name = 'descricao_produto', type = 'string', quantity = produtos_row_count, can_generate_null=False, possible_random_values=['Descrição 1', 'Descrição 2', 'Descrição 3']),
        field(name = 'preco', type = 'float', quantity = produtos_row_count, can_generate_null=False, kwargs={'min_value': 50, 'max_value': 120}),
        field(name = 'categoria_id', type = 'integer', quantity = produtos_row_count, can_generate_null=False, possible_random_values=categorias_produto['categoria_id'].to_list()),
    ])
    
    produtos = db.upsert_data(produtos, 'scp', 'produtos', 'produto_id', connection, ['nome_produto'], 'full')
    
    info(f'{len(produtos)} rows inserted or updated in scp.produtos')
    
    info(f'Generating svc.clientes data')
    
    clientes_row_count = 5
    
    clientes = generate_dataframe([
        field(name = 'nome_cliente', type = 'name', quantity = clientes_row_count, can_generate_null=False),
        field(name = 'endereco_entrega', type = 'address', quantity = clientes_row_count, can_generate_null=False),
    ])
    
    clientes['endereco_entrega'] = clientes['endereco_entrega'].str.replace('\n', ', ')
    
    clientes = clientes.drop_duplicates()
    clientes = db.upsert_data(clientes, 'svc', 'clientes', 'cliente_id', connection, ['nome_cliente'], 'full')
    
    info(f'{len(clientes)} rows inserted or updated in svc.clientes')
    
    info(f'Generating svc.pedidos data')
    
    pedidos_row_count = 100000
    
    pedidos = generate_dataframe([
        field(name = 'cliente_id', type = 'integer', quantity = pedidos_row_count, can_generate_null=False, possible_random_values=clientes['cliente_id'].to_list()),
        field(name = 'status_pedido', type = 'string', quantity = pedidos_row_count, can_generate_null=False, possible_random_values=['Pendente', 'Entregue']),
        field(name = 'data_pedido', type = 'datetime', quantity = pedidos_row_count, can_generate_null=False, kwargs={'now_date': True}),
    ])
    
    pedidos = db.insert_data(pedidos, 'svc', 'pedidos', 'pedido_id', connection, 'delta')
    
    info(f'{len(pedidos)} rows inserted in svc.pedidos')
    
    info(f'Generating svc.itens_pedido data')
    
    # STG created to enable the creation of 1 to 3 items within the order
    stg_itens_pedido = pedidos[['pedido_id']].copy()
    
    # Creating a column with the number of products to be created for each order
    stg_itens_pedido['_quantidade_de_produtos'] = np.random.randint(1, 3, len(stg_itens_pedido))
    
    # Repeating the order for each product to be created
    stg_itens_pedido: pd.DataFrame = stg_itens_pedido.loc[stg_itens_pedido.index.repeat(stg_itens_pedido['_quantidade_de_produtos'])]
    stg_itens_pedido = stg_itens_pedido.reset_index(drop=True)
    stg_itens_pedido = stg_itens_pedido.drop(columns=['_quantidade_de_produtos'])
    
    itens_pedidos_row_count = len(stg_itens_pedido)
    
    itens_pedido = generate_dataframe([
        field(name = 'produto_uuid', type = 'uuid', quantity = itens_pedidos_row_count, can_generate_null=False, possible_random_values=produtos['uuid_externo'].to_list()),
        field(name = 'quantidade', type = 'integer', quantity = itens_pedidos_row_count, can_generate_null=False, kwargs={'min_value': 1, 'max_value': 10}),
        field(name = 'preco_unitario', type = 'float', quantity = itens_pedidos_row_count, can_generate_null=False, kwargs={'min_value': 50, 'max_value': 120}),
    ])
    
    itens_pedido = pd.concat([stg_itens_pedido, itens_pedido], axis=1)
    
    itens_pedido = db.insert_data(itens_pedido, 'svc', 'itens_pedido', 'item_pedido_id', connection, 'delta')
    
    info(f'{len(itens_pedido)} rows inserted in svc.itens_pedido')
    
def main(event, context):
    load_dotenv()
    
    
    """Main Function of the AWS Lambda Function"""
    configure_logger()
    
    info('Getting the connection into EC2 PostgreSQL database')
    
    connection = db.get_connection()
    info('Database connection established')
    
    update_existing_data(connection)
    generate_new_data(connection)
    
    connection.commit()
    
    return Response({'statusCode': 200, 'body': 'Data generated successfully'})

if __name__ == "__main__":
    main({}, {})