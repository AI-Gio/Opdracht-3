import pymongo
import psycopg2
import recommendation_algorithms as ra

test_session = {'_id': '1',
                'order': {'products': [{'id': '16862'}, {'id': '43611'}],
                          'payment_method': None, 'taxes': None}}

test_session2 = {'_id': '1',
                'order': {'products': [{'id': '23978'}, {'id': '8533'}],
                          'payment_method': None, 'taxes': None}}


def add_prev_session_to_postgres():
    connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS previous_sessions (sessionid VARCHAR, productid VARCHAR)""")
    connection.commit()
    connection.close()


def add_properties_too_postgres():
    connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS prod_properties (productid VARCHAR, doelgroep VARCHAR)""")
    connection.commit()
    connection.close()


def insert_into_prev_session(cursor, session_id, product_id):
    try:
        query = """INSERT INTO previous_sessions VALUES (%s, %s)"""
        cursor.execute(query, (session_id, product_id))
    except Exception as e:
        print(e)


def insert_into_prod_properties(cursor, productid, properties):
    try:
        query = """INSERT INTO prod_properties VALUES (%s, %s)"""
        cursor.execute(query, (productid, properties))
    except Exception as q:
        print(q)


def copy_product_properties_to_postgres():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    webshop_databases = client['huwebshop']
    products_database = webshop_databases['products']
    products = products_database.find({}, {'properties':1,'_id':1}).limit(5000)
    # print(products[0])

    connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
    cursor = connection.cursor()
    cursor.execute("""SELECT * FROM prod_properties""")
    check_results = cursor.fetchall()
    if not check_results:
        print('Start inserting product properties into postgresql')
        for item in products:
            product_id = item['_id']
            if item['properties']['doelgroep']:
                doelgroep = item['properties']['doelgroep']
            else:
                # print('Geen doelgroep gevonden, doelgroep = {}'.format(item['properties']['doelgroep']))
                continue
            insert_into_prod_properties(cursor=cursor, productid=product_id, properties=doelgroep)
            connection.commit()
        print('finished write to postgres')
    else:
        print('Previous_sessions table already filled')

    connection.close()


def copy_sessions_to_postgres():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    webshop_databases = client['huwebshop']
    sessions_database = webshop_databases['sessions']
    sessions = sessions_database.find({'has_sale': True}, {'order': 1}).limit(1000)
    connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
    cursor = connection.cursor()
    cursor.execute("""SELECT * FROM prod_properties""")
    check_results = cursor.fetchall()
    if not check_results:
        print('Start inserting previous sessions into postgres')
        for session in sessions:
            try:
                for product in session['order']['products']:
                    insert_into_prev_session(cursor=cursor, session_id=session['_id'],
                                             product_id=product['id'])
            except Exception as e:
                print(e)
            connection.commit()
        connection.close()
        print('finished write to postgres')
    else:
        print('Prod_properties table already filled')

if __name__ == '__main__':
    add_properties_too_postgres()
    add_prev_session_to_postgres()
    copy_sessions_to_postgres()
    copy_product_properties_to_postgres()
    print(ra.content_based_filtering(test_session))
    print(ra.collaborative_filtering(test_session2))