from collections import Counter
import pymongo
import psycopg2

eigen_sessie = {'_id': '1',
                'order': {'products': [{'id': '16862'}, {'id': '43611'}],
                          'payment_method': None, 'taxes': None}}

def add_table_to_postgres():
    connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS previous_sessions (sessionid VARCHAR, productid VARCHAR)""")
    connection.commit()
    connection.close()

def insert_into_postgres(cursor, session_id, product_id):
    try:
        query = "INSERT INTO previous_sessions VALUES ('{}', '{}')".format(session_id, product_id)
        cursor.execute(query)
    except Exception as e:
        print(e)

def mongo_to_postgres():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    webshop_databases = client['huwebshop']
    sessions_database = webshop_databases['sessions']
    sessions = sessions_database.find({'has_sale': True}, {'order': 1})
    print(sessions[0])
    print('Start write to postgres')
    if sessions.count() > 50:
        connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
        for session in sessions[:100]:
            try:
                for product in session['order']['products']:
                    insert_into_postgres(cursor=connection.cursor(), session_id=session['_id'], product_id=product['id'])
            except Exception as e:
                print(e)
        print('finished write to postgres')
        connection.commit()
        connection.close()
    else:
        print('Te weinig sessions')
        quit()

def content_based_filtering(eigen_sessie):
    connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
    cursor = connection.cursor()

    eigen_producten = list(x['id'] for x in eigen_sessie['order']['products'])
    common_sessions = []
    for item in eigen_sessie['order']['products']:
        query = "SELECT sessionid FROM previous_sessions WHERE productid='{}'".format(item['id'])
        cursor.execute(query)
        results = cursor.fetchall()
        print(results)
        common_sessions += list([x[0] for x in results])

    all_products = []
    for sessionid in common_sessions:
        query = "SELECT productid FROM previous_sessions WHERE sessionid='{}'".format(sessionid)
        cursor.execute(query)
        products = cursor.fetchall()
        all_products += list([x[0] for x in products])
    unique_products_prev_sessions = list(Counter(all_products).keys())

    for x in eigen_producten:
        unique_products_prev_sessions.remove(x)

    return unique_products_prev_sessions

    # 1 - uit de postgres vergelijkbare sessie ophalen
    # 2 - eigen_sessie vergelijken met opgehaalde sessies. Opzoek naar overeenkomende producten
    # 3 - niet overeenkomende producten return als suggesties

# add_table_to_postgres()
# mongo_to_postgres()
content_based_filtering(eigen_sessie=eigen_sessie)

# connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
# cursor = connection.cursor()
# query = """SELECT productid FROM previous_sessions"""
# cursor.execute(query)
# results = cursor.fetchall()
# print(results)
#
# temp = []
# for tpl in results:
#     temp.append(tpl[0])
# print(temp)
# from collections import Counter
# print(Counter(temp))