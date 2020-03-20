from collections import Counter
import psycopg2


def content_based_filtering(own_session):
    connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
    cursor = connection.cursor()

    own_products = list(x['id'] for x in own_session['order']['products'])
    common_sessions = []
    for item in own_session['order']['products']:
        query = """SELECT sessionid FROM previous_sessions WHERE productid=%s"""
        cursor.execute(query, (item['id'],))
        results = cursor.fetchall()
        common_sessions += list([x[0] for x in results])

    all_products = []
    for sessionid in common_sessions:
        query = """SELECT productid FROM previous_sessions WHERE sessionid=%s"""
        cursor.execute(query, (sessionid,))
        products = cursor.fetchall()
        all_products += list([x[0] for x in products])
    unique_products_prev_sessions = list(Counter(all_products).keys())

    for x in own_products:
        if x in unique_products_prev_sessions:
            unique_products_prev_sessions.remove(x)

    return unique_products_prev_sessions

def collaborative_filtering(own_session):
    connection = psycopg2.connect("dbname=Voordeelshop user=postgres password=Muis1234 port=5433")
    cursor = connection.cursor()
    own_products = list(x['id'] for x in own_session['order']['products'])
    query_own = """SELECT doelgroep FROM prod_properties WHERE productid=%s"""
    query_collaborative_products = """SELECT productid FROM prod_properties WHERE doelgroep=%s"""

    all_properties = []
    for product_id in own_products:
        cursor.execute(query_own, (str(product_id),))
        results = cursor.fetchall()
        all_properties += list(x[0] for x in results)

    collaborative_products = []

    for property in all_properties:
        cursor.execute(query_collaborative_products, (property,))
        products = cursor.fetchall()
        collaborative_products += list(x[0] for x in products)

    return collaborative_products