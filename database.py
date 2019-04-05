import psycopg2
import csv
from urllib.parse import urlparse, uses_netloc
import configparser


config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']
conn = None
################################################################################
#  REMOVE THESE LISTS, THEY ARE HERE AS MOCK DATA ONLY.
# customers = list()
# customers.append({'id': 0, 'firstName': "Kasandra", 'lastName': "Cryer", 'street':"884 Meadow Lane", 'city':"Bardstown", 'state':"KY", 'zip':  "4004"})
# customers.append({'id': 1, 'firstName': "Ferne", 'lastName': "Linebarger", 'street':"172 Academy Street", 'city':"Morton Grove", 'state':"IL", 'zip':  "60053"})
# customers.append({'id': 2, 'firstName': "Britany", 'lastName': "Manges", 'street':"144 Fawn Court", 'city':"Antioch", 'state':"TN", 'zip':  "37013"})
#
# products = list()
# products.append({'id':0, 'name': "Product A", 'price': 5})
# products.append({'id':1, 'name': "Product B", 'price': 10})
# products.append({'id':2, 'name': "Product C", 'price': 2.5})
#
# orders = list()
# orders.append({'id':0, 'customerId': 0, 'productId':0, 'date':"2017-04-12"})
# orders.append({'id':1, 'customerId': 2, 'productId':1, 'date':"2015-08-13"})
# orders.append({'id':2, 'customerId': 0, 'productId':2, 'date':"2019-10-18"})
# orders.append({'id':3, 'customerId': 1, 'productId':0, 'date':"2011-03-30"})
# orders.append({'id':4, 'customerId': 0, 'productId':1, 'date':"2017-09-01"})
# orders.append({'id':5, 'customerId': 1, 'productId':2, 'date':"2017-12-17"})
#

################################################################################
# The following three functions are only for mocking data - they should be removed,
# def _find_by_id(things, id):
#     results = [thing for thing in things if thing['id'] == id]
#     if ( len(results) > 0 ):
#         return results[0]
#     else:
#         return None
#
# def _upsert_by_id(things, thing):
#     if 'id' in thing:
#         index = [i for i, c in enumerate(things) if c['id'] == thing['id']]
#         if ( len(index) > 0 ) :
#             del things[index[0]]
#             things.append(thing)
#     else:
#         thing['id'] = len(things)
#         things.append(thing)
#
# def _delete_by_id(things, id):
#     index = [i for i, c in enumerate(things) if c['id'] == id]
#     if ( len(index) > 0 ) :
#         del things[index[0]]
#

# def check_if_tables_exist():
#     curs = conn.cursor()
#     curs.execute('select exits(select 1 from information_schema.tables where table_schema=(%s) and table_name=(%s))', ('public', 'customers'))
#     customers = curs.fetchone()
#     curs.execute('select exits(select 1 from information_schema.tables where table_schema=(%s) and table_name=(%s))', ('public', 'products'))
#     products = curs.fetchone()
#     curs.execute('select exits(select 1 from information_schema.tables where table_schema=(%s) and table_name=(%s))', ('public', 'orders'))
#     orders = curs.fetchone()
#     return(customers[0] and products[0] and class[0])
def create_tables():
    curs = conn.cursor()
    curs.execute('create table if not exists customers(id serial, firstName text, lastName text, street text, city text, state text, zip int, primary key(id))')
    curs.execute('create table if not exists products(id serial, name text, price real, primary key(id))')
    curs.execute('create table if not exists orders(id serial, customerId int, productId int, date text, primary key(id), foreign key(customerId) references customers(id) on delete set null on update cascade, foreign key(productId) references products(id) on delete set null on update cascade)')
    conn.commit()


# The following functions are REQUIRED - you should REPLACE their implementation
# with the appropriate code to interact with your PostgreSQL database.
def initialize():
    # this function will get called once, when the application starts.
    # this would be a good place to initalize your connection!
    def connect_to_db(conn_str):
        uses_netloc.append("postgreslastName, street, city, state, zip) values (%s,%s,%s,%s,%s,%s) returning id', (customer.get(firstName), customer.get(lastName), customer.get(street), customer.get(city), customer.get(state), customer.get(zip)))")
        url = urlparse(conn_str)

        conn = psycopg2.connect(database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port)

        return conn
    global conn
    conn = connect_to_db(connection_string)
    # print(conn)
    create_tables() # if not exists
    print('Nothing to do here...')

def get_customers():
    curs = conn.cursor()
    curs.execute('select * from customers')
    customers = list()
    for customer_tuple in curs:
        customer = dict()
        customer['id'] = customer_tuple[0]
        customer['firstName'] = customer_tuple[1]
        customer['lastName'] = customer_tuple[2]
        customer['street'] = customer_tuple[3]
        customer['city'] = customer_tuple[4]
        customer['state'] = customer_tuple[5]
        customer['zip'] = customer_tuple[6]
        customers.append(customer);
    # customers = curs.fetchall()
    return customers

    # return customers

def get_customer(id):
    print('inside get customer')
    curs = conn.cursor()
    curs.execute('select * from customers where id=(%s)', (id,))
    customer_tuple = curs.fetchone()
    customer = {'id' : 'None', 'firstName' : 'None', 'lastName' : 'None', 'street' : 'None', 'city' : 'None', 'state' : 'None', 'zip' : 'None'}
    if customer_tuple == None:
        return customer
    customer['id'] = customer_tuple[0]
    customer['firstName'] = customer_tuple[1]
    customer['lastName'] = customer_tuple[2]
    customer['street'] = customer_tuple[3]
    customer['city'] = customer_tuple[4]
    customer['state'] = customer_tuple[5]
    customer['zip'] = customer_tuple[6]

    # Note: fetchone returns a tuple in this case
    print("--------------------")
    print("customer")
    print(customer)
    return customer;
    # return _find_by_id(customers, id)

def upsert_customer(customer):
    curs = conn.cursor()
    # print("------------------------------")
    # print("------------------------------")
    # print(type(customer))
    # print(customer)
    # for key,value in customer.items():
    #     print("key = (%s) and value = (%s) ", (key,value))
    # print(customer.get('firstName'))
    if 'id' in customer:
        curs.execute('update customers set firstName=(%s), lastName=(%s), street=(%s), city=(%s), state=(%s), zip=(%s) where id=(%s)', (customer.get('firstName'), customer.get('lastName'), customer.get('street'), customer.get('city'), customer.get('state'), customer.get('zip'), customer.get('id')))
    else:
        curs.execute('insert into customers(firstName, lastName, street, city, state, zip) values (%s,%s,%s,%s,%s,%s) returning id', (customer.get('firstName'), customer.get('lastName'), customer.get('street'), customer.get('city'), customer.get('state'), customer.get('zip')))
    conn.commit()
    # _upsert_by_id(customers, customer)

def delete_customer(id):
    curs = conn.cursor()
    curs.execute('delete from customers where id=(%s)', (id, ))
    conn.commit()
    # _delete_by_id(customers, id)

def get_products():
    curs = conn.cursor()
    curs.execute('select * from products')
    products = list()
    for product_tuple in curs:
        product = dict()
        product['id'] = product_tuple[0]
        product['name'] = product_tuple[1]
        product['price'] = product_tuple[2]
        products.append(product)
    # products = curs.fetchall()
    return products

def get_product(id):
    print('inside get product')
    curs = conn.cursor()
    curs.execute('select * from products where id=(%s)', (id,))
    product_tuple = curs.fetchone()
    product = {'id' : 'None', 'name' : 'None', 'price': 'None'}
    if product_tuple == None:
        return product
    product['id'] = product_tuple[0]
    product['name'] = product_tuple[1]
    product['price'] = product_tuple[2]
    print("----------------")
    print("product")
    print(product)
    return product
    # return _find_by_id(products, id)

def upsert_product(product):
    curs = conn.cursor()
    print("--------------")
    print(type(product))
    print(product)
    if 'id' in product:
        curs.execute('update products set name=(%s), price=(%s) where id=(%s)', (product.get('name'), product.get('price'), product.get('id')))
    else:
        curs.execute('insert into products(name, price) values(%s,%s) returning id',(product.get('name'), product.get('price')))
    conn.commit()
    # _upsert_by_id(products, product)

def delete_product(id):
    curs = conn.cursor()
    curs.execute('delete from products where id=(%s)',(id,))
    conn.commit()
    #_delete_by_id(products, id)

def get_orders():
    print('inside get orders')
    curs = conn.cursor()
    curs.execute('select * from orders')
    orders = list()
    for order_tuple in curs:
        order = dict()
        order['id'] = order_tuple[0]
        order['customerId'] = order_tuple[1]
        order['productId'] = order_tuple[2]
        order['date'] = order_tuple[3]
        order['product'] = get_product(order_tuple[2])
        order['customer'] = get_customer(order_tuple[1])
        orders.append(order)
        print(order)
    return orders
    # for order in orders:
    #     order['product'] = _find_by_id(products, order['productId'])
    #     order['customer'] = _find_by_id(customers, order['customerId'])
    # return orders

def get_order(id):
    curs = conn.cursor()
    curs.execute('select * from orders where id=(%s)', (id,))
    order_tuple = curs.fetchone()
    order = dict()
    order['id'] = order_tuple[0]
    order['customerId'] = order_tuple[1]
    order['productId'] = order_tuple[2]
    order['date'] = order_tuple[3]
    # might not need these two
    order['product'] = get_product(order_tuple[2])
    order['customer'] = get_customer(order_tuple[1])
    print("----------------")
    print("order")
    print(order)
    return order
    # return _find_by_id(orders, id)

def upsert_order(order):
    curs = conn.cursor()
    curs.execute('insert into orders(customerId, productId, date) values(%s,%s,%s) returning id',(order.get('customerId'), order.get('productId'), order.get('date')))
    value = curs.fetchone()
    print("order inserted id: ")
    print(value)
    conn.commit()
    # _upsert_by_id(orders, order)

def delete_order(id):
    curs = conn.cursor()
    curs.execute('delete from orders where id=(%s)', (id,))
    conn.commit()
    # _delete_by_id(orders, id)

# Return the customer, with a list of orders.  Each order should have a product
# property as well.
def customer_report(id):
    customer = get_customer(id)
    orders = get_orders()
    # customer = _find_by_id(customers, id)
    # orders = get_orders()
    customer['orders'] = [o for o in orders if o['customerId'] == id]
    return customer

# Return a list of products.  For each product, build
# create and populate a last_order_date, total_sales, and
# gross_revenue property.  Use JOIN and aggregation to avoid
# accessing the database more than once, and retrieving unnecessary
# information
def sales_report():
    #if number of orders is less than 1 than return empty dict
    #else return below
    products = get_products()
    for product in products:
        orders = [o for o in get_orders() if o['productId'] == product['id']]
        if len(orders) == 0:
            product['last_order_date'] = 'None'
            product['total_sales'] = 'None'
            product['gross_revenue'] = 'None'
            continue
        orders = sorted(orders, key=lambda k: k['date'])
        product['last_order_date'] = orders[-1]['date']
        product['total_sales'] = len(orders)
        product['gross_revenue'] = product['price'] * product['total_sales']
    return products
