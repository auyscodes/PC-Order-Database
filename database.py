import psycopg2
import csv
from urllib.parse import urlparse, uses_netloc
import configparser


config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']
conn = None


def create_tables():
    curs = conn.cursor()
    curs.execute('create table if not exists customers(id serial, firstName text, lastName text, street text, city text, state text, zip text, primary key(id))')
    curs.execute('create table if not exists products(id serial, name text, price real, primary key(id))')
    curs.execute('create table if not exists orders(id serial, customerId int, productId int, date text, primary key(id), foreign key(customerId) references customers(id) on delete set null on update cascade, foreign key(productId) references products(id) on delete set null on update cascade)')
    conn.commit()


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
    create_tables() # if not exists

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
    return customers

    # return customers

def get_customer(id):
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
    return customer;
    # return _find_by_id(customers, id)

def upsert_customer(customer):
    curs = conn.cursor()
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
    return products

def get_product(id):
    curs = conn.cursor()
    curs.execute('select * from products where id=(%s)', (id,))
    product_tuple = curs.fetchone()
    product = {'id' : 'None', 'name' : 'None', 'price': 'None'}
    if product_tuple == None:
        return product
    product['id'] = product_tuple[0]
    product['name'] = product_tuple[1]
    product['price'] = product_tuple[2]
    return product
    # return _find_by_id(products, id)

def upsert_product(product):
    curs = conn.cursor()
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
    return orders

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
    return order
    # return _find_by_id(orders, id)

def upsert_order(order):
    curs = conn.cursor()
    curs.execute('insert into orders(customerId, productId, date) values(%s,%s,%s) returning id',(order.get('customerId'), order.get('productId'), order.get('date')))
    value = curs.fetchone()
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
    curs = conn.cursor()
    curs.execute('select name, avg(price)*count(products.id),  count(orders.productId), max(date) from products left join orders on orders.productId = products.id group by products.id')
    reports = list()
    for report_tuple in curs:
        report = dict()
        # report = {'name' : 'None', 'gross_revenue' : 'None', 'total_sales' : 'None', 'last_order_date' : 'None'}
        report['name'] = report_tuple[0]
        report['gross_revenue'] = report_tuple[1]
        report['total_sales'] = report_tuple[2]
        report['last_order_date'] = report_tuple[3]
        reports.append(report)
    return reports
    # products = get_products()
    # for product in products:
    #     orders = [o for o in get_orders() if o['productId'] == product['id']]
    #     if len(orders) == 0:
    #         product['last_order_date'] = 'None'
    #         product['total_sales'] = 'None'
    #         product['gross_revenue'] = 'None'
    #         continue
    #     orders = sorted(orders, key=lambda k: k['date'])
    #     product['last_order_date'] = orders[-1]['date']
    #     product['total_sales'] = len(orders)
    #     product['gross_revenue'] = product['price'] * product['total_sales']
    # return products
