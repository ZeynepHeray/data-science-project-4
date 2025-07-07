import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = 'postgres'

def connect_db():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password=password
    )

def create_view_completed_orders():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""Create or replace view "completed" as (Select * from cw4.orders as o where o.status ='completed')""")
            conn.commit()

def create_view_electronics_products():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""Create or replace view  "electronics" as(Select * from cw4.products as p where p.category ='Electronics')""")
            conn.commit()

def total_spending_per_customer():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("with total as (select *  from cw4.orders as o join cw4.products as p on o.product_id = p.product_id ) select customer_id, sum(price) from total group by customer_id")
            return cur.fetchall()

def order_details_with_total():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("with total as (select *, (o.quantity*p.price) as total_price  from cw4.orders as o join cw4.products as p on o.product_id = p.product_id ) select * from total ")
            return cur.fetchall()

def get_customer_who_bought_most_expensive_product():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("select c.full_name  from cw4.customers as c where customer_id= (select o.customer_id from cw4.orders as o join cw4.products as p on o.product_id = p.product_id order by p.price desc limit 1)")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 2. Sipariş durumlarına göre açıklama
def get_order_status_descriptions():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""select o.order_id, o.status , case when o.status ='completed' then 'Tamamlandı' when o.status = 'cancelled' then 'İptal Edildi' else 'Bilinmiyor' end  as "status_description"  from cw4.orders as o""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 3. Ortalama fiyatın üstündeki ürünler
def get_products_above_average_price():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""select p.product_name, p.price from cw4.products as p where p.price > (select avg(p.price) as ort from cw4.products as p ) """)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 4. Müşteri kategorileri
def get_customer_categories():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""select c.full_name ,
                case
	                when (select count(o.order_id) from cw4.orders as o) >5 then 'Sadık Müşteri'
	                when (select count(o.order_id) from cw4.orders as o )<2  then 'Yeni Müşteri'
	                else 'Orta Seviye'
                end  as "customer_category" 
                from cw4.customers as c""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 5. Son 30 gün içinde sipariş veren müşteriler
def get_recent_customers():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""select c.full_name from cw4.customers as c join cw4.orders as o on c.customer_id = o.customer_id where o.order_date > '2025-06-07 '""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 6. En çok sipariş verilen ürün
def get_most_ordered_product():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT p.product_name, COUNT(*) AS order_count
                FROM cw4.orders AS o
                JOIN cw4.products AS p ON o.product_id = p.product_id
                GROUP BY p.product_name
                ORDER BY order_count DESC
                LIMIT 1 """)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 7. Ürün fiyatlarına göre etiketleme
def get_product_price_categories():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""select p.product_name , p.price,
                    case
	                    when p.price >1000 then 'Pahalı'
	                    when p.price<100  then 'Ucuz'
	                    else 'Orta'
                    end  as "price_category" 
                    from cw4.products as p""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result