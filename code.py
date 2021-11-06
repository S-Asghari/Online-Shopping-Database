import psycopg2
import random
from beautifultable import BeautifulTable
import warnings

warnings.filterwarnings("ignore")

con = psycopg2.connect(
    host="localhost",
    database="marketdb",
    user="postgres",
    password="1234"
)
con.autocommit = True
cur = con.cursor()


def pretty_print(cur):
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    table = BeautifulTable()
    table.column_headers = column_names
    table.set_style(BeautifulTable.STYLE_BOX)
    table.column_widths = 11
    table.width_exceed_policy = BeautifulTable.WEP_STRIP
    for row in rows:
        table.append_row(row)
    print(table)
    return


def check_id(customerid):
    query = "select c_id from customer where " + "'" + customerid + "'" + " in (select c_id from customer)"
    cur.execute(query)
    leng = len(cur.fetchall())
    if leng == 0:
        return False
    else:
        return True


def selection(order, order_count):
    result1 = []
    result2 = []
    update_count = []
    result = []
    for i in range(0, len(order)):
        query = "select count from products where name='%s'" % (order[i])
        cur.execute(query)
        avail = cur.fetchall()
        if len(avail) != 0:
            available = avail[0][0]
            result1.append(order[i])
            if available >= int(order_count[i]):
                result2.append(int(order_count[i]))
                update_count.append(available - int(order_count[i]))
            # str="updating(order[i],available-int(order_count[i]))"
            else:
                result2.append(available)
                update_count.append(0)
            # str="updating(order[i],0)"
    result.append(result1)
    result.append(result2)
    result.append(update_count)
    return result


def updating(pr_name, value):
    if value == 0:
        query = "DELETE FROM products WHERE name='" + pr_name + "'"
    else:
        query = "update products set count=%s where name='%s'" % (value, pr_name)
    cur.execute(query)
    con.commit()
    return


def make_str(new_fish):
    res = ""
    for i in range(0, len(new_fish[0])):
        res += "%s:%s " % (new_fish[0][i], new_fish[1][i])
    return res


def record():
    cur.execute("select * from finished")
    rows = cur.fetchall()
    result = ""
    for r in rows:
        result += r[0] + ' '
    return result


def charge(list):
    cur.execute("SELECT charge_product('%s',%s,%s);" % (list[0], list[1], list[2]))
    con.commit()
    return


def get_purchase_id(c_id):
    cur.execute("SELECT purchase_id FROM fish where c_id = '%s' ORDER BY purchase_id DESC;" % c_id)
    list = cur.fetchall()
    return list[0][0]


def total_price(list):
    total = 0.00
    for i in range(0, len(list[0])):
        cur.execute("select price from products where name='" + list[0][i] + "'")
        cost = (cur.fetchall())[0][0]
        total += list[1][i] * float(cost)
    return total


while True:
    print("Enter your mode (customer | purchaser | salary_responsible | transmitter | manager)")
    input1 = input()
    if input1 == 'customer':
        print("Do you have an account?")
        in1 = input()
        if in1 == 'yes':
            print('Enter your password')
            in1 = input()
            identify = check_id(in1)
            if identify is True:
                print("Make order | Get transmitter status")
                choose = input()
                if choose == '1':
                    cur.execute("select * from products")
                    pretty_print(cur)
                    print("Enter your order like this pattern: milk:2,...")
                    str = input()
                    a = str.split(',')
                    order = []
                    order_count = []
                    for i in a:
                        i = i.split(':')
                        order.append(i[0])
                        order_count.append(i[1])
                    new_fish = selection(order, order_count)
                    new = make_str(new_fish)
                    total = total_price(new_fish)
                    cur.execute("select * from discount")
                    free_people = cur.fetchall()
                    for eleman in free_people:
                        if in1 == eleman[0]:
                            total = total-(10*total)/100
                            cur.execute("delete from discount where c_id='%s'" % in1)
                            break
                    for t in range(0, len(new_fish[0])):
                        updating(new_fish[0][t], new_fish[2][t])
                    query = "insert into fish(c_id, total_price, orders, status,transmit_id) values(%s, %s,'%s', %s, %s)"\
                            % (in1, total, new, 0, random.randint(1, 4))
                    cur.execute(query)
                    con.commit()
                    print("Your purchase id is %s" % get_purchase_id(in1))
                if choose == '2':
                    print("Enter your purchase id")
                    cur.execute("select status from fish where purchase_id=%s" % input())
                    print("Your transmitter status is %s" % (cur.fetchall())[0][0])
            else:
                print("Wrong password!")
        else:
            print('Enter password,name,family,address')
            in1 = input()
            infos = in1[0:len(in1)].split(",")
            try:
                cur.execute(
                    "CALL add_new_member(" + "'" + infos[0] + "'" + "," + "'" + infos[1] + "'" + "," + "'" + infos[2] + "'" + "," + "'" + infos[3] + "'" + ");")
                print("Done!")
            except Exception as e:
                print("Something went wrong...Change your password and try again.")
            finally:
                con.commit()

    if input1 == "purchaser":
        print("Enter your password")
        input1 = input()
        if input1 == '14':
            print("Charging finished products")
            status = input()
            if status == '1':
                print(record())
                print("Write elements you want to charge with its count and price like this milk 2 12,...")
                chlist = input()        # charging_list
                chlist = chlist.split(',')
                for c in chlist:
                    c = c.split(' ')
                    charge(c)
        else:
            print("Wrong password!")

    if input1 == 'salary_responsible':
        print("Enter your password")
        input1 = input()
        if input1 == '1210':
            print("Get record | Change salary")
            state = input()
            if state == '1':
                cur.execute("SELECT transmit_id,name,family,salary,purchase_id,count(status) "
                            "OVER (PARTITION BY transmit_id) "
                            "FROM transmitter INNER JOIN fish using(transmit_id) where status=1;")
                pretty_print(cur)
            if state == '2':
                print("Enter increase percentage in salary")
                increase = input()
                cur.execute("DROP VIEW IF EXISTS transporter")
                cur.execute("CREATE VIEW transporter AS "
                            "SELECT transmit_id,count(status) "
                            "from fish where status=1 group by transmit_id having count(status)>=2")
                cur.execute("select transmit_id from transporter")
                update_list = cur.fetchall()
                for l in range(0, len(update_list)):
                    cur.execute("update transmitter set salary=salary+salary*%s where transmit_id=%s"
                                % (float(increase) / 100, update_list[l][0]))
                    cur.execute("CALL update_fish(%s)" % update_list[l][0])
                    con.commit()
        else:
            print("Wrong password!")

    if input1 == 'transmitter':
        print("Enter your password")
        id = input()
        if id in ['1', '2', '3', '4']:
            print("Transmit another order | Change status")
            tr_in = input()
            if tr_in == '1':
                cur.execute("SELECT transmit_id,name,family,purchase_id,RANK() "
                            "OVER (PARTITION BY transmit_id ORDER BY purchase_id) "
                            "FROM transmitter INNER JOIN fish using(transmit_id) where transmit_id=%s and status=0;" % id)
                tr_list = cur.fetchall()
                pretty_print(cur)
                if len(tr_list) != 0:
                    cur.execute("SELECT address FROM customer inner join fish using(c_id) where purchase_id=%s" % tr_list[0][3])
                    tr_address = (cur.fetchall())[0][0]
                    print("Transmit product with purchase_id %s and address %s" % (tr_list[0][3], tr_address))
            if tr_in == '2':
                print("Enter purchase id")
                p_id = input()
                cur.execute("update fish set status=1 where purchase_id=%s" % p_id)
                con.commit()
        else:
            print("Wrong password!")

    if input1 == 'manager':
        print("Enter your password")
        input1 = input()
        if input1 == '2112':
            print("customer_transporter report | top_customers record | active_customers record | delete old fishes | daily/monthly/yearly outcome report")
            choose = input()
            if choose == '1':
                cur.execute("SELECT coalesce(c_id) AS customer_id, coalesce(transmit_id) AS transmitter_id, sum(total_price) as total_cost "
                            "FROM fish GROUP BY ROLLUP (customer_id, transmitter_id)")
                pretty_print(cur)
            if choose == '2':
                cur.execute("CALL get_top()")
                cur.execute("select name,family from customer inner join top_customers using(c_id)")
                pretty_print(cur)
            if choose == '3':
                cur.execute("select customer.c_id, customer.name, customer.family from customer, (select c_id, count(c_id) "
                            "from fish group by c_id having count(c_id)>=5) as tmp where customer.c_id=tmp.c_id;")
                pretty_print(cur)
            if choose == '4':
                cur.execute("delete from fish where status=3")
                con.commit()
            if choose == '5':
                cur.execute("select "
                            "EXTRACT(year FROM exact_time) as yyear, "
                            "EXTRACT(month FROM exact_time) as mmonth, "
                            "EXTRACT(day FROM exact_time) as dday, "
                            "sum(total_price) "
                            "from fish group by rollup (yyear, mmonth, dday) order by yyear, mmonth, dday;")
                pretty_print(cur)
        else:
            print("Wrong password!")

con.commit()
cur.close()
con.close()
