import psycopg2

con = psycopg2.connect(
     host="localhost",
     database="postgres",
     user="postgres",
     password="1234"
 )

con.autocommit = True
cur = con.cursor()

# creating DB:
cur.execute("CREATE database marketdb;")

con.commit()
cur.close()
con.close()

# ---------------------------------------------------------------------

con = psycopg2.connect(
     host="localhost",
     database="marketdb",
     user="postgres",
    password="1234"
 )

con.autocommit = True
cur = con.cursor()

cur.execute("CREATE TABLE customer("
            "c_id text NOT NULL PRIMARY KEY,"     # c_id = customer password 
            "name text NOT NULL,"
            "family text NOT NULL,"
            "address text NOT NULL)")

cur.execute("CREATE TABLE products("
            "name text NOT NULL PRIMARY KEY,"
            "count int,"
            "price NUMERIC(5,2))")

cur.execute("CREATE TABLE finished(title text)")

# fish -> purchase receipt
cur.execute("CREATE TABLE fish("
            "purchase_id serial NOT NULL PRIMARY KEY,"
            "c_id text,"
            "total_price numeric(5,2),"
            "orders text,"
            "status int,"
            "transmit_id int,"
            "exact_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

cur.execute("CREATE TABLE transmitter("
            "transmit_id int NOT NULL PRIMARY KEY,"     # transmit_id = transmitter's password
            "name text,"
            "family text,"
            "salary numeric(5,2))")

cur.execute("CREATE TABLE discount(c_id text)")

cur.execute("insert into products values('water',4,12.2)")
cur.execute("insert into products values('bread',8,8)")
cur.execute("insert into products values('milk',6,5)")
cur.execute("insert into products values('eggs',4,2)")
cur.execute("insert into products values('apple',12,2)")
cur.execute("insert into products values('toast',10,10)")
cur.execute("insert into transmitter values(1,'Hamed','Sadr','10')")
cur.execute("insert into transmitter values(2,'Hamid','Ahmadi','10')")
cur.execute("insert into transmitter values(3,'Ali','Khajavi','10')")
cur.execute("insert into transmitter values(4,'Kamran','Ilami','10')")

cur.execute("CREATE OR REPLACE PROCEDURE add_new_member(c_idn text,namen text,familyn text,addressn text) "
            "AS $$ BEGIN "
            "INSERT INTO customer(c_id,name,family,address) VALUES(c_idn,namen,familyn,addressn);"
            "END;$$ LANGUAGE PLPGSQL;")

cur.execute("CREATE OR REPLACE PROCEDURE update_fish(tr_id int) "
            "AS $$ BEGIN "
            "update fish set status=3 WHERE transmit_id=tr_id and status=1;"
            "END;$$ LANGUAGE PLPGSQL;")

cur.execute("CREATE OR REPLACE PROCEDURE get_top() "
            "AS $$ BEGIN "
            "DROP VIEW IF EXISTS top_customers;"
            "CREATE OR REPLACE VIEW top_customers AS select distinct c_id from fish where c_id in"
            "(select  c_id from(select c_id,sum(total_price) from fish group by c_id having sum(total_price)>100) as foo);"
            "END;$$ LANGUAGE PLPGSQL;")

cur.execute("CREATE OR REPLACE FUNCTION function_copy() "
            "RETURNS TRIGGER AS "
            "$BODY$ BEGIN "
            "INSERT INTO finished(title) VALUES(old.name);"
            "return new;"
            "END;$BODY$language plpgsql;")

cur.execute("CREATE OR REPLACE FUNCTION function_copy2() "
            "RETURNS TRIGGER AS "
            "$BODY$ BEGIN "
            "IF NEW.total_price>10 THEN insert into discount values(new.c_id);END IF;"
            "return new;"
            "END;$BODY$language plpgsql;")

cur.execute("CREATE OR REPLACE FUNCTION function_award() "
            "RETURNS TRIGGER AS "
            "$BODY$ BEGIN "
            "update fish set orders = CONCAT(NEW.orders, ' award:1') WHERE purchase_id = NEW.purchase_id and total_price >= 50;"
            "return new;"
            "END;$BODY$language plpgsql;")

cur.execute("CREATE OR REPLACE FUNCTION charge_product(name1 text,count1 int,price1 numeric(5,2))"
            "RETURNS void AS "
            "$$ BEGIN "
            "insert into products(name,count,price) values($1,$2,$3);"
            "DELETE FROM finished WHERE title=$1;"
            "RETURN;"
            "END;$$ LANGUAGE plpgsql;")

cur.execute("CREATE TRIGGER trig_copy "
            "AFTER DELETE ON products FOR EACH ROW EXECUTE PROCEDURE function_copy();")

cur.execute("CREATE TRIGGER trig_copy2 "
            "AFTER INSERT ON fish FOR EACH ROW EXECUTE PROCEDURE function_copy2();")

cur.execute("CREATE TRIGGER award "
            "AFTER INSERT ON fish FOR EACH ROW EXECUTE PROCEDURE function_award();")

con.commit()
cur.close()
con.close()
