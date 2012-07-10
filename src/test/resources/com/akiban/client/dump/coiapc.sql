--- customers
---  addresses
---  orders
---   items
---   shipments

DROP TABLE IF EXISTS addresses;
DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS shipments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers(
  cid INT NOT NULL PRIMARY KEY,
  name VARCHAR(32) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL
);

CREATE TABLE addresses(
  aid INT NOT NULL PRIMARY KEY,
  cid INT NOT NULL,
  GROUPING FOREIGN KEY(cid) REFERENCES customers(cid),
  state CHAR(2) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
  city VARCHAR(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci
);

CREATE TABLE orders(
  oid INT NOT NULL PRIMARY KEY,
  cid INT NOT NULL,
  GROUPING FOREIGN KEY(cid) REFERENCES customers(cid),
  order_date DATE NOT NULL
);

CREATE TABLE items(
  iid INT NOT NULL PRIMARY KEY,
  oid INT NOT NULL,
  GROUPING FOREIGN KEY(oid) REFERENCES orders(oid),
  sku VARCHAR(32) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  quan INT NOT NULL
);

CREATE TABLE shipments(
  sid INT NOT NULL PRIMARY KEY,
  oid INT NOT NULL,
  GROUPING FOREIGN KEY(oid) REFERENCES orders(oid),
  ship_date DATE NOT NULL
);


CREATE INDEX name ON customers(name);
CREATE INDEX `__akiban_fk_2` ON addresses(cid);
CREATE INDEX state ON addresses(state);
CREATE INDEX `__akiban_fk_0` ON orders(cid);
CREATE INDEX order_date ON orders(order_date);
CREATE INDEX `__akiban_fk_1` ON items(oid);
CREATE INDEX sku ON items(sku);
CREATE INDEX cname_and_sku ON items(customers.name, sku) USING LEFT JOIN;
CREATE INDEX sku_and_date ON items(sku, orders.order_date) USING LEFT JOIN;
CREATE INDEX `__akiban_fk_3` ON shipments(oid);
CREATE INDEX ship_date ON shipments(ship_date);

INSERT INTO customers VALUES(1, 'Smith');
INSERT INTO addresses VALUES(101, 1, 'MA', 'Boston');
INSERT INTO orders VALUES(101, 1, DATE '2011-03-01');
INSERT INTO items VALUES(1, 101, '1234', 100);
INSERT INTO items VALUES(2, 101, '4567', 50);
INSERT INTO orders VALUES(102, 1, DATE '2011-03-02');
INSERT INTO customers VALUES(2, 'Jones');
INSERT INTO addresses VALUES(201, 2, 'NY', 'New York');
INSERT INTO orders VALUES(201, 2, DATE '2011-03-03');
INSERT INTO items VALUES(3, 201, '9876', 1);
INSERT INTO orders VALUES(202, 2, DATE '2011-02-28');
INSERT INTO items VALUES(4, 202, '1234', 99);
INSERT INTO customers VALUES(3, 'Adams');
INSERT INTO orders VALUES(301, 3, DATE '2011-03-04');
INSERT INTO items VALUES(5, 301, '9876', 2);
INSERT INTO orders VALUES(401, 4, DATE '2011-01-01');
INSERT INTO items VALUES(6, 401, '1234', 150);

--- parent
---  child

DROP TABLE IF EXISTS child;
DROP TABLE IF EXISTS parent;

CREATE TABLE parent(
  id INT NOT NULL PRIMARY KEY,
  name VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  state CHAR(2) CHARACTER SET utf8 COLLATE utf8_bin
);

CREATE TABLE child(
  id INT NOT NULL PRIMARY KEY,
  pid INT,
  GROUPING FOREIGN KEY(pid) REFERENCES parent(id),
  name VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL
);


CREATE INDEX name ON child(name);
CREATE INDEX pc_gi ON child(parent.name, name) USING LEFT JOIN;

