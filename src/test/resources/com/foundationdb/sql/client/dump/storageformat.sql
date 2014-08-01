--- t1format

DROP TABLE IF EXISTS t1format;

CREATE TABLE t1format(
  id INT NOT NULL,
  CONSTRAINT t1format_pkey PRIMARY KEY (id),
  s VARCHAR(10) CHARACTER SET utf8 COLLATE ucs_binary
) STORAGE_FORMAT protobuf;


CREATE UNIQUE INDEX foo11 ON t1format(id) STORAGE_FORMAT tuple;


--- t2format

DROP TABLE IF EXISTS t2format;

CREATE TABLE t2format(
  id INT NOT NULL,
  CONSTRAINT t2format_pkey PRIMARY KEY (id),
  s VARCHAR(10) CHARACTER SET utf8 COLLATE ucs_binary
) STORAGE_FORMAT column_keys;


CREATE UNIQUE INDEX foo21 ON t2format(id) STORAGE_FORMAT tuple;


--- t3format

DROP TABLE IF EXISTS t3format;

CREATE TABLE t3format(
  id INT NOT NULL,
  CONSTRAINT t3format_pkey PRIMARY KEY (id),
  s VARCHAR(10) CHARACTER SET utf8 COLLATE ucs_binary
) STORAGE_FORMAT tuple;


CREATE UNIQUE INDEX foo31 ON t3format(id) STORAGE_FORMAT tuple;


