
--- x_pk

DROP TABLE IF EXISTS x_pk;

CREATE TABLE x_pk(
  str VARCHAR(16) CHARACTER SET utf8 COLLATE utf8_bin
);



INSERT INTO x_pk VALUES('alfa'),
                       ('bravo');

--- x_unsigned

DROP TABLE IF EXISTS x_unsigned;

CREATE TABLE x_unsigned(
  n DECIMAL(10,2) UNSIGNED
);



