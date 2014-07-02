DROP SEQUENCE IF EXISTS `from` RESTRICT;

CREATE SEQUENCE `from` START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 2 NO CYCLE;


--- as

DROP TABLE IF EXISTS `as`;

CREATE TABLE `as`(
  testid INT
);



INSERT INTO `as` VALUES(3);

--- max

DROP TABLE IF EXISTS `max`;

CREATE TABLE `max`(
  testid INT
);



INSERT INTO `max` VALUES(2);

--- table
---  table_child

DROP VIEW IF EXISTS `to`;
DROP GROUP IF EXISTS `table`;
DROP TABLE IF EXISTS table_child;
DROP TABLE IF EXISTS `table`;

CREATE TABLE `table`(
  `select` INT NOT NULL,
  `into` INT NOT NULL,
  CONSTRAINT table_pkey PRIMARY KEY (`select`, `into`)
);

CREATE TABLE table_child(
  `select` INT NOT NULL,
  `into` INT NOT NULL,
  GROUPING FOREIGN KEY(`select`, `into`) REFERENCES `table`(`select`, `into`)
);


CREATE INDEX `and` ON `table`(`select`);
CREATE INDEX `or` ON `table`(`select`, `into`);

CREATE VIEW `to` AS SELECT `select` AS `is` FROM `table`;

INSERT INTO `table` VALUES(1, 2);

--- values

DROP TABLE IF EXISTS `values`;

CREATE TABLE `values`(
  `min` INT NOT NULL,
  CONSTRAINT values_pkey PRIMARY KEY (`min`)
);



INSERT INTO `values` VALUES(1);

