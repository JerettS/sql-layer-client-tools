CREATE TABLE t(id INT, v VARCHAR(32));
SELECT * FROM t;
INSERT INTO t VALUES (1, 'foo'), (2, 'bar'), (3, 'zap');
SELECT * FROM t;
UPDATE t SET v='pop' WHERE id=1;
DELETE FROM t WHERE id=3;
SELECT id col1, v col2 FROM t;
