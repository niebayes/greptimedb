CREATE TABLE integers (i BIGINT);

CREATE TABLE integers (i INT TIME INDEX);

CREATE TABLE integers (i BIGINT TIME INDEX NULL);

CREATE TABLE integers (i BIGINT TIME INDEX, j BIGINT, TIME INDEX(j));

CREATE TABLE integers (i BIGINT TIME INDEX, j BIGINT, TIME INDEX(i, j));

CREATE TABLE integers (i BIGINT TIME INDEX);

CREATE TABLE times (i TIMESTAMP TIME INDEX DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS integers (i BIGINT TIME INDEX);

CREATE TABLE test1 (i INTEGER, j INTEGER);

CREATE TABLE test1 (i INTEGER, j BIGINT TIME INDEX NOT NULL);

CREATE TABLE test2 (i INTEGER, j BIGINT TIME INDEX NULL);

CREATE TABLE test2 (i INTEGER, j BIGINT TIME INDEX);

DESC TABLE integers;

DESC TABLE test1;

DESC TABLE test2;

DROP TABLE integers;

DROP TABLE times;

DROP TABLE test1;

DROP TABLE test2;

-- TODO(LFC): Finish #923 in Distribute Mode, port standalone test cases.
-- TODO(LFC): Seems creating distributed table has some column schema related issues, look into "order_variable_size_payload" test cases.
