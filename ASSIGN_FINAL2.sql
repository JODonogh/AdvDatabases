select * from members;

select * from members where gender='female';

select * from transactions;

select * from transactions where is_cancel=0;

select * from userlogs;

select * from userlogs where NUM_50 = 0;

SELECT /*+ gather_plan_statistics */ rowid, num_25 from userlogs where num_25=2;

select registered_via from members;

/* second performance over edited .5 queries *\

CREATE INDEX members_ix ON members (msno, registration_init_time);
CREATE INDEX transactions_ix ON transactions (msno, membership_expire_date);
CREATE INDEX userlogs_ix ON userlogs (msno, total_secs);

ALTER TABLE members ADD PRIMARY KEY (msno, registration_init_time);
DELETE FROM transactions WHERE membership_expire_date IS null;
DELETE FROM transactions WHERE rowid NOT IN (SELECT MIN(rowid) FROM transactions GROUP BY msno, membership_expire_date);
ALTER TABLE transactions ADD PRIMARY KEY (msno, membership_expire_date);
DELETE FROM userlogs WHERE msno IS null;
DELETE FROM userlogs WHERE rowid NOT IN (SELECT MIN(rowid) FROM userlogs GROUP BY msno, total_secs);
ALTER TABLE userlogs ADD PRIMARY KEY (msno, total_secs);

CREATE TABLE iot_members 
(msno,
city,
bd,
gender,
registered_via,
registration_init_time, 
CONSTRAINT Pk_iot_members PRIMARY KEY(msno, registration_init_time)
)
ORGANIZATION INDEX 
PARALLEL (DEGREE 2)
AS SELECT * FROM members;

CREATE TABLE iot_transactions
(msno,payment_method_id,payment_plan_days,
plan_list_price,actual_amount_paid,
is_auto_renew,transaction_date,
membership_expire_date,is_cancel,
CONSTRAINT Pk_iot_transactions PRIMARY KEY(msno, membership_expire_date)
)
ORGANIZATION INDEX 
PARALLEL (DEGREE 2)
AS SELECT * FROM transactions;

CREATE TABLE iot_userlogs
(msno,
userlog_date,
num_25,
num_50, 
num_75,
num_985,
num_100,
num_unq,
total_secs,
CONSTRAINT Pk_iot_userlogs PRIMARY KEY(msno, total_secs)
)
ORGANIZATION INDEX 
PARALLEL (DEGREE 2)
AS SELECT * FROM userlogs;

SELECT DISTINCT registration_init_time FROM members ORDER BY registration_init_time DESC;

ALTER TABLE members MODIFY
    PARTITION BY RANGE (registration_init_time )
    ( PARTITION P1 VALUES LESS THAN (20090210),
    PARTITION P2 VALUES LESS THAN (20111108),
    PARTITION P3 VALUES LESS THAN (20140804),
    PARTITION P4 VALUES LESS THAN (20170430));
    
SELECT DISTINCT membership_expire_date FROM transactions ORDER BY membership_expire_date ASC;

ALTER TABLE transactions MODIFY
    PARTITION BY RANGE (membership_expire_date)
    ( PARTITION P1 VALUES LESS THAN(20160419),
    PARTITION P2 VALUES LESS THAN (20180628),
    PARTITION P3 VALUES LESS THAN (20191110),
    PARTITION P4 VALUES LESS THAN (20361016));

SELECT DISTINCT userlog_date FROM userlogs ORDER BY userlog_date ASC;

ALTER TABLE userlogs MODIFY
    PARTITION BY RANGE (num_50)
    ( PARTITION P1 VALUES LESS THAN(20170310),
    PARTITION P2 VALUES LESS THAN (20170320),
    PARTITION P3 VALUES LESS THAN (20170332));

ALTER TABLE userlogs INMEMORY;
ALTER TABLE transactions INMEMORY;
ALTER TABLE members INMEMORY;

/* Initial performance run through over queries *\
create index trans_date_idx on transactions(transaction_date);
create index users_date_idx on userlogs(total_secs);
create index memb_init_idx on members(registration_init_time);

drop index trans_date_idx;
drop index users_date_idx;
drop index memb_init_idx; 

ALTER TABLE members PARTITION BY LIST(gender)(PARTITION mem_male VALUES('male'), PARTITION mem_female VALUES('female'), PARTITION mem_other VALUES(DEFAULT));
ALTER TABLE userlogs PARTITION BY LIST(num_50)(PARTITION user_one VALUES(1), PARTITION user_two VALUES(2), PARTITION user_other VALUES(DEFAULT));
ALTER TABLE transactions PARTITION BY LIST(payment_method_id)(PARTITION trans_fortyone VALUES(41), PARTITION trans_thirtysix VALUES(36), PARTITION trans_other VALUES(DEFAULT));

ALTER TABLE members 
    PARTITION BY HASH(msno)
    PARTITIONS 8;
ALTER TABLE userlogs PARTITION BY HASH(msno)PARTITIONS 4;
ALTER TABLE transactions PARTITION BY HASH(msno)PARTITIONS 4;

ALTER TABLE members PARTITION BY LIST(gender)(PARTITION mem_male VALUES('male'), PARTITION mem_female VALUES('female'), PARTITION mem_other VALUES(DEFAULT));
ALTER TABLE userlogs PARTITION BY LIST(num_50)(PARTITION user_one VALUES(1), PARTITION user_two VALUES(2), PARTITION user_other VALUES(DEFAULT));
ALTER TABLE transactions PARTITION BY LIST(payment_method_id)(PARTITION trans_fortyone VALUES(41), PARTITION trans_thirtysix VALUES(36), PARTITION trans_other VALUES(DEFAULT));

ALTER TABLE members MODIFY
    PARTITION BY RANGE (registered_via)
    ( PARTITION P1 VALUES LESS THAN (5),
    PARTITION P2 VALUES LESS THAN (10),
    PARTITION P3 VALUES LESS THAN (15),
    PARTITION P4 VALUES LESS THAN (20));
    
ALTER TABLE userlogs MODIFY
    PARTITION BY RANGE (num_50)
    ( PARTITION P1 VALUES LESS THAN(300),
    PARTITION P3 VALUES LESS THAN (607),
    PARTITION P4 VALUES LESS THAN (913));

ALTER TABLE transactions MODIFY
    PARTITION BY RANGE (payment_method_id)
    ( PARTITION P1 VALUES LESS THAN(10),
    PARTITION P2 VALUES LESS THAN (20),
    PARTITION P3 VALUES LESS THAN (31),
    PARTITION P4 VALUES LESS THAN (42));
    
SELECT MAX(payment_method_id), MIN(payment_method_id)
FROM transactions;
    
DROP TABLE members;
DROP TABLE transactions;
DROP TABLE userlogs;

ALTER TABLE members PARALLEL 16;
ALTER TABLE transactions PARALLEL 16;
ALTER TABLE userlogs PARALLEL 16;

DROP TABLE members;
DROP TABLE transactions;
DROP TABLE userlogs;

DROP CLUSTER kkbox;

CREATE CLUSTER kkbox
    (msno VARCHAR2(128 BYTE));

CREATE INDEX idx_kkbox ON CLUSTER kkbox;

CREATE TABLE kkbox_members
   CLUSTER kkbox (msno)
   AS SELECT msno FROM members;

CREATE TABLE kkbox_transactions
   CLUSTER kkbox (msno)
   AS SELECT msno FROM transactions;
   
CREATE TABLE kkbox_userlogs
   CLUSTER kkbox (msno)
   AS SELECT msno FROM userlogs;

/* query 1*\
SET AUTOTRACE ON 
EXPLAIN PLAN FOR
SELECT   
    SUM(userlogs.total_secs) TOTAL_TIME, transactions.is_cancel, SUM( transactions.actual_amount_paid) TOTAL_PAID, is_auto_renew, num_100, payment_plan_days
FROM 
    userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
WHERE (transactions.is_cancel=1 AND  is_auto_renew=1) AND (total_secs>50000)
HAVING TOTAL_TIME> 90000
GROUP BY transactions.is_cancel, transactions.actual_amount_paid, is_auto_renew, num_100, payment_plan_days
SELECT * FROM
TABLE(dbms_xplan.display);
SET AUTOTRACE OFF;

/* query 1.5 + Parallel *\
SET AUTOTRACE ON 
SELECT  /*+ PARALLEL(transactions,2) PARALLEL(userlogs,2)*/ is_cancel, actual_amount_paid, is_auto_renew, plan_list_price, payment_plan_days, total_secs
FROM transactions
FULL OUTER JOIN userlogs ON userlogs.msno = transactions.msno
WHERE (transactions.is_cancel=1 AND is_auto_renew=1) and (total_secs>50000)
HAVING total_secs> 90000
GROUP BY is_cancel, is_auto_renew, plan_list_price, payment_plan_days, total_secs, num_100, actual_amount_paid;
SET AUTOTRACE OFF;

/* query 1.5 + Parallel + Materialised *\
SET AUTOTRACE ON 
CREATE MATERIALIZED VIEW qry1 AS
SELECT  /*+ PARALLEL(transactions,2) PARALLEL(userlogs,2)*/ is_cancel, actual_amount_paid, is_auto_renew, plan_list_price, payment_plan_days, total_secs
FROM transactions
FULL OUTER JOIN userlogs ON userlogs.msno = transactions.msno
WHERE (transactions.is_cancel=1 AND is_auto_renew=1) and (total_secs>50000)
HAVING total_secs> 90000
GROUP BY is_cancel, is_auto_renew, plan_list_price, payment_plan_days, total_secs, num_100, actual_amount_paid;
SET AUTOTRACE OFF;

SET AUTOTRACE ON 
SELECT * FROM qry1;
SET AUTOTRACE OFF;

/* query 1.5 IOT *\
SET AUTOTRACE ON 
SELECT is_cancel, actual_amount_paid, is_auto_renew, plan_list_price, payment_plan_days, total_secs
FROM iot_transactions
FULL OUTER JOIN iot_userlogs ON iot_userlogs.msno = iot_transactions.msno
WHERE (is_cancel=1 AND is_auto_renew=1) and (total_secs>50000)
HAVING total_secs> 90000
GROUP BY is_cancel, is_auto_renew, plan_list_price, payment_plan_days, total_secs, num_100, actual_amount_paid;
SET AUTOTRACE OFF;

/* query 2*\
SET AUTOTRACE ON 
SELECT userlogs.msno
FROM  members
INNER JOIN userlogs ON members.msno = userlogs.msno
WHERE userlogs.num_100> 1000
MINUS
SELECT transactions.msno
FROM  transactions
INNER JOIN userlogs ON transactions.msno = userlogs.msno
WHERE is_auto_renew=1;
SET AUTOTRACE OFF;

/* query 2.5 *\
SET AUTOTRACE ON 
SELECT COUNT(userlogs.msno), num_100-num_25, total_secs, userlog_date
FROM  userlogs
WHERE NOT EXISTS 
(
SELECT *
FROM members
WHERE members.msno = userlogs.msno
)
HAVING (num_100> 50 and total_secs>1000) AND (num_unq <100 ) 
GROUP BY userlog_date, num_25, num_100, total_secs, num_unq;
SET AUTOTRACE OFF;

/* query 2.5 MATERIALISED*\
SET AUTOTRACE ON 
CREATE MATERIALIZED VIEW qry2 AS SELECT COUNT(userlogs.msno), num_100-num_25, total_secs, userlog_date
FROM  userlogs
WHERE NOT EXISTS 
(
SELECT *
FROM members
WHERE members.msno = userlogs.msno
)
HAVING (num_100> 50 and total_secs>1000) AND (num_unq <100 ) 
GROUP BY userlog_date, num_25, num_100, total_secs, num_unq;
SET AUTOTRACE OFF;

SET AUTOTRACE ON 
SELECT * FROM qry2;
SET AUTOTRACE OFF;

/* query 2.5 Parallel*\
SET AUTOTRACE ON 
SELECT /*+ PARALLEL(members ,4) PARALLEL(userlogs,4)*/ COUNT(userlogs.msno), num_100-num_25, total_secs, userlog_date
FROM  userlogs
WHERE NOT EXISTS 
(
SELECT *
FROM members
WHERE members.msno = userlogs.msno
)
HAVING (num_100> 50 and total_secs>1000) AND (num_unq <100 ) 
GROUP BY userlog_date, num_25, num_100, total_secs, num_unq;
SET AUTOTRACE OFF;

/* query 2.5 IOT*\
SET AUTOTRACE ON 
SELECT COUNT(iot_userlogs.msno), num_100-num_25, total_secs, userlog_date
FROM  iot_userlogs
WHERE NOT EXISTS 
(
SELECT *
FROM iot_members
WHERE iot_members.msno = iot_userlogs.msno
)
HAVING (num_100> 50 and total_secs>1000) AND (num_unq <100 ) 
GROUP BY userlog_date, num_25, num_100, total_secs, num_unq;
SET AUTOTRACE OFF;

/* query 3*\
SET AUTOTRACE ON 
SELECT AVG(transactions.actual_amount_paid) "Average amount paid", members.city, MIN(transactions.payment_plan_days) "minimum payment plan"
FROM transactions 
INNER JOIN members ON members.msno = transactions.msno
WHERE EXISTS (SELECT COUNT(*) FROM members WHERE bd BETWEEN 20 AND 40)
GROUP BY members.city, transactions.payment_plan_days;
SET AUTOTRACE OFF; 

/* query 3- Parallel*\
SET AUTOTRACE ON 
SELECT AVG(transactions.actual_amount_paid) "Average amount paid", members.city, MIN(transactions.payment_plan_days) "minimum payment plan" /* PARALLEL(transactions,8) */
FROM transactions 
INNER JOIN members ON members.msno = transactions.msno
WHERE EXISTS (SELECT COUNT(*) FROM members WHERE bd BETWEEN 20 AND 40)
GROUP BY members.city, transactions.payment_plan_days;
SET AUTOTRACE OFF; 

/* query 3.5*\
SET AUTOTRACE ON 
SELECT payment_plan_days, city, actual_amount_paid 
FROM transactions 
FULL OUTER JOIN members ON members.msno = transactions.msno
WHERE (is_cancel = 0 AND is_auto_renew = 1) AND (payment_plan_days>30)
HAVING (bd BETWEEN 20 AND 40) AND (city BETWEEN 1 AND 15)
GROUP BY city, bd, payment_plan_days, actual_amount_paid; 
SET AUTOTRACE OFF; 

/* query 3.5 Materialised*\
SET AUTOTRACE ON 
CREATE MATERIALIZED VIEW qry3 AS
SELECT payment_plan_days, city, actual_amount_paid 
FROM transactions 
FULL OUTER JOIN members ON members.msno = transactions.msno
WHERE (is_cancel = 0 AND is_auto_renew = 1) AND (payment_plan_days>30)
HAVING (bd BETWEEN 20 AND 40) AND (city BETWEEN 1 AND 15)
GROUP BY city, bd, payment_plan_days, actual_amount_paid; 
SET AUTOTRACE OFF; 

SET AUTOTRACE ON 
SELECT * FROM qry3;
SET AUTOTRACE OFF;

/* query 3.5 Parallel*\
SET AUTOTRACE ON 
SELECT /*+ PARALLEL(members ,4) PARALLEL(transactions,4)*/ payment_plan_days, city, actual_amount_paid   
FROM transactions 
FULL OUTER JOIN members ON members.msno = transactions.msno
WHERE (is_cancel = 0 AND is_auto_renew = 1) AND (payment_plan_days>30)
HAVING (bd BETWEEN 20 AND 40) AND (city BETWEEN 1 AND 15)
GROUP BY city, bd, payment_plan_days, actual_amount_paid; 
SET AUTOTRACE OFF; 


/* query 3.5 iot*\
SET AUTOTRACE ON 
SELECT payment_plan_days, city, actual_amount_paid
FROM iot_transactions 
FULL OUTER JOIN iot_members ON iot_members.msno = iot_transactions.msno
WHERE (is_cancel = 0 AND is_auto_renew = 1) AND (payment_plan_days>30)
HAVING (bd BETWEEN 20 AND 40) AND (city BETWEEN 1 AND 15)
GROUP BY city, bd, payment_plan_days, actual_amount_paid; 
SET AUTOTRACE OFF; 


/* query 4*\
SET AUTOTRACE ON 
SELECT COUNT(members.registration_init_time) "Start", userlogs.total_secs
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
FULL OUTER JOIN members ON members.msno = userlogs.msno
WHERE members.gender NOT LIKE 'f%' 
GROUP BY userlogs.total_secs;
SET AUTOTRACE OFF; 

/* query 4- Parallel*\
SET AUTOTRACE ON 
SELECT COUNT(members.registration_init_time) "Start", userlogs.total_secs   /* PARALLEL(transactions,8) */
FROM userlogs
F JOIN transactions ON userlogs.msno = transactions.msno
FULL OUTER JOIN members ON members.msno = userlogs.msno
WHERE members.gender NOT LIKE 'f%' 
GROUP BY userlogs.total_secs;
SET AUTOTRACE OFF; 

/* query 4.5*\
SET AUTOTRACE ON 
SELECT registration_init_time, membership_expire_date,  payment_method_id,  (num_25*num_50*num_75*num_100), 
SUM(total_secs) OVER (
PARTITION BY userlog_date ORDER BY userlog_date
)
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msno
WHERE (members.gender NOT LIKE 'f%' AND bd <30 ) AND (num_100>100)
GROUP BY userlogs.total_secs, registration_init_time, membership_expire_date, payment_method_id, num_25, num_50, num_75, num_100, userlog_date;
SET AUTOTRACE OFF; 

/* query 4.5 MATERIALISED*\
SET AUTOTRACE ON 
CREATE MATERIALIZED VIEW qry4 AS SELECT registration_init_time, membership_expire_date,  payment_method_id,  (num_25*num_50*num_75*num_100), 
SUM(total_secs) OVER (
PARTITION BY userlog_date ORDER BY userlog_date
)
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msno
WHERE (members.gender NOT LIKE 'f%' AND bd <30 ) AND (num_100>100)
GROUP BY userlogs.total_secs, registration_init_time, membership_expire_date, payment_method_id, num_25, num_50, num_75, num_100, userlog_date;
SET AUTOTRACE OFF; 

SET AUTOTRACE ON 
SELECT * FROM qry4;
SET AUTOTRACE OFF;

/* query 4.5 Parallel*\
SET AUTOTRACE ON 
SELECT /*+ PARALLEL(userlogs ,2) PARALLEL(transactions,2) PARALLEL(members,2) */ 
    registration_init_time, 
    membership_expire_date, 
    payment_method_id,  (num_25*num_50*num_75*num_100),
SUM(total_secs) OVER (
PARTITION BY userlog_date ORDER BY userlog_date
)
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msno
WHERE (members.gender NOT LIKE 'f%' AND bd <30 ) AND (num_100>100)
GROUP BY userlogs.total_secs, registration_init_time, membership_expire_date, payment_method_id, num_25, num_50, num_75, num_100, userlog_date;
SET AUTOTRACE OFF; 

/* query 4.5 iot*\
SET AUTOTRACE ON 
SELECT registration_init_time, membership_expire_date,  payment_method_id,  (num_25*num_50*num_75*num_100),
SUM(total_secs) OVER (
PARTITION BY userlog_date ORDER BY userlog_date
)
FROM iot_userlogs
INNER JOIN iot_transactions ON iot_userlogs.msno = iot_transactions.msno
INNER JOIN iot_members ON iot_members.msno = iot_userlogs.msno
WHERE (iot_members.gender NOT LIKE 'f%' AND bd <30 ) AND (num_100>100)
GROUP BY iot_userlogs.total_secs, registration_init_time, membership_expire_date, payment_method_id, num_25, num_50, num_75, num_100, userlog_date;
SET AUTOTRACE OFF; 

/* query 5*\
SET AUTOTRACE ON 
SELECT DISTINCT members.gender, AVG(transactions.actual_amount_paid) AS "Average amount paid", userlogs.num_unq
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msno
WHERE members.gender IS NOT NULL
GROUP BY userlogs.num_unq,  members.gender
ORDER BY members.gender;
SET AUTOTRACE OFF; 

/* query 5.5*\
SET AUTOTRACE ON 
SELECT DISTINCT 
num_unq, 
gender, 
actual_amount_paid, 
city, 
plan_list_price, 
is_cancel,
RANK() OVER (ORDER BY actual_amount_paid DESC)
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msno
WHERE (members.gender IS NOT NULL AND bd>40) AND (is_cancel = 1)
GROUP BY num_unq, gender, actual_amount_paid, city, plan_list_price, is_cancel
ORDER BY gender;
SET AUTOTRACE OFF; 

/* query 5.5 Parallel*\
SET AUTOTRACE ON 
SELECT /*+ PARALLEL(userlogs 2) PARALLEL(transactions 2) PARALLEL(members 2) */ 
DISTINCT num_unq, 
gender, 
actual_amount_paid, 
city, 
plan_list_price, 
is_cancel,
RANK() OVER (ORDER BY actual_amount_paid DESC)
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msno
WHERE (members.gender IS NOT NULL AND bd>40) AND (is_cancel = 1)
GROUP BY num_unq, gender, actual_amount_paid, city, plan_list_price, is_cancel
ORDER BY gender;
SET AUTOTRACE OFF; 

/* query 5.5 Parallel & Materiliased*\
SET AUTOTRACE ON 
CREATE MATERIALIZED VIEW qry5 AS SELECT /*+ PARALLEL(userlogs 2) PARALLEL(transactions 2) PARALLEL(members 2) */ 
DISTINCT num_unq, 
gender, 
actual_amount_paid, 
city, 
plan_list_price, 
is_cancel,
RANK() OVER (ORDER BY actual_amount_paid DESC)
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msno
WHERE (members.gender IS NOT NULL AND bd>40) AND (is_cancel = 1)
GROUP BY num_unq, gender, actual_amount_paid, city, plan_list_price, is_cancel
ORDER BY gender;
SET AUTOTRACE OFF; 

SET AUTOTRACE ON 
SELECT * FROM qry5;
SET AUTOTRACE OFF;

/* query 5.5 iot*\
SET AUTOTRACE ON 
SELECT DISTINCT 
num_unq, 
gender, 
actual_amount_paid, 
city, 
plan_list_price, 
is_cancel,
RANK() OVER (ORDER BY actual_amount_paid DESC)
FROM iot_userlogs
INNER JOIN iot_transactions ON iot_userlogs.msno = iot_transactions.msno
INNER JOIN iot_members ON iot_members.msno = iot_userlogs.msno
WHERE (iot_members.gender IS NOT NULL AND bd>40) AND (is_cancel = 1)
GROUP BY num_unq, gender, actual_amount_paid, city, plan_list_price, is_cancel
ORDER BY gender;
SET AUTOTRACE OFF; 

/* query 6*\
SELECT members.city, MAX(userlogs.total_secs) "Max", transactions.is_auto_renew
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msn
WHERE userlogs.total_secs= Max;

/* query 7*\
SELECT IQR(bd) "Age", transactions.is_cancel, AVG(userlogs.total_secs) AS "Avg_song"
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msn
WHERE userlogs.total_secs > Avg_song;

/* query 8*\
SELECT COUNT(members.gender= "male") "Male", COUNT(members.gender= "female") "Female", AVG(transactions.actual_amount_paid) AS "Avg_pay", AVG(userlogs.total_secs) AS "Avg_song",
FROM userlogs
INNER JOIN transactions ON userlogs.msno = transactions.msno
INNER JOIN members ON members.msno = userlogs.msn
WHERE Male> Female AND userlogs.total_secs> Avg_song AND transactions.actual_amount_paid>Avg_pay;

