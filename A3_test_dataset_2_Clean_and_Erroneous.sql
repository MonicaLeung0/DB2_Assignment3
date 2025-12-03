-- Inserts into NEW_TRANSACTIONS
--transaction 1
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Payment for services rendered', 1250, 'D', 30000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Payment for services rendered', 3058, 'C', 30000);

--transaction 2
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Investment purchased', 1850, 'D', 30000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Investment purchased', 1250, 'C', 30000);

--transaction 3
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Royalty revenue', 1250, 'D', 10000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Royalty revenue', 3073, 'C', 10000);

--transaction 4
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'New property purchased', 1930, 'D', 129400);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'New property purchased', 2580, 'C', 120000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'New property purchased', 1250, 'C', 9400);

--transaction 5
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Invoiced for services rendered', 1150, 'D', 1400);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Invoiced for services rendered', 3058, 'C', 1400);

--transaction 6
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Payroll processed', 4045, 'D', 50000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Payroll processed', 2050, 'C', 50000);

--transaction 7
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Monthend roll to owners equity', 3058, 'D', 31400);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Monthend roll to owners equity', 3073, 'D', 10000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Monthend roll to owners equity', 5555, 'D', 8600);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Monthend roll to owners equity', 4045, 'C', 50000);

--transaction 8
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Payment for services rendered', 1250, 'D', 40000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Payment for services rendered', 3058, 'C', 40000);

--transaction 9
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Mortgage payment', 4006, 'D', 15000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Mortgage payment', 1250, 'C', 15000);

--transaction 10
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Monthend roll to owners equity', 3058, 'D', 40000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Monthend roll to owners equity', 4006, 'C', 15000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Monthend roll to owners equity', 5555, 'C', 25000);

--transaction 11
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Payroll processed', 4045, 'D', 5000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Payroll processed', 2050, 'C', 5000);

--transaction 12
INSERT INTO new_transactions
VALUES
(NULL, SYSDATE, 'Payroll processed', 4045, 'D', 5000);

INSERT INTO new_transactions
VALUES
(NULL, SYSDATE, 'Payroll processed', 2050, 'C', 5000);

--transaction 13
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Payroll paid', 2050, 'D', 5500);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Payroll paid', 1250, 'C', 5000);

--transaction 14
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Payroll paid', 2500, 'D', 400);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Payroll paid', 1250, 'C', 400);

--transaction 15
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Payroll paid', 2050, 'D', 10000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Payroll paid', 1250, 'C', 10000);

--transaction 16
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, SYSDATE, 'Royalty revenue', 1250, 'D', 4000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Royalty revenue', 1150, 'D', -1000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, SYSDATE, 'Royalty revenue', 3073, 'C', 3000);

--transaction 17
INSERT INTO new_transactions
VALUES
(50, TRUNC(SYSDATE), 'Payment for services rendered', 1250, 'D', 30000);

INSERT INTO new_transactions
VALUES
(50, TRUNC(SYSDATE), 'Payment for services rendered', 3058, 'Q', 30000);


COMMIT;
