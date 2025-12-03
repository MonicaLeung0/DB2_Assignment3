-- Inserts into NEW_TRANSACTIONS
--transaction 1
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Payment for services rendered', 1250, 'D', 30000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Payment for services rendered', 3058, 'C', 30000);

--transaction 2
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Investment purchased', 1850, 'D', 30000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Investment purchased', 1250, 'C', 30000);

--transaction 3
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Royalty revenue', 1250, 'D', 10000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Royalty revenue', 3073, 'C', 10000);

--transaction 4
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'New property purchased', 1930, 'D', 129400);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'New property purchased', 2580, 'C', 120000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'New property purchased', 1250, 'C', 9400);

--transaction 5
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Invoiced for services rendered', 1150, 'D', 1400);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Invoiced for services rendered', 3058, 'C', 1400);

--transaction 6
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Payroll processed', 4045, 'D', 50000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Payroll processed', 2050, 'C', 50000);

--transaction 7
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Monthend roll to owners equity', 3058, 'D', 31400);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Monthend roll to owners equity', 3073, 'D', 10000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Monthend roll to owners equity', 5555, 'D', 8600);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Monthend roll to owners equity', 4045, 'C', 50000);

--transaction 8
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Payment for services rendered', 1250, 'D', 40000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Payment for services rendered', 3058, 'C', 40000);

--transaction 9
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Mortgage payment', 4006, 'D', 15000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Mortgage payment', 1250, 'C', 15000);

--transaction 10
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Monthend roll to owners equity', 3058, 'D', 40000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Monthend roll to owners equity', 4006, 'C', 15000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Monthend roll to owners equity', 5555, 'C', 25000);

--transaction 11
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Payroll processed', 4045, 'D', 5000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Payroll processed', 2050, 'C', 5000);

--transaction 12
INSERT INTO new_transactions
VALUES
(wkis_seq.NEXTVAL, TRUNC(SYSDATE), 'Payroll paid', 2050, 'D', 10000);

INSERT INTO new_transactions
VALUES
(wkis_seq.CURRVAL, TRUNC(SYSDATE), 'Payroll paid', 1250, 'C', 10000);



COMMIT;

