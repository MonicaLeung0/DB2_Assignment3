-- Inserts into ACCOUNT_TYPE

INSERT INTO account_type
VALUES
('A', 'D', 'Asset');

INSERT INTO account_type
VALUES
('L', 'C', 'Liability');

INSERT INTO account_type
VALUES
('EX', 'D', 'Expense');

INSERT INTO account_type
VALUES
('RE', 'C', 'Revenue');

INSERT INTO account_type
VALUES
('OE', 'C', 'Owners Equity');

COMMIT;

-- Inserts into ACCOUNT
--	Assets are in the 1000 range
--	Liabilities are in the 2000 range
--	Revenues are in the 3000 range
--	Expenses are in the 4000 range
--	Owners Equity is account 5555

INSERT INTO account
VALUES
(1250, 'Cash', 'A', 0);

INSERT INTO account
VALUES
(1150, 'Accounts Receivable', 'A', 0);

INSERT INTO account
VALUES
(1850, 'Investment', 'A', 0);

INSERT INTO account
VALUES
(1930, 'Building', 'A', 0);

INSERT INTO account
VALUES
(2050, 'Accounts Payable', 'L', 0);

INSERT INTO account
VALUES
(2580, 'Mortgage', 'L', 0);

INSERT INTO account
VALUES
(4006, 'Mortgage Expense', 'EX', 0);

INSERT INTO account
VALUES
(4045, 'Payroll Expense', 'EX', 0);

INSERT INTO account
VALUES
(4078, 'Utilities Expense', 'EX', 0);

INSERT INTO account
VALUES
(3058, 'Service Revenue', 'RE', 0);

INSERT INTO account
VALUES
(3073, 'Royalty Revenue', 'RE', 0);

INSERT INTO account
VALUES
(5555, 'Owners Equity', 'OE', 0);

COMMIT;

-- Inserts into PAYROLL_PROCESSING

INSERT INTO payroll_processing
VALUES
('Y', 'Y');

COMMIT;
