SET SERVEROUTPUT ON;

DECLARE
    -- Cursor and variables
    -- Outer cursor: loop distinct transactions
    CURSOR c_txns IS
        SELECT DISTINCT transaction_no,
                    transaction_date,
                    description
        FROM new_transactions
        WHERE transaction_no IS NOT NULL
        ORDER BY transaction_no;

    -- Inner cursor: loop rows by transaction group
    CURSOR c_txn_rows (p_txn_no NUMBER) IS
        SELECT transaction_no,
            transaction_date,
            description,
            account_no,
            transaction_type,
            transaction_amount
        FROM new_transactions
        WHERE transaction_no = p_txn_no
        ORDER BY ROWID;

    -- Transaction-level shared variables
    v_txn_no        new_transactions.transaction_no%TYPE;
    v_txn_date      new_transactions.transaction_date%TYPE;
    v_description   new_transactions.description%TYPE;

    -- Row-level shared variables
    v_account_no    new_transactions.account_no%TYPE;
    v_tran_type     new_transactions.transaction_type%TYPE;
    v_amount        new_transactions.transaction_amount%TYPE;

    -- Totals for validation
    v_total_debit   NUMBER := 0;
    v_total_credit  NUMBER := 0;

    -- Control / error signaling variables 
    v_error_flag    BOOLEAN := FALSE;           
    v_error_msg     VARCHAR2(200) := NULL;      

    -- Helper / counters
    v_row_index     PLS_INTEGER := 0;

    v_acct_exists NUMBER :=0;

    c_DEBIT  CONSTANT CHAR(1) := 'D';
    c_CREDIT CONSTANT CHAR(1) := 'C';

BEGIN
-- Transaction grouping
    OPEN c_txns;
    LOOP
        FETCH c_txns INTO v_txn_no, v_txn_date, v_description;
        EXIT WHEN c_txns%NOTFOUND;

        -- Reset per-transaction state
        v_error_flag   := FALSE;
        v_error_msg    := NULL;
        v_total_debit  := 0;
        v_total_credit := 0;
        v_row_index   := 0;

        DBMS_OUTPUT.PUT_LINE('--- TRANSACTION GROUP ' || v_txn_no || ' ---');

        OPEN c_txn_rows(v_txn_no);
        LOOP
            FETCH c_txn_rows INTO v_txn_no, v_txn_date, v_description,
                                v_account_no, v_tran_type, v_amount;
            EXIT WHEN c_txn_rows%NOTFOUND;

            v_row_index := v_row_index + 1;

            
            IF v_tran_type = c_DEBIT THEN
                v_total_debit := v_total_debit + NVL(v_amount,0);
            ELSIF v_tran_type = c_CREDIT THEN
                v_total_credit := v_total_credit + NVL(v_amount,0);
            END IF;

            IF v_error_flag = FALSE THEN

            -- Missing transaction number
            IF v_txn_no is null THEN
            v_error_flag := true;
            v_error_msg := 'transaction number is missing!';

            -- Invalid transaction type
            ElSIF v_tran_type not in (c_DEBIT, c_CREDIT) then 
                v_error_flag := true;
                v_error_msg := 'Invalid transaction type: ' || v_tran_type;

            -- Negative transaction amount
            elsif v_amount < 0 THEN
             v_error_flag := true;
             v_error_msg := 'Negative transaction amount: ' || v_amount;

            -- Invalid account number
            else 
              select COUNT(*) into v_acct_exists from account where account_no = v_account_no;
              if v_acct_exists = 0 then 
                v_error_flag := true;
                v_error_msg := 'Invalid account number: ' || v_account_no;

            END IF;
            END IF;

            END IF;
            -- print row for debugging
            DBMS_OUTPUT.PUT_LINE('Row '||v_row_index||': acct='||v_account_no||', type='||v_tran_type||', amt='||v_amount);

        END LOOP;
        CLOSE c_txn_rows;

        If v_error_flag = false THEN
            if v_total_debit != v_total_credit then 
                v_error_flag := true;
                v_error_msg := 'Debits ('||  v_total_debit || ') do not equal Credits (' || v_total_credit || ')';
            END IF;
            END IF;
        -- output totals
        DBMS_OUTPUT.PUT_LINE('  Debit Total  = ' || v_total_debit);
        DBMS_OUTPUT.PUT_LINE('  Credit Total = ' || v_total_credit);
        IF v_error_flag = TRUE THEN
            INSERT INTO wkis_error_log (transaction_no,
                                        transaction_date,
                                        description,
                                        error_msg)
            VALUES (v_txn_no,
                    v_txn_date,
                    v_description,
                    v_error_msg);
        END IF;

  IF v_error_flag = FALSE THEN

            INSERT INTO transaction_history
                (transaction_no, transaction_date, description)
            VALUES
                (v_txn_no, v_txn_date, v_description);

            FOR det_rec IN c_txn_rows(v_txn_no) LOOP
                INSERT INTO transaction_detail
                    (transaction_no, account_no, transaction_type, transaction_amount)
                VALUES (
                    det_rec.transaction_no,
                    det_rec.account_no,
                    det_rec.transaction_type,
                    det_rec.transaction_amount
                );

                DECLARE
                    v_default_type account_type.default_trans_type%TYPE;
                    v_balance      account.account_balance%TYPE;
                    v_newbal       account.account_balance%TYPE;
                BEGIN
                    SELECT at.default_trans_type, a.account_balance
                    INTO   v_default_type, v_balance
                    FROM   account a
                    JOIN   account_type at
                    ON     a.account_type_code = at.account_type_code
                    WHERE  a.account_no = det_rec.account_no;

                    IF v_default_type = det_rec.transaction_type THEN
                        v_newbal := v_balance + det_rec.transaction_amount;
                    ELSE
                        v_newbal := v_balance - det_rec.transaction_amount;
                    END IF;

                    UPDATE account
                    SET account_balance = v_newbal
                    WHERE account_no = det_rec.account_no;
                END;

            END LOOP;

            DELETE FROM new_transactions
            WHERE transaction_no = v_txn_no;

        END IF;

    END LOOP;
    CLOSE c_txns;

        -- Handle missing (NULL) transaction numbers
    FOR bad_rec IN (
        SELECT transaction_no,
               transaction_date,
               description
        FROM new_transactions
        WHERE transaction_no IS NULL
    ) LOOP
        INSERT INTO wkis_error_log (transaction_no,
                                    transaction_date,
                                    description,
                                    error_msg)
        VALUES (bad_rec.transaction_no,
                bad_rec.transaction_date,
                bad_rec.description,
                'Transaction number is missing.');
    END LOOP;

    COMMIT;


    DBMS_OUTPUT.PUT_LINE('Done grouping.'); 

EXCEPTION
    WHEN OTHERS THEN
        -- Capture system-generated error message
        v_error_msg := SQLERRM;

        -- Log unexpected/system errors as well
        INSERT INTO wkis_error_log (
            transaction_no,
            transaction_date,
            description,
            error_msg
        )
        VALUES (
            v_txn_no,       -- may be NULL if error occurred before first transaction
            v_txn_date,     -- same
            v_description,  -- same
            v_error_msg     -- system-generated error message
        );

        DBMS_OUTPUT.PUT_LINE('Unexpected block failure: ' || v_error_msg);
END;
/

