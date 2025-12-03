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

            
            IF v_tran_type = 'D' THEN
                v_total_debit := v_total_debit + NVL(v_amount,0);
            ELSIF v_tran_type = 'C' THEN
                v_total_credit := v_total_credit + NVL(v_amount,0);
            END IF;

            -- print row for debugging
            DBMS_OUTPUT.PUT_LINE('Row '||v_row_index||': acct='||v_account_no||', type='||v_tran_type||', amt='||v_amount);

        END LOOP;
        CLOSE c_txn_rows;

        -- output totals
        DBMS_OUTPUT.PUT_LINE('  Debit Total  = ' || v_total_debit);
        DBMS_OUTPUT.PUT_LINE('  Credit Total = ' || v_total_credit);


    END LOOP;
    CLOSE c_txns;

    DBMS_OUTPUT.PUT_LINE('Done grouping.'); 

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Unexpected block failure: ' || SQLERRM);
END;
/