LOAN_URL = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

SCRIPTS = {
    'get_transactions': """
        SELECT cu.FIRST_NAME, cu.LAST_NAME, cr.TRANSACTION_ID, cr.TRANSACTION_TYPE,
                cr.TRANSACTION_VALUE, 
                MONTH(cr.TIMEID) AS TRANSACTION_MONTH, 
                YEAR(cr.TIMEID) AS TRANSACTION_YEAR, 
                DAY(cr.TIMEID) AS TRANSACTION_DAY
        FROM credit_data cr
        JOIN customer_data cu ON cr.CUST_SSN = cu.SSN
        WHERE cu.CUST_ZIP = %s 
            AND MONTH(cr.TIMEID) = %s 
            AND YEAR(cr.TIMEID) = %s
        ORDER BY TRANSACTION_DAY DESC;
    """,
    'prev_balance': """
        SELECT 
            COALESCE(SUM(TRANSACTION_VALUE), 0) AS PREVIOUS_BALANCE
        FROM 
            credit_data
        WHERE 
            CUST_CC_NO = %s
            AND TIMEID < %s;
    """,
    'current_transactions': """
        SELECT 
            cd.TIMEID,
            cd.TRANSACTION_TYPE,
            cd.TRANSACTION_VALUE,
            cd.BRANCH_CODE,
            cust.FIRST_NAME,
            cust.LAST_NAME
        FROM 
            credit_data cd
        JOIN 
            customer_data cust ON cd.CUST_CC_NO = cust.CREDIT_CARD_NO
        WHERE 
            cd.CUST_CC_NO = %s
            AND MONTH(cd.TIMEID) = %s
            AND YEAR(cd.TIMEID) = %s
        ORDER BY 
            cd.TIMEID;
    """,
    'timeframe_transactions': """
        SELECT * FROM credit_data
        WHERE TIMEID BETWEEN %s AND %s
        ORDER BY YEAR(TIMEID) DESC, MONTH(TIMEID) DESC, DAY(TIMEID) DESC;
    """
}