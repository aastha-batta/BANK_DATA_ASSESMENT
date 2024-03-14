# Bank Statement Analyzer - Python Script

This Python script provides functionalities to clean, analyze and identify anomalies in bank statement data. It leverages pandas for data manipulation and loguru for logging.
Functionality Overview

    1) Cleans common Optical Character Recognition (OCR) errors in account numbers and extracts numeric amounts.
    2) Normalizes transaction amounts based on transaction types and codes.
    3) Sorts the data by transaction date and account number for easier analysis.
    4) Identifies individual transactions and groups monthly data, saving monthly transactions to separate files.
    5) Analyzes aggregated transaction data for discrepancies and compares subtotals with calculated totals.
    6) Groups transaction data by account number, calculates standard deviation for amount, and marks outliers as anomalies.

Prerequisites

    Python 3.9
    pandas library (pip install pandas)
    numpy library (pip install numpy)
    loguru library (pip install loguru)
    click library (pip install click)

The `config.yml` file is available for creating the environment

Usage

This script can be run as a command-line tool or imported as a module.
`python bank_statement_analyzer.py -q <query_file>`

Arguments:

    -q <query_file> (or --query_file <query_file>): Path to the CSV file containing your bank statement data.

Output:

    The script generates several CSV files:
        cleaned_data_file.csv: Contains cleaned and normalized transaction data.
        monthly_transactions_<month>.csv: Separate files for monthly transaction data
        SUBTOTAL_DESCREPENCIES_BY_MONTH.csv: Contains identified discrepancies between subtotals and calculated totals.
        ANOMALIES.csv: Contains transactions flagged as anomalies based on standard deviation calculations.
        report_logs.txt: Contains the final report of analysis
    Logs are generated during execution, providing information about the cleaning, analysis, and anomaly identification processes. These logs are typically displayed in the console by default.
CHECK THE OUTPUT FOLDER FOR RESULTS
Configuration

    The script uses the __logger class with predefined log levels for debug and info messages. You can modify these levels or add custom logging configurations by editing the script.
    The script defines a std_dev_multiplier variable (default: 2) used for identifying transaction anomalies. You can adjust this value to control the sensitivity of anomaly detection. (2 gives you about 95% accuracy)

Contributing

This script is intended to be a starting point for analyzing bank statement data. Feel free to modify and extend functionalities based on your specific needs.
