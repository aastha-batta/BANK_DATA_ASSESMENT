import pandas as pd
import numpy as np
from loguru import logger
import sys,click


LOG_LEVEL='DEBUG'
LOG_LEVEL_FILE="INFO"
FILE='report_log.txt'

class __logger:
    """
    __logger.

    Attributes:
    - logger: Loguru logger instance.
    """
    def __init__(self):
        self.logger = logger
        self.logger.remove()

        self.logger.add(sys.stdout, format="{time: YYYY:MM:DD  HH:mm:ss zz} | {level} | {module}:{function}:{line} - {message}", level=LOG_LEVEL)
        self.logger.add(FILE, format="{time: YYYY:MM:DD  HH:mm:ss:zz} | {level} | {module}:{function}:{line} - {message}", level=LOG_LEVEL_FILE)

logging = __logger().logger


def handle_parsing_error(row):
  """
  Handles a parsing error for a single row in the DataFrame.

  Args:
      row (pandas.Series): The row containing the parsing error.

  Raises:
      ValueError: If there's an error parsing the 'Transaction Date' column.

  Returns:
      None: Returns None to indicate an error occurred.
  """
  row['Transaction Date'] = 'NA'
  raise ValueError(f"Error parsing date: {row['Transaction Date']}")
  return None


def normalize_row(row,transaction_codes):
  """
  Normalizes transaction amounts based on transaction types and codes.

  Args:
      row (pandas.Series): A single row of transaction data.
      transaction_codes (dict): A dictionary mapping transaction types to codes.

  Returns:
      pandas.Series: The normalized row with updated transaction codes and amounts.
  """

  row['Transaction Code'] = transaction_codes.get(row['Transaction Type'])
  
  #Normalise Withdrawal
  if row['Transaction Code'] in [3]:  
    row['Amount'] *= -1 if row['Amount'] > 0 else 1
  
  #Normalise Deposit
  if row['Transaction Code'] in [4]:  
    row['Amount'] *= -1 if row['Amount'] < 0 else 1
  
  
  return row


def clean_ocr_errors(data):
  
  """
  Cleans common OCR-like errors in account numbers and extracts numeric amounts.

  Args:
      data (pandas.DataFrame): A DataFrame containing transaction data.

  Returns:
      pandas.DataFrame: The DataFrame with cleaned account numbers and numeric amounts.
  """
  
  # Correct OCR-like errors in account numbers
  data['Account Number'] = data['Account Number'].str.replace('l', '1').str.replace('O', '0')

  # Extract numeric amount (including negative sign)
  data['Amount'] = data['Amount'].str.extract('([\-]{0,1}\d+)')
  data['Amount'] = pd.to_numeric(data['Amount'])

  logging.debug("OCR errors present in Account number has been cleaned")

  return data
  

def clean_and_normalize_data(data):
  """
  Cleans and normalizes transaction data in a DataFrame.

  Args:
      data (pandas.DataFrame): A DataFrame containing transaction data.

  Returns:
      pandas.DataFrame: A DataFrame with cleaned and normalized data, sorted by date and account number.
  """

  transaction_codes = {
    'Online Transfer': 1,
    'Card Payment': 2,
    'ATM Withdrawal': 3,
    'Direct Debit': 3,
    'Deposit': 4,
    'Withdrawal': 3
  }

  cleaned_data=clean_ocr_errors(data)

  # Apply normalization to each row
  cleaned_data = cleaned_data.apply(normalize_row, axis=1, args=(transaction_codes,))

  logging.debug("The data has been normaised based on Deposit and Withdrawal")
  # Sort the DataFrame by 'Transaction Date' in ascending order (oldest first)
  # and then by 'Account Number' in ascending order
  cleaned_data['Transaction Date'] = pd.to_datetime(cleaned_data['Transaction Date'])
  cleaned_data['Account Number'] = cleaned_data['Account Number'].astype(str)
  cleaned_data = cleaned_data.sort_values(by=['Transaction Date', 'Account Number'])
  logging.debug("The data is sorted date wise")

  cleaned_data_file="cleaned_data_file.csv"

  logging.info(f'The OCR errors have been cleaned and the data has been normalised on the basis of Deposit and Withdrawal, the cleaned data is saved in {cleaned_data_file}')
  return cleaned_data

def identify_individual_transactions(cleaned_data):
  """
  Identifies individual transactions and groups monthly data, saving monthly transactions to separate files.

  Args:
      cleaned_data (pandas.DataFrame): A DataFrame with cleaned and normalized transaction data.

  Returns:
      pandas.DataFrameGroupBy: A DataFrameGroupBy object grouping data by month.
  """

  
  monthly_data = cleaned_data.groupby(cleaned_data['Transaction Date'].dt.month)
  logging.debug("The data grouping is complete (based on month)")
  
  #Save data in different files
  filtered_data = cleaned_data[~cleaned_data['Account Number'].str.contains('SUBT0TAL|YEARLY', case=True)]
  monthly_individual_data = filtered_data.groupby(filtered_data['Transaction Date'].dt.month)

  logging.info('The identification of individual transactions is complete')

  base_filename = 'monthly_transactions_'
  base_path = './'  

  # Save each monthly DataFrame to a separate CSV file
  for month, group_data in monthly_individual_data:
    filename = f"{base_filename}{month}.csv"
    filepath = f"{base_path}{filename}"
    group_data.to_csv(filepath, index=False)

  return monthly_data


def analyse_aggregated_data(cleaned_data,grouped_data):
  """
  Analyzes aggregated transaction data for discrepancies and compares subtotals with calculated totals.

  Args:
      cleaned_data (pandas.DataFrame): A DataFrame with cleaned and normalized transaction data.
      grouped_data (pandas.DataFrameGroupBy): A DataFrameGroupBy object grouping data by month.

  Returns:
      None: Analyzes data for discrepancies and logs findings, but does not return a value.
  """

  # Calculate the total amount
  monthly_totals = grouped_data['Amount'].sum()

  logging.debug('Calculation of subtotal is complete')
  
  #Find discrepencies based on subtotal matching
  discrepancies = pd.DataFrame(columns=['Month', 'Subtotal', 'Calculated Total'])

  #Calculate monthly total and check if it is equal to the subtotal of that month
  for index, row in cleaned_data[cleaned_data['Account Number'] == 'SUBT0TAL'].iterrows():
    month = row['Transaction Date'].month
    subtotal = row['Amount']
    calculated_total = monthly_totals.get(month)
    if calculated_total is not None and calculated_total != subtotal:
      new_row = pd.DataFrame({'Month': [month], 'Subtotal': [subtotal], 'Calculated Total': [calculated_total]})
      discrepancies = pd.concat([discrepancies, new_row], ignore_index=True)
  
  #Save the subtotal discrepencies
  file_discrepency="SUBTOTAL_DESCREPENCIES_BY_MONTH.csv"
  discrepancies.to_csv(file_discrepency,index=None)
  
  if not discrepancies.empty:
    logging.info(f"Discrepencies are found and are saved in {file_discrepency}")
  else:
    logging.info("No discrepancies found")

  logging.debug("data discrepencies have been logged")

  #check if yearly total = sum of subtotals 
  yearly_total_exists = cleaned_data['Account Number'].str.contains('YEARLY T0TAL').any()

  if yearly_total_exists:
    # Get the 'Amount' value from the row with 'Account Number' as 'YEARLY TOTAL'
    yearly_total = cleaned_data[cleaned_data['Account Number'] == 'YEARLY T0TAL']['Amount'].iloc[0]

  subtotal_data = cleaned_data[cleaned_data['Account Number'] == 'SUBT0TAL']
  total_subtotals = subtotal_data['Amount'].sum()
  
  if total_subtotals == yearly_total:
      logging.info("Since the yearly total is equal to sum of subtotals, there might be some missing data values.The reliability of data should be checked")
  else:
      logging.info("The data is unreliable")


def identify_transaction_anomalies(cleaned_data, std_dev_multiplier=2):
  """
  Groups transaction data by account number, calculates standard deviation for amount, 
  and marks outliers as anomalies.

  Args:
    cleaned_data: A Pandas DataFrame containing cleaned and normalized transaction data.
    std_dev_multiplier: A multiplier for the standard deviation to define outliers (default: 3).

  Returns:
    A Pandas DataFrame with a new column 'Anomaly' indicating flagged transactions.
  """

  
  flagged_data=cleaned_data.copy()
  # Group by account number
  grouped_data = flagged_data.groupby('Account Number')

  # Define a function to calculate anomaly flag
  def calculate_anomaly(group):
    mean = group['Amount'].mean()
    std_dev = group['Amount'].std()
    group['Anomaly'] = (group['Amount'] < (mean - std_dev_multiplier * std_dev)) | (group['Amount'] > (mean + std_dev_multiplier * std_dev))
    return group

  # Apply the function to each group
  flagged_data = grouped_data.apply(calculate_anomaly)

  logging.debug("Flagging the anomalies")

  flagged_data = flagged_data[flagged_data['Anomaly'] == True].drop("Transaction Code",axis=1)
  flagged_data_file="ANOMALIES.csv"
  logging.debug(f"Anomalies have been flagged")
  logging.info(f"The anomalies identified from data have been saved in {flagged_data_file}")

  flagged_data.to_csv(flagged_data_file,index=None)
  

@click.group()
@click.help_option('-h','--help')
def cli_module():
	pass
 
@cli_module.command(context_settings=dict(max_content_width=102))
@click.help_option('-h', '--help')
@click.option(
    '--query_file',
    '-q',
    type=click.Path(exists=True, file_okay=True),
    help='Path to the query file.'
)
def analyse_bank_data(query_file):
  data=pd.read_csv(query_file)
  cleaned_data = clean_and_normalize_data(data.copy())
  grouped_data=identify_individual_transactions(cleaned_data)
  analyse_aggregated_data(cleaned_data,grouped_data)
  identify_transaction_anomalies(cleaned_data)



if __name__ == "__main__":
    if len(sys.argv) == 1:
        cli_module.main(['--help','-h'])
        
    else:
        cli_module()
