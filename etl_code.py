import pandas as pd
import glob
from datetime import datetime
import xml.etree.ElementTree as ET

# file location to store log files and transformed data
log_file = 'log_file.txt'
destination_file = 'transformed_data.csv'


# To extract data from csv
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


# To extract data from json
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe


# To extract data from xml
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(
        columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for row in root:
        car_model = row.find('car_model').text
        year_of_manufacture = int(row.find('year_of_manufacture').text)
        price = float(row.find('price').text)
        fuel = row.find('fuel').text
        dataframe = pd.concat([dataframe, pd.DataFrame(
            [{'car_model': car_model, 'year_of_manufacture': year_of_manufacture, 'price': price, 'fuel': fuel}])],
                              ignore_index=True)
    return dataframe


# function to call and create an empty dataframe
def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    # function to call all csv
    for csvfile in glob.glob('*.csv'):
        try:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)
        except pd.errors.EmptyDataError:
            print(f"Warning: {csvfile} is empty.")

    # function to call all json
    for jsonfile in glob.glob('*.json'):
        try:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)
        except pd.errors.EmptyDataError:
            print(f"Warning: {jsonfile} is empty.")

    # function to call all xml
    for xml_file in glob.glob('*.xml'):
        try:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xml_file))], ignore_index=True)
        except pd.errors.EmptyDataError:
            print(f"Warning: {xml_file} is empty.")

    return extracted_data


# transform data
def transform(root):
    root['price'] = round(root.price, 2)
    return root


# loading data
def load_data(destination_file, transformed_data):
    transformed_data.to_csv(destination_file)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

    # Log the initialization of the ETL process


log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(destination_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")
