# Simple ETL Pipeline with Logging

## Overview
This document outlines a simple Extract, Transform, Load (ETL) pipeline without using any specific ETL framework. The pipeline extracts data from JSON, CSV, and XML formats, transforms the data, and loads it into a CSV file. Logging is implemented to record the time each activity in the pipeline takes place.

## ETL Pipeline Steps

### 1. Extraction
- **Source Data Formats:**
  - JSON
  - CSV
  - XML

- **Extraction Process:**
  - Extract data from JSON file.
  - Extract data from CSV file.
  - Extract data from XML file.

- **Logging:**
  - Record the start time of each extraction process.
  - Record the end time of each extraction process.

### 2. Transformation
- **Transformation Process:**
  - Apply necessary transformations to the extracted data.
  - Example transformations:
    - Data cleansing
    - Data enrichment
    - Data restructuring

- **Logging:**
  - Record the start time of the transformation process.
  - Record the end time of the transformation process.

### 3. Loading
- **Destination:**
  - CSV File

- **Loading Process:**
  - Load the transformed data into a CSV file.

- **Logging:**
  - Record the start time of the loading process.
  - Record the end time of the loading process.

## Logging Details
- Logging is implemented to capture the start and end times of each phase in the ETL pipeline.
- Log entries are stored for future reference.

## Conclusion
This simple ETL pipeline extracts data from various formats, applies transformations, and loads the transformed data into a CSV file. Logging is incorporated to provide visibility into the timing of each activity within the pipeline.
