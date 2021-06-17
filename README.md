# Composer-to-load-CSV-to-BQ
Loading Cloud storage CSV data into Bigquery table through Cloud Composer.

Here the dag code will create the BQ dataset and the table to load the csv data.

## First create a storage bucket and place a CSV file.

## Create a Cloud Composer instance from console.

## Place the csv-to-bigquery.py file in the dag folder.

The dag folder is created in the cloud storage during Composer instance creation.

## Click on the Airflow link i.e. under 'Airflow webserver' tab of the Composer instance.

## You will find a new dag instance(gcs_to_bigquery_operator) for the new file placed in the cloud storage dag folder.

## Trigger dag to exeute the dag to load the csv into bigquer table.

## Check in the BQ table for the data post completion of the dag process successfully.
