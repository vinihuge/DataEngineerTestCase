from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

import csv

payment = 'data/data-payment_lookup-csv.csv'
with open(payment, mode='r') as infile:
    reader = csv.reader(infile)
    PAYMENT_DICT = {rows[0]: rows[1] for rows in reader}


TRANSFORM_QUERY = f"""
            SELECT to_timestamp(pickup_datetime) AS pickup_datetime, pickup_latitude, pickup_longitude, 
                   to_timestamp(dropoff_datetime) AS dropoff_datetime, dropoff_latitude, dropoff_longitude,
                   YEAR(to_timestamp(pickup_datetime)) AS year,
                   tip_amount, passenger_count, payment_type, total_amount, trip_distance, vendor_id
            FROM rides
            WHERE passenger_count > 0
            AND trip_distance > 0
            AND pickup_latitude <> 0
            AND pickup_longitude <> 0
            AND dropoff_latitude <> 0
            AND dropoff_longitude <> 0
            """


# Sequential ETL
class ETLJob:

    def __init__(
            self,
            spark: SparkSession,
            output_path: str = None
    ):
        if output_path is None:
            output_path = 'data/treated/data-nyc-taxi.parquet'

        self.output_path = output_path
        self.spark = spark


    @staticmethod
    def _standard_payment_type(payment_type):
        return PAYMENT_DICT.get(payment_type, payment_type)

    def extract(self):
        print('[ EXTRACT ] Initiating extract')
        rides = [f"data/data-sample_data-nyctaxi-trips-{year}-json_corrigido.json" for year in [2009, 2010, 2011, 2012]]
        rides_df = self.spark.read.json(rides)
        print('[ EXTRACT ] Finished extract')
        return rides_df

    def transform(self, rides_df):
        print('[ TRANSFORM ] Initiating transform')
        rides_df.createOrReplaceTempView("rides")

        # Remove 0 passenger trips, 0 distance trips, 0 lat/lon, and added trip_dayOfWeek
        transformed_rides = self.spark.sql(TRANSFORM_QUERY)

        # convert to a UDF Function
        udf_standard_payment_type = F.udf(self._standard_payment_type, StringType())

        # Standard Payment type
        standard_rides = transformed_rides.withColumn(
                                        "standard_payment_type",
                                        udf_standard_payment_type("payment_type")).drop('payment_type')
        print('[ TRANSFORM ] Finished transform')
        return standard_rides

    def load(self, standard_rides):
        print('[ LOAD ] Initiating load')
        # standard_rides.repartition(1).write.save(self.output_path)
        standard_rides.write.partitionBy("year")\
                      .format('parquet')\
                      .mode('overwrite')\
                      .option("header", "true")\
                      .save(self.output_path)
        print('[ LOAD ] Finished load')

    def run(self):
        self.load(self.transform(self.extract()))


if __name__ == '__main__':
    etl = ETLJob()
    etl.run()
