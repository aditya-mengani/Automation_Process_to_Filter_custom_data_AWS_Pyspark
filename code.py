

import boto3
import time
import logging
import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession

target_key = 'XXXXXXXXX'

def main(sess):


    # define bucket information
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('XXXXXXXXXXXXXXXXXXXXXXX')

    # calculate current timestamp
    curr_timestamp = datetime.datetime.now()

    # read the existing objects into a dataframe by filtering orphaned records
    # Note: You can also read using spark dataframe.read option instead of spark sql, in that case
    # you need to use filter method and optionally a join method to join with other data sets and filter 
    # records that are not required 
    df_input = sess.query("""select * from XXXXXXX""")

    # load output to a single parquet file in the same directory
    df_input.repartition(1).write.format("parquet").mode("append").options(header="true", sep=",").save(
       "s3://XXXXXXX/prefix/folder_name")

    # delete the original file using the current timestamp calculated earlier
    for obj in bucket.objects.filter(Prefix='prefix/folder_name/'):
        # fetch the s3 object create timestamp using boto3 module
        s3_object_cre_timestamp = obj.last_modified.replace(tzinfo=None)
        # delete all the parquet files which were loaded initially before job run
        if s3_object_cre_timestamp < curr_timestamp:
            obj = s3.Object('XXXXXXXXXXXXXXXXXXXX', obj.key)
            obj.delete()
    # end of for loop

if __name__ == '__main__':
            sc =SparkContext()
            sess = SparkSession(sc)
            main(sess)

