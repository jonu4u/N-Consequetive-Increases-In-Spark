# N-Consequetive-Increases-In-Spark
Finding out N consequetive increases in a dataset.
# Use Case
Say we need to find the performance of an airline based on the delay of departure.
We want to flag those airlines which has a delay N consequetive times and say these are bad.
So maybe next time users will avoid them.
The goal of this project is to solve this use case using apache spark.

# Solution summary
The steps involves in solving this:
1) First we need to group data of a single airline in a single partition and sort them out.
2)We need to iterate inside the partition and find out N consequetive increases and raise an alarm which says these are bad.

# Solution Detail

The data we used in from the Transporatation bureau(https://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time).
We loaded the data into spark and took the following fields UNIQUE_CARRIER, DEST_AIRPORT_ID, ARR_DELAY

We used a case class which would hold these fields and also defined the implicit ordering in the case class so that the delay would be sorted by carrier and airport. This would in turn suggest that for a particular carrier and airport data the delay is sorted in ascending order.

Now define a custom partitioner to partition by carrier.
We use repartitionAndSortWithinPartitions to partition and sort the data using the case class and the custom partitioner.

Finally we use mapPartitions and iterate within the partitions and see if we find N consequetive increase in delays over multiple airports and raise alarm for them.

