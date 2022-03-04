
# Objective

* Learn Spark with Python.
* in this project we're gonna use the real-world movie dataset from: https://grouplens.org/


# Used Technologies

1. Pyspark.
2. SparkSQL.
3. Core Python.
4. Spark standalone cluster on my local machine.



# Plan for going into it  :

1. find every pair of movies that were watched by the same person.
2. measure the similarity of their ratings across all users who watched both.
3. we set some limits that we can get confidence that the pair is surely recommendable.
4. then we take sort by movies and then by similarity strength.
5. finally, we will display the top 10 Recocords that we found.

> Note: Here, we will use Dataframes for capturing data into it because we are going to perform [massive self-join] operations.


THANK YOU!
