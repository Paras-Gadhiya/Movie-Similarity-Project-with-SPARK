# Movie-Similarity-Project-with-SPARK


--------------------------------------------------------------------------------------------------------------------------------------------------------------------
we are going for performe distributed computation on 25 Million of Movie ratings records and near about 1.1 Gib files with all(*) CPU cores on my local system....
--------------------------------------------------------------------------------------------------------------------------------------------------------------------

in this project we're gonna use the real world movie dataset from : https://grouplens.org/
we pass the movie id with the command argument and then execute the script via spark-submit command.
we'll use "Item based colaborative filtering" formula for getting similar movies.
we also use caching concept in spark for better performance.


# [ Plan for going into it ] :

1. find every pair of movies that were watched by the same person
2. measure the similarity of their ratings across all users who watched both.
3. we set some limits that we can get confidence that pair is surely reccomendable.
4. then we take sort by movies and then by similarity strength
5. finally, we will display top 10 Recocords that we found.

Note : Here, we will use Dataframes for capturing data into it because we are going for performe [massive self join] operations.
