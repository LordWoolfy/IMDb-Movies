from pyspark.sql.types import *
from pyspark.sql.functions import udf

def cleanse(path,columns,x):
  """This function cleanses data. For 'path' you should insert the path to the file you want.
  For 'columns' insert the columns as a list ['abc','dfe'] you want deleted from the Data Frame.
  x is for which schema is used for the csv file. 0 for Movies, 1 for Names, 2 for Ratings, 3 for Title principals"""
  schemas = [StructType([
        StructField("imdb_title_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("date_published", TimestampType(), True),
        StructField("genre", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("language", StringType(), True),
        StructField("director", StringType(), True),
        StructField("writer", StringType(), True),
        StructField("production_company", StringType(), True),
        StructField("actors", StringType(), True),
        StructField("description", StringType(), True),
        StructField("avg_vote", FloatType(), True),
        StructField("votes", FloatType(), True),
        StructField("budget", StringType(), True),
        StructField("usa_gross_income", StringType(), True),
        StructField("worlwide_gross_income", StringType(), True),
        StructField("metascore", IntegerType(), True),
        StructField("reviews_from_users", IntegerType(), True),
        StructField("reviews_from_critics", IntegerType(), True)
    ]), StructType([
        StructField("imdb_name_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("birth_name", StringType(), True),
        StructField("height", IntegerType(), True),
        StructField("bio", StringType(), True),
        StructField("birth_details", StringType(), True),
        StructField("date_of_birth", TimestampType(), True),
        StructField("place_of_birth", StringType(), True),
        StructField("death_details", StringType(), True),
        StructField("date_of_death", TimestampType(), True),
        StructField("place_of_death", StringType(), True),
        StructField("reason_of_death", StringType(), True),
        StructField("spouses_string", StringType(), True),
        StructField("spouses", IntegerType(), True),
        StructField("divorces", IntegerType(), True),
        StructField("spouses_with_children", IntegerType(), True),
        StructField("children", IntegerType(), True)
    ]), StructType([
        StructField("imdb_title_id", StringType(), True),
        StructField("weighted_average_vote", FloatType(), True),
        StructField("total_votes", IntegerType(), True),
        StructField("mean_vote", FloatType(), True),
        StructField("median_vote", IntegerType(), True),
        StructField("votes_10", IntegerType(), True),
        StructField("votes_9", IntegerType(), True),
        StructField("votes_8", IntegerType(), True),
        StructField("votes_7", IntegerType(), True),
        StructField("votes_6", IntegerType(), True),
        StructField("votes_5", IntegerType(), True),
        StructField("votes_4", IntegerType(), True),
        StructField("votes_3", IntegerType(), True),
        StructField("votes_2", IntegerType(), True),
        StructField("votes_1", IntegerType(), True),
        StructField("allgenders_0age_avg_vote", FloatType(), True),
        StructField("allgenders_0age_votes", FloatType(), True),
        StructField("allgenders_18age_avg_vote", FloatType(), True),
        StructField("allgenders_18age_votes", FloatType(), True),
        StructField("allgenders_30age_avg_vote", FloatType(), True),
        StructField("allgenders_30age_votes", FloatType(), True),
        StructField("allgenders_45age_avg_vote", FloatType(), True),
        StructField("allgenders_45age_votes", FloatType(), True),
        StructField("males_allages_avg_vote", FloatType(), True),
        StructField("males_allages_votes", FloatType(), True),
        StructField("males_0age_avg_vote", FloatType(), True),
        StructField("males_0age_votes", FloatType(), True),
        StructField("males_18age_avg_vote", FloatType(), True),
        StructField("males_18age_votes", FloatType(), True),
        StructField("males_30age_avg_vote", FloatType(), True),
        StructField("males_30age_votes", FloatType(), True),
        StructField("males_45age_avg_vote", FloatType(), True),
        StructField("males_45age_votes", FloatType(), True),
        StructField("females_allages_avg_vote", FloatType(), True),
        StructField("females_allages_votes", FloatType(), True),
        StructField("females_0age_avg_vote", FloatType(), True),
        StructField("females_0age_votes", FloatType(), True),
        StructField("females_18age_avg_vote", FloatType(), True),
        StructField("females_18age_votes", FloatType(), True),
        StructField("females_30age_avg_vote", FloatType(), True),
        StructField("females_30age_votes", FloatType(), True),
        StructField("females_45age_avg_vote", FloatType(), True),
        StructField("females_45age_votes", FloatType(), True),
        StructField("top1000_voters_rating", FloatType(), True),
        StructField("top1000_voters_votes", FloatType(), True),
        StructField("us_voters_rating", FloatType(), True),
        StructField("us_voters_votes", FloatType(), True),
        StructField("non_us_voters_rating", FloatType(), True),
        StructField("non_us_voters_votes", FloatType(), True)
    ]), StructType([
        StructField("imdb_title_id", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("imdb_name_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ])]


  spdf = spark.read.csv(path, multiLine = True, escape = '"', header = True, schema=schemas[x])
  # Change to characters to array type
  if x == 0:
    spdf = spdf.withColumn("genre", parse_arraystr_udf("genre"))
    spdf = spdf.withColumn("country", parse_arraystr_udf("country"))
    spdf = spdf.withColumn("language", parse_arraystr_udf("language"))
    spdf = spdf.withColumn("director", parse_arraystr_udf("director"))
    spdf = spdf.withColumn("writer", parse_arraystr_udf("writer"))
    spdf = spdf.withColumn("production_company", parse_arraystr_udf("production_company"))
    spdf = spdf.withColumn("actors", parse_arraystr_udf("actors"))
  elif x == 1:
    spdf = spdf.withColumn("spouses_string", parse_arraystr_udf("spouses_string"))
  elif x == 3:
    spdf = spdf.withColumn("characters", parse_arraystr_udf("characters"))

  final = spdf.drop(*columns)
  return final

# UDF to Create Array Type for Characters
def parse_arraystr(column_str):
  """An udf that turns a Stringtype, structured as an array, into an ArrayType.
  Input: column_str: a column name string. Output: a StringType transformed into an ArrayType"""
  try:
    return column_str.strip('[]').split(', ')
  except:
    try:
      return column_str.strip('[]').split('\n')
    except:
        return NullType()


# Register UDF
parse_arraystr_udf = udf(parse_arraystr, ArrayType(StringType()))
