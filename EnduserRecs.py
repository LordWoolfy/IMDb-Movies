from pyspark.sql.functions import col, array_contains
import datetime
import shortuuid

def set_age_gender_range(age, gender):
  """
  Sets the age and gender range for the population type.

  Args:
  age: An integer representing the age.
  gender: A string representing the gender.

  Returns:
  population_type: A string representing the population type.
  """
  population_type = 'mean_vote'
  if age is None and gender in ('M', 'F'):
    population_type = f"{'males' if gender == 'M' else 'females'}_avg_vote"
  elif age is None and gender is None:
    population_type = "weighted_average_vote"
  elif 0 < age < 18:
    population_type = f"{'males' if gender == 'M' else 'females' if gender == 'F' else 'allgenders'}_0age_avg_vote"
  elif 18 <= age < 30:
    population_type = f"{'males' if gender == 'M' else 'females' if gender == 'F' else 'allgenders'}_18age_avg_vote"
  elif 30 <= age < 45:
    population_type = f"{'males' if gender == 'M' else 'females' if gender == 'F' else 'allgenders'}_30age_avg_vote"
  elif age >= 45:
    population_type = f"{'males' if gender == 'M' else 'females' if gender == 'F' else 'allgenders'}_45age_avg_vote"
  return population_type

def get_recommendations(datasets, input_dict, movies_list = [None]):
  """
  Generates movie recommendations based on user input.

  Args:
  datasets: A tuple containing 4 Spark DataFrames: movies, names, ratings, and title_principals.
  input_dict (dict): User input containing preferences (age, genre, etc.).
  movies_list (list, optional): previous recommendations, in order of preference. Defaults to [None]

  Returns:
  list: Ranked list of recommended movies.
  """
  movies_df, names_df, ratings_df, title_principals_df = datasets

  filtered_movies = movies_df

  population_range = set_age_gender_range(input_dict['Age'], input_dict['Gender'])

  if movies_list != [None]:
    filtered_movies = filtered_movies.filter(~filtered_movies.imdb_title_id.isin(movies_list))
  for key, value in input_dict.items():
    if value != None or value != '' or value != []:
      if key == 'Years':
        if value[1] == None:
          filtered_movies = filtered_movies.filter(filtered_movies.year >= value[0])
        else:
          filtered_movies = filtered_movies.filter((filtered_movies.year >= value[0]) & (filtered_movies.year <= value[1]))
      # Add genres from input movies and remove movies from recommendation dataset
      elif key == 'Movies':
        if value == [None]:
          continue
        added_genres = filtered_movies.filter(filtered_movies.original_title.isin(value)).select('genre')
        added_genres = added_genres.rdd.flatMap(lambda x: x).collect()
        new_genres = []
        for genres in added_genres:
          for genre in genres[1:]:
            if genre in genres:
              new_genres.append(genre)
            else:
              new_genres.remove(genre)
        for genre in new_genres:
          if genre not in input_dict['Genres']:
              input_dict['Genres'].append(genre)
        filtered_movies = filtered_movies.filter(~filtered_movies.original_title.isin(value))
      # Filter dataset based on Genre
      elif key == 'Genres':
        if value == [None]:
          continue
        genre_condition = array_contains(col("genre"), value[0])
        for genre in value[1:]:
          if genre != None:
              genre_condition = genre_condition | array_contains(col("genre"), genre)
        filtered_movies = filtered_movies.filter(genre_condition)
      # Filter dataset based on Not Genre
      elif key == 'Not Genre':
        if value == [None]:
          continue
        for genre in value:
          filtered_movies = filtered_movies.filter(~array_contains(col("genre"), genre))
      # Filter dataset based on spoken language
      elif key == 'Languages':
        if value == [None]:
          continue
        language_condition = array_contains(col("language"), value[0])
        for language in value[1:]:
          language_condition = language_condition | array_contains(col("language"), language)
        filtered_movies = filtered_movies.filter(language_condition)
      # Filter dataset based on actors, directors, producers, etc.
      elif key == 'Names':
        if value == [None]:
          continue
        name_filter = filtered_movies.join(title_principals_df, filtered_movies.imdb_title_id == title_principals_df.imdb_title_id, 'inner').drop(title_principals_df["imdb_title_id"])
        name_filter = name_filter.join(names_df, name_filter.imdb_name_id == names_df.imdb_name_id, 'inner').filter(names_df.name.isin(value) | names_df.birth_name.isin(value)).drop(names_df["imdb_name_id"])
        if name_filter.count() >= 10:
          filtered_movies = name_filter
      # Filter dataset based on production companies
      elif key == 'Production Companies':
        if value == [None]:
          continue
        company_condition = array_contains(col('production_company'), value[0])
        for company in value[1:]:
          company_condition = company_condition | array_contains(col('production_company'), company)
        company_filter = filtered_movies.filter(company_condition)
        if company_filter.count() >= 10:
          filtered_movies = company_filter
      # Filter dataset based on min rating
      elif key == 'Min Rating':
        filtered_movies = filtered_movies.filter(filtered_movies.avg_vote >= value)

  filtered_ratings = ratings_df
  filtered_movies = filtered_movies.dropDuplicates()
  ranked_movies = None
  if input_dict['Critic']:
    ranked_movies = filtered_movies.orderBy(col('metascore').desc()).select(filtered_movies.imdb_title_id, filtered_movies.original_title, filtered_movies.year, filtered_movies.genre).limit(10)
  else:
    ranked_movies = filtered_movies.join(filtered_ratings, filtered_movies.imdb_title_id == filtered_ratings.imdb_title_id, 'inner')\
    .orderBy(col(population_range).desc()).select(filtered_movies.imdb_title_id, filtered_movies.original_title, filtered_movies.year, filtered_movies.genre).limit(10)
  ranked_class = ranked_movies.rdd.map(lambda x: x).collect()
  ranked_list = []
  for row in ranked_class:
    ranked_list.append([row['imdb_title_id'], row['original_title'], row['year'], row['genre']])
  if len(ranked_list) == 0:
    ranked_list = ['No Movies Found']
  return ranked_list

def rate_rec(rating = 0):
  """
  Set the rating for a recommended movie.

  Args:
  rating (int): Rating for the movie. 0, -1, 1, 2

  Returns:
  str: Rating string. 'Not Rated', 'Disiked', 'Liked', or 'Loved'.
  """
  rating_string = ''
  if rating == 0:
    rating_string = 'Not Rated'
  elif rating == -1:
    rating_string = 'Disiked'
  elif rating == 1:
    rating_string = 'Liked'
  elif rating == 2:
    rating_string = 'Loved'
  return rating_string

def get_user_info(movies_dict):
  """
  Generate a dictionary containing user information, including a unique user ID.

  Args:
  movies_dict (dict): Dictionary containing user preferences.

  Returns:
  dict: Dictionary containing user information, including a unique user ID.
  """
  user_info_dict = {'User Id': 'Uid_' + datetime.datetime.now().strftime('%Y%m%d%H%M%f') + str(shortuuid.uuid()[:2]),
                    'Entry Date': datetime.datetime.now().strftime('%Y-%m-%d'),
                    'Age': movies_dict['Age'],
                    'Gender': movies_dict['Gender'],
                    'Country': movies_dict['Country'],
                    'Prefered Languages': movies_dict['Languages']}
  return user_info_dict

def get_movies_ids(rec_results):
  """
  Returns a list of movie IDs based on recommended movies.

  Args:
  rec_results (list): List of recommended movies.

  Returns:
  list: List of movie IDs.
  """
  movies_list = []
  for i in range(len(rec_results)):
    movies_list.append(rec_results[i][0])
  return movies_list
