# Advanced Snowflake UDFs

### Query Homework

Assignment
==================

## Dataset Overview

This assignment involves working with the **`bootcamp.actor_films`** dataset in Snowflake

The `bootcamp.actor_films` dataset contains the following fields:

- `actor`: The name of the actor.
- `actor_id`: A unique identifier for each actor.
- `film`: The name of the film.
- `year`: The year the film was released.
- `votes`: The number of votes the film received.
- `rating`: The rating of the film.
- `film_id`: A unique identifier for each film.

The primary key for this dataset is (`actor_id`, `film_id`).

## Query Tasks

### Find all the years actors did not make a film after they started

- Do this in two ways
  - First using window functions with Snowflake SQL
  - Using a Python UDF in Snowflake

### Find actor names that are within 3 Levenshtein distance from each other

- Create a Python UDF that includes the pip package `levenshtein` 
  - Use this UDF in a query to find all actors who have "similar" names

### How to submit
- Include the .sql file(s) in zip file.
- Go to the [Assignments page](https://www.dataexpert.io/assignment/158) and upload it there
