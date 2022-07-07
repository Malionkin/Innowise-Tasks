from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f

spark = SparkSession.builder.getOrCreate()


def read_table(table_address):
    return spark.read.format("csv").option("header", "true").load(table_address)


actor = read_table("data/actor.csv")
address = read_table("data/address.csv")
category = read_table("data/category.csv")
city = read_table("data/city.csv")
country = read_table("data/country.csv")
customer = read_table("data/customer.csv")
film = read_table("data/film.csv")
film_actor = read_table("data/film_actor.csv")
film_category = read_table("data/film_category.csv")
inventory = read_table("data/inventory.csv")
language = read_table("data/language.csv")
payment = read_table("data/payment.csv")
rental = read_table("data/rental.csv")
staff = read_table("data/staff.csv")
store = read_table("data/store.csv")

first_query = \
    category.join(film_category, on="category_id") \
        .withColumn("total_amount", f.count("film_id").over(Window.partitionBy("category_id"))) \
        .select(f.col("name"), f.col("total_amount")) \
        .distinct() \
        .orderBy(f.col("total_amount").desc())


second_query = \
    actor.join(film_actor, on="actor_id") \
        .join(film, on="film_id") \
        .withColumn("rental_ammount",
                    f.sum(film.rental_duration * film.rental_rate).over(Window.partitionBy("actor_id"))) \
        .select(f.col("first_name"), f.col("last_name"), f.col("rental_ammount")) \
        .distinct() \
        .orderBy(f.col("rental_ammount").desc()) \
        .limit(10)


third_query = \
    film.join(film_category, on="film_id") \
        .join(category, on="category_id") \
        .withColumn("total_cost", f.sum("replacement_cost").over(Window.partitionBy("category_id"))).select(
        f.col("name"), f.col("total_cost")).orderBy(f.col("total_cost").desc()).limit(1)


fourth_query = \
    film.join(inventory, on="film_id", how="left") \
        .groupby("film_id", "title") \
        .agg(f.count("inventory_id").alias("number")) \
        .filter(f.col("number") == 0) \
        .select(f.col("title")) \


fifth_query = \
    film.join(film_actor, on="film_id") \
        .join(actor, on="actor_id").join(film_category, on="film_id") \
        .join(category, on="category_id").groupby("actor_id", "name", "first_name", "last_name") \
        .agg(f.count("film_id").alias("number_of_children_films")) \
        .filter(f.col("name") == "Children") \
        .select(f.col("first_name"), f.col("last_name"), f.col("number_of_children_films")).orderBy(
        f.col("number_of_children_films").desc()).limit(5)


sixth_query = city.join(address, on="city_id") \
    .join(customer, on="address_id") \
    .withColumn("number_of_active", f.sum("active").over(Window.partitionBy("city"))) \
    .withColumn("number_of_inactive", f.sum(f.abs(customer.active - 1))
                .over(Window.partitionBy("city"))) \
    .select(f.col("city"), f.col("number_of_active"), f.col("number_of_inactive")) \
    .orderBy(f.col("number_of_inactive").desc())


data = city.join(address, on="city_id") \
    .join(customer, on="address_id") \
    .join(rental, on="customer_id") \
    .join(inventory, on="inventory_id") \
    .join(film, on="film_id") \
    .join(film_category, on="film_id") \
    .join(category, on="category_id") \
    .filter(f.col("return_date").isNotNull()) \
    .select(f.col("name").alias("category_name"), f.col("return_date"), f.col("rental_date"), f.col("city"))


data_1 = data.filter(f.col("title").like("A%"))\
    .withColumn("total_duration",
        f.sum(f.datediff(f.col("return_date") , f.col("rental_date")) * 24).over(Window.partitionBy(f.col("category_name")))) \
     .distinct() \
     .select(f.col("category_name"), f.col("total_duration")) \
     .orderBy(f.col("total_duration").desc()) \
     .limit(1)
data_2 = data.filter(f.col("city").like("%-%")) \
    .withColumn("total_duration",
                f.sum(f.datediff(f.col("return_date"), f.col("rental_date")) * 24).over(
                    Window.partitionBy(f.col("category_name")))) \
    .distinct() \
    .select(f.col("category_name"), f.col("total_duration")) \
    .orderBy(f.col("total_duration").desc()) \
    .limit(1)

seventh_query = data_1.union(data_2)





###################################
first_query.show()
second_query.show()
third_query.show()
fourth_query.show()
fifth_query.show()
sixth_query.show()
seventh_query.show()

