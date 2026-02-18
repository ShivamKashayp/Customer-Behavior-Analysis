from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine 

df= pd.read_csv("customer_shopping_behavior.csv")
print(df.head())

print(df.info())
print(df.describe(include="all"))
print(df.isnull().sum())

# Handling missing values
# For numerical columns, we can fill missing values with the mean or median
df["Review Rating"] = df.groupby("Category")["Review Rating"].transform(lambda x: x.fillna(x.median()))
print(df.info())
print(df.isnull().sum())

df.columns=df.columns.str.lower().str.replace(" ","_")
df=df.rename(columns={"purchase_amount_(usd)":"purchase_amount","promo_code_used":"promo_code"})
print(df.columns)

# create a column for age group
labels=["young_adult","adult","middle_age","senior"]

df["age_group"]=pd.qcut(df["age"],q=4,labels=labels)
print(df[["age","age_group"]].head())

# purchase frequency days
frequency_mapping={
    "Fortnightly":14,
    "Weekly":7,
    "Monthly":30,
    "Quarterly":90,
    "Biweekly":14,
    "Annually":365,
    "Every 3 Months":90
}
df["purchase_frequency_days"]=df["frequency_of_purchases"].map(frequency_mapping)
(df[["purchase_frequency_days","frequency_of_purchases"]].head(10))

df[["discount_applied","promo_code"]].head()

(df["discount_applied"]==df["promo_code"]).all()

df=df.drop("promo_code",axis=1)
print(df.columns)
from sqlalchemy import create_engine
#  step 1: connect to database and save cleaned data
# replace placeholder with actual details
username="postgres"
password=quote_plus("Kashyap@4002")
host="localhost"
port="5432"
database="customer_behaviour"

engine=create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")

# step 2: load dataframe into postgresql 
table_name="customer"
df.to_sql(table_name,engine,if_exists="replace",index=False)

print(f"Data successfully loaded into the '{table_name}' in database '{database}'")