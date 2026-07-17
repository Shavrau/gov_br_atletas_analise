from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform_to_string(df):
    for col in df.columns:
        df = df.withColumn(col, df[col].cast('string'))
    return df

def save_to_bronze(
    df: DataFrame, 
    table_name: str) -> None:
    path = f"projeto_gov.bronze.{table_name}"
    df.write.mode("overwrite").saveAsTable(path)
    
# Source - https://stackoverflow.com/a/73733058
# Posted by Chris de Groot
# Retrieved 2026-07-16, License - CC BY-SA 4.0

def remove_bda_chars_from_columns(df):
    return  df.select([col(x).alias(x.replace(" ", "_").replace("/", "").replace("%", "pct").replace("(", "").replace(")", "")) for x in df.columns])
