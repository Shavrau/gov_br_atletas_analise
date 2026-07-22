from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.testing import assertDataFrameEqual
from pyspark.errors import PySparkAssertionError
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from bronze_utils import transform_to_string, remove_bda_chars_from_columns


#remove special characters function tests
def test_remove_bda_chars_from_columns():
    data = [
        (1, "a", 3),
        (2, "b", 4)
    ]

    df = spark.createDataFrame(data, ["id teste", "codigo/categoria", "valor%pedido"])
    
    df_expected = spark.createDataFrame(data, ["id_teste", "codigo_categoria", "valor_pedido"])
    
    df_transformed = remove_bda_chars_from_columns(df)
        
    assertDataFrameEqual(df_expected, df_transformed)

def test_remove_bda_chars_from_columns_negative():
    data = [
        (1, "a", 3),
        (2, "b", 4)
    ]

    df = spark.createDataFrame(data, ["id teste", "codigo/categoria", "valor%pedido"])
    
    df_expected_wrong = spark.createDataFrame(data, ["id teste", "codigo/categoria", "valor%pedido"])
    
    df_transformed = remove_bda_chars_from_columns(df)
    
    try:
        assertDataFrameEqual(df_expected_wrong, df_transformed)
        assert False, "Expected PySparkAssertionError but none was raised"
    except PySparkAssertionError:
        pass

#transform to string function tests
def test_transform_to_string():

    df_expect = spark.createDataFrame(["1"], ["n_teste"])

    df = spark.createDataFrame([1], ["n_teste"])

    df_cleaned = transform_to_string(df)

    assertDataFrameEqual(df_expect, df_cleaned)

