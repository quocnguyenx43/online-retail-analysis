import pandas as pd
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

# Create a GX context (environment setup)
context = gx.get_context()

# df = pd.read_csv("online-retail.csv")
# 
# # # Data Source
# 
# data_source = context.data_sources.add_pandas("pandas")
# data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")
# 
# batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
# batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
# # print("Sample data:\n", batch.head())
# 
# expectation = gx.expectations.ExpectColumnValuesToBeBetween(
#     column="UnitPrice", min_value=1, max_value=100, severity="warning"
# )
# 
# validation_result = batch.validate(expectation)
# print(validation_result)


# connection_string = "postgresql+psycopg2://airflow:airflow@localhost/sales_etl"
# 
# data_source = context.data_sources.add_postgres(
#     "postgres db", connection_string=connection_string
# )
# data_asset = data_source.add_table_asset(name="country data", table_name="dwh.country")
# 
# batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
# batch = batch_definition.get_batch()
# 
# print(batch.head())
# 
# suite = context.suites.add(
#     gx.core.expectation_suite.ExpectationSuite(name="expectations")
# )
# suite.add_expectation(
#     gx.expectations.ExpectColumnValuesToNotBeNull(
#         column="country_id", severity="warning"
#     )
# )
# suite.add_expectation(
#     gx.expectations.ExpectColumnValuesToBeBetween(
#         column="numeric_code", min_value=4, severity="critical"
#     )
# )
# 
# validation_definition = context.validation_definitions.add(
#     gx.core.validation_definition.ValidationDefinition(
#         name="validation definition",
#         data=batch_definition,
#         suite=suite,
#     )
# )
# 
# checkpoint = context.checkpoints.add(
#     gx.checkpoint.checkpoint.Checkpoint(
#         name="checkpoint", validation_definitions=[validation_definition]
#     )
# )
# 
# checkpoint_result = checkpoint.run()
# print(checkpoint_result.describe())

source_folder = "/home/skibidi/code/"
data_source_name = "source_data_hehe"

data_source = context.data_sources.add_pandas_filesystem(
    name=data_source_name, base_directory=source_folder
)

data_source = context.data_sources.get(data_source_name)

data_asset_name = "online_file"

file_csv_asset = data_source.add_csv_asset(name=data_asset_name)


file_data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)

print(file_data_asset)

batch_definition_name = "online-retail"
batch_definition_path = "online_retail_analytics/abc/data/online_retail.csv"

batch_definition = file_data_asset.add_batch_definition_path(
    name=batch_definition_name, path=batch_definition_path
)


batch = batch_definition.get_batch()
print(batch.head())