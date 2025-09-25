import great_expectations as gx
# from great_expectations.core.expectation_suite import ExpectationSuite

# Create a GX context (environment setup)
context = gx.get_context()

# # Data Source
# datasource = context.data_sources.add_pandas("sales_dataframe")
# asset = datasource.add_csv_asset("sales_data", filepath_or_buffer="online-retail.csv")
# 
# # 3. Define how to retrieve data (Batch Definition)
# batch_def = asset.add_batch_definition("full_sales_data")
# 
# # 4. Load a Batch (actual data records)
# batch = batch_def.get_batch()
# print("Sample data:\n", batch.head())
# 
# # 5. Create an Expectation Suite
# suite = ExpectationSuite(name="sales_suite")
# suite = context.suites.add_or_update(suite)
# 
# # Add expectations (rules)
# suite.add_expectation(
#     gx.expectations.ExpectColumnValuesToNotBeNull(
#         column="order_id"
#     )
# )
# 
# validation_def = context.validation_definitions.add(
#     name="validate_sales_data",
#     suite_name="sales_suite",
#     batch_definition_name="full_sales_data"
# )
# 
# # 7. Run a Validation
# result = context.run_validation(validation_def)
# print("\nValidation Result:\n", result)
# 
# # 8. Create and run a Checkpoint (production style)
# checkpoint = context.checkpoints.add(
#     name="sales_checkpoint",
#     validation_definitions=["validate_sales_data"],
#     actions={"update_data_docs": {}}  # also build Data Docs
# )
# checkpoint_result = checkpoint.run()
# 
# # 9. Build and open Data Docs (HTML reports)
# context.build_data_docs()
# context.open_data_docs()
