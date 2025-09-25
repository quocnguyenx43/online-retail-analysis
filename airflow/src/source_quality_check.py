import os
from datetime import datetime

import pandas as pd
import great_expectations as ge
from hdfs import InsecureClient
from airflow.models import Variable

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class EmailClient:
    """A simple client for sending HTML emails via SMTP."""

    def __init__(self, smtp_host: str = "smtp-mail.outlook.com", smtp_port: int = 587) -> None:
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port

    def send_html_email(
        self,
        sender_email: str,
        recipient_email: str,
        sender_password: str,
        subject: str,
        html_body: str
    ) -> None:
        """
        Send an HTML email using the configured SMTP server.

        Args:
            sender_email: The sender's email address
            sender_password: The sender's email account password
            recipient_email: The recipient's email address
            subject: The subject of the email
            html_body: The HTML content of the email

        Returns:
            None
        """
        try:
            # Establish connection
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(sender_email, sender_password)

                # Construct the email
                message = MIMEMultipart("alternative")
                message["Subject"] = subject
                message["From"] = sender_email
                message["To"] = recipient_email

                # Attach the HTML body
                message.attach(MIMEText(html_body, "html"))

                # Send the email
                smtp.sendmail(sender_email, recipient_email, message.as_string())
                print(f"Email sent to {recipient_email}")

        except Exception as e:
            print(f"Failed to send email: {e}")


OUTLOOK_USERNAME = Variable.get("alert_outlook_username", default_var="20520270@ms.uit.edu.vn")
OUTLOOK_PASSWORD = Variable.get("alert_outlook_password", default_var="P@ssw0rd")
HDFS_HOSTNAME = Variable.get("hdfs_hostname", default_var="localhost")
HDFS_PORT = Variable.get("hdfs_port", default_var="9870")
HDFS_FILE_PATH = "/data_lake/online-retail.csv"
LOCAL_TMP_FILE = "./online-retail.csv"

email_client = EmailClient()
ge_context = ge.get_context()
hdfs_client = InsecureClient(f"http://{HDFS_HOSTNAME}:{HDFS_PORT}", user='sales-etl-hadoop')


def hdfs_file_exists(client: InsecureClient, path: str) -> bool:
    """Check if a file exists in HDFS."""
    try:
        print(client.status("/data_lake/"))
        return True
    except Exception:
        return False
    

def download_file_from_hdfs(hdfs_client: InsecureClient, hdfs_path: str, local_path: str) -> None:
    """Download a file from HDFS to local."""
    with hdfs_client.read(hdfs_path, encoding="utf-8") as reader:
        df = pd.read_csv(reader)
        df.to_csv(local_path, index=False, header=True)
        print("done\n")


def validate_data(local_path: str):
    """Run Great Expectations validations on the dataset."""
    df = pd.read_csv(local_path)
    batch_parameters = {"dataframe": df}

    print("dasdasda1")
    ge_context.data_sources.add_pandas("sales_data_source1")
    ge_context.data_sources.get("sales_data_source1"). \
        add_dataframe_asset(name="sales_data_source2")
    
    print("dasdasda")

    batch_definition = (
        ge_context.data_sources.get("sales_data_source1")
        .get_asset("sales_data_source2")
        .get_batch_definition("sales_data_source3")
    )

#     datasource = ge_context.data_sources.add_pandas("pandas_datasource")
#     asset = datasource.add_dataframe_asset("retail_data", dataframe=df)
# 
#     batch_request = asset.build_batch_request()
# 
#     suite = ge_context.suites.add(ge.ExpectationSuite(name="retail_validation_suite"))

    # Check schema of the data source
    expected_columns = [
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    ]

    # Check table columns
    # suite.add_expectation(
    #     ge.expectations.ExpectTableColumnsToMatchSet(
    #         column_set=expected_columns,
    #         exact_match=True
    #     )
    # )

    expectations = []

    expectations.append(
        ge.expectations.ExpectTableColumnsToMatchSet(
            column_set=expected_columns,
            exact_match=True
        )
    )

    key_columns = ["InvoiceNo", "CustomerID", "StockCode"]

    # Check for null values in key columns
    for col in key_columns:
        expectations.append(
            ge.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

#     validator = ge_context.get_validator(
#         batch_request=batch_request,
#         expectation_suite=suite
#     )
# 
    # Test the Expectation
    validation_results = batch.validate(expectations)
    print(validation_results)

    return validation_results


def build_email_message(file_exists_msg: str, validation_results: dict) -> str:
    """Build an HTML message summarizing validation tasks."""
    task_messages = []

    for index, result in enumerate(validation_results["results"]):
        expectation_type = result["expectation_config"]["expectation_type"]
        column = result["expectation_config"]["kwargs"].get("column", "N/A")
        task_no = index + 2  # Task numbering continues from 2

        if result["success"]:
            if expectation_type == "expect_table_columns_to_match_set":
                msg = f"<strong>Task {task_no}:</strong> Schema check passed => <strong><span style='color:green;'>Success</span></strong>"
            else:
                msg = f"<strong>Task {task_no}:</strong> No null values in column <em>{column}</em> => <strong><span style='color:green;'>Success</span></strong>"
        else:
            if expectation_type == "expect_table_columns_to_match_set":
                msg = f"<strong>Task {task_no}:</strong> Schema check failed => <strong><span style='color:red;'>Failed</span></strong>"
            else:
                msg = f"<strong>Task {task_no}:</strong> Null values found in column <em>{column}</em> => <strong><span style='color:red;'>Failed</span></strong>"

        task_messages.append(msg)

    return f"{file_exists_msg}<br>" + "<br>".join(task_messages)


def run_source_quality_check():
    # Check file existence
    if hdfs_file_exists(hdfs_client, HDFS_FILE_PATH):
        file_exists_msg = (
            f"<strong>Task 1:</strong> File exists: {HDFS_FILE_PATH} "
            f"=> <strong><span style='color:green;'>Success</span></strong>"
        )
        download_file_from_hdfs(hdfs_client, HDFS_FILE_PATH, LOCAL_TMP_FILE)
    else:
        file_exists_msg = (
            f"<strong>Task 1:</strong> File missing: {HDFS_FILE_PATH} "
            f"=> <strong><span style='color:red;'>Failed</span></strong>"
        )
        final_message = file_exists_msg
        validation_results = None

    # Only validate if file exists
    if file_exists_msg and "Success" in file_exists_msg:
        validation_results = validate_data(LOCAL_TMP_FILE)
        final_message = build_email_message(file_exists_msg, validation_results)

    # Prepare email
    subject = "Data Quality Check for Source"
    body = f"""
    <html>
        <body>
            <p>{final_message}</p>
            <p>Please review the attached report and fix any errors.</p>
        </body>
    </html>
    """

    # # Send alert
    # email_client.send_html_email(
    #     sender_email="20520270@ms.uit.edu.vn",
    #     recipient_email=OUTLOOK_USERNAME,
    #     sender_password=OUTLOOK_PASSWORD,
    #     subject=subject,
    #     html_body=body,
    # )

    # Clean up local file
    if os.path.exists(LOCAL_TMP_FILE):
        os.remove(LOCAL_TMP_FILE)


# Entry point (if running as script)
if __name__ == "__main__":
    run_source_quality_check()
