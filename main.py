from typing import Any, Dict, List, Optional
import boto3
import argparse
import logging
import os
import re
import textwrap
import time
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

S3_BUCKET_NAME = "textract-console-us-east-1-1b79e4f8-575e-4e6b-b80c-9d6cf5953a01"


def fallback_find_payment_terms(bucket_name: str, object_name: str) -> Optional[str]:
    """
    Uses DetectDocumentText as a fallback to find payment terms.

    This is called when AnalyzeExpense fails to identify the PAYMENT_TERMS field.
    It starts a text detection job, waits for it to succeed by polling its
    status, and then scans the raw text for a predefined list of keywords.

    Args:
        bucket_name: The S3 bucket where the document is located.
        object_name: The name (key) of the document in the S3 bucket.

    Returns:
        A string containing the concatenated lines where payment terms were found,
        or None if no terms were found or an error occurred.
    """
    logging.info(
        f"Executing fallback text detection for s3://{bucket_name}/{object_name}"
    )
    textract_client = boto3.client("textract")

    try:
        # 1. Start the asynchronous Textract job
        response = textract_client.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": bucket_name, "Name": object_name}}
        )
        job_id = response["JobId"]
        logging.info(f"Fallback job started with ID: {job_id}")

        # 2. Wait for the job to complete
        job_status = ""
        while job_status != "SUCCEEDED":
            time.sleep(5)
            result = textract_client.get_document_text_detection(JobId=job_id)
            job_status = result["JobStatus"]
            logging.info(f"Fallback job status: {job_status}")
            if job_status in ["FAILED", "PARTIAL_SUCCESS"]:
                logging.error(f"Fallback Textract job failed with status: {job_status}")
                return None

        # 3. Once successful, fetch all results (handles pagination)
        logging.info("Fallback job succeeded. Fetching all pages of results...")
        pages = []
        get_pages_result = textract_client.get_document_text_detection(JobId=job_id)
        pages.append(get_pages_result)
        next_token = get_pages_result.get("NextToken")
        while next_token:
            get_pages_result = textract_client.get_document_text_detection(
                JobId=job_id, NextToken=next_token
            )
            pages.append(get_pages_result)
            next_token = get_pages_result.get("NextToken")

        # 4. Process the results to find payment terms keywords
        payment_terms_keywords = (
            "terms of payment",
            "payment is due",
            "late payment",
            "immediate payment",
            "payment terms",
            "please pay",
            "terms:",
            "balance due",
            "unpaid for",
            "penalty",
            "interest",
            "please remit",
            "net 15",
            "net 30",
            "net 60",
            "net 90",
            "due upon receipt",
        )

        found_terms = []
        logging.info("Searching for payment terms in the raw text...")
        for page in pages:
            for block in page.get("Blocks", []):
                if block.get("BlockType") == "LINE":
                    line_text = block.get("Text", "").lower()
                    if any(keyword in line_text for keyword in payment_terms_keywords):
                        found_terms.append(block.get("Text"))

        return " ".join(found_terms) if found_terms else None

    except Exception as e:
        logging.error(f"An error occurred during fallback processing: {e}")
        return None


def _parse_float(value_str: Optional[str]) -> Optional[float]:
    """
    Safely parses a string containing a US or European-style number into a float.

    Handles currency symbols (€, $), spaces, thousands separators (',' or '.'),
    and decimal separators.

    Args:
        value_str: The string representation of the number.

    Returns:
        The parsed float, or None if parsing fails.
    """

    if not value_str:
        return None
    try:
        cleaned_str = re.sub(r"[€$ ]", "", str(value_str)).strip()
        num_commas = cleaned_str.count(",")
        num_periods = cleaned_str.count(".")
        if (
            num_periods > 0
            and num_commas == 1
            and cleaned_str.rfind(".") < cleaned_str.rfind(",")
        ):
            return float(cleaned_str.replace(".", "").replace(",", "."))
        if num_commas > 0 and (
            num_periods == 0 or cleaned_str.rfind(",") < cleaned_str.rfind(".")
        ):
            return float(cleaned_str.replace(",", ""))
        if num_commas == 1 and num_periods == 0:
            return float(cleaned_str.replace(",", "."))
        return float(cleaned_str)
    except (ValueError, TypeError):
        return None


def analyze_invoice_primary(bucket_name: str, file_name: str) -> Optional[Dict[str, Any]]:
    """
    Starts an asynchronous AnalyzeExpense job and polls for a successful
    completion status. After success, it fetches all paginated results and
    returns the complete JSON response.

    Args:
        bucket_name: The name of the S3 bucket containing the invoice.
        file_name: The key (filename) of the invoice in the bucket.

    Returns:
        The complete JSON response from the GetExpenseAnalysis API call as a
        dictionary, or None if the job fails or an error occurs.
    """
    textract_client = boto3.client("textract")
    job_id = ""

    try:
        logging.info("Starting primary asynchronous Textract job (AnalyzeExpense)...")
        response = textract_client.start_expense_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket_name, "Name": file_name}}
        )
        job_id = response["JobId"]
        logging.info(f"Primary job started with ID: {job_id}")

        logging.info("Waiting for primary job to complete...")
        job_status = "IN_PROGRESS"
        while job_status == "IN_PROGRESS":
            time.sleep(5)
            job_response = textract_client.get_expense_analysis(JobId=job_id)
            job_status = job_response["JobStatus"]
            logging.info(f"Current primary job status: {job_status}")

        if job_status == "SUCCEEDED":
            logging.info("Primary job completed successfully. Fetching all results...")
            full_response = job_response
            next_token = full_response.get("NextToken")
            while next_token:
                job_response = textract_client.get_expense_analysis(
                    JobId=job_id, NextToken=next_token
                )
                full_response["Blocks"].extend(job_response.get("Blocks", []))
                full_response["ExpenseDocuments"].extend(
                    job_response.get("ExpenseDocuments", [])
                )
                next_token = job_response.get("NextToken")
            full_response.pop("NextToken", None)
            return full_response
        else:
            logging.error(f"Primary Textract job failed with status: {job_status}")
            return None
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        error_message = e.response.get("Error", {}).get("Message")
        logging.error(
            f"AWS Client Error during primary analysis: {error_code} - {error_message}"
        )
        return None


def merge_invoice_documents(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merges multiple ExpenseDocument sections from a Textract response.

    Some multi-page invoices are returned as separate document sections. This
    function consolidates them by merging LineItemGroups and non-duplicate
    SummaryFields into the first document section.

    Args:
        docs: A list of ExpenseDocument dictionaries from the Textract response.

    Returns:
        A list containing a single, merged ExpenseDocument dictionary.
    """
    if not docs or len(docs) <= 1:
        return docs
    logging.info(f"Multiple ({len(docs)}) document sections found. Merging.")
    primary_doc = docs[0]
    if "LineItemGroups" not in primary_doc:
        primary_doc["LineItemGroups"] = []
    if "SummaryFields" not in primary_doc:
        primary_doc["SummaryFields"] = []
    primary_field_types = {
        f.get("Type", {}).get("Text") for f in primary_doc.get("SummaryFields", [])
    }
    for subsequent_doc in docs[1:]:
        if subsequent_doc.get("LineItemGroups"):
            primary_doc["LineItemGroups"].extend(subsequent_doc.get("LineItemGroups"))
        for field in subsequent_doc.get("SummaryFields", []):
            field_type = field.get("Type", {}).get("Text")
            if field_type and field_type not in primary_field_types:
                primary_doc["SummaryFields"].append(field)
                primary_field_types.add(field_type)
    return [primary_doc]


def parse_extracted_data(response: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """

    Extracts summary fields (invoice number, date, total, ...) and line items.
    Includes a fallback to treat certain SummaryFields as line items if no
    standard LineItemGroups are detected.

    Args:
        response: The complete JSON response dictionary from Textract.

    Returns:
        A list of dictionaries, where each dictionary represents a parsed invoice.
    """
    if not response or not response.get("ExpenseDocuments"):
        logging.warning("Response is empty or does not contain ExpenseDocuments.")
        return []

    merged_docs = merge_invoice_documents(response["ExpenseDocuments"])
    all_invoices_data = []

    for doc in merged_docs:
        extracted_data = {}
        summary_fields = {
            f.get("Type", {}).get("Text"): f.get("ValueDetection", {}).get("Text")
            for f in doc.get("SummaryFields", [])
        }
        extracted_data["Invoice Number"] = summary_fields.get(
            "INVOICE_RECEIPT_ID", "N/A"
        )
        extracted_data["Invoice Date"] = summary_fields.get(
            "INVOICE_RECEIPT_DATE", "N/A"
        )
        extracted_data["Invoice Total"] = summary_fields.get("TOTAL", "N/A")

        payment_terms_value = summary_fields.get("PAYMENT_TERMS") or summary_fields.get(
            "TERMS"
        )
        payment_terms = (
            payment_terms_value
            if payment_terms_value and payment_terms_value.strip()
            else None
        )
        extracted_data["Payment Terms"] = payment_terms  # Will be None if not found

        line_items = []
        # PRIMARY METHOD: Process standard LineItemGroups
        for group in doc.get("LineItemGroups", []):
            for item in group.get("LineItems", []):
                raw_fields = {
                    f.get("Type", {})
                    .get("Text"): f.get("ValueDetection", {})
                    .get("Text")
                    for f in item.get("LineItemExpenseFields", [])
                }
                if "ITEM" not in raw_fields:
                    continue

                line_item_details = {"Description": raw_fields.get("ITEM")}
                amount_str = raw_fields.get("PRICE") or raw_fields.get("AMOUNT") # The total amount for the line item is usually 'PRICE'
                quantity_str = raw_fields.get("QUANTITY")  
                unit_price_str = raw_fields.get("UNIT_PRICE") 
                hours_str = raw_fields.get("HOURS")
                rate_str = raw_fields.get("RATE")

                line_item_details["Amount"] = amount_str

                is_service_invoice = hours_str or (
                    quantity_str and "." in str(quantity_str)
                )

                if is_service_invoice:
                    hours = hours_str or quantity_str
                    line_item_details["Hours"] = hours
                    line_item_details["Rate"] = rate_str

                elif quantity_str:
                    line_item_details["Quantity"] = quantity_str
                    line_item_details["Unit Price"] = unit_price_str

                else:
                    line_item_details["Quantity"] = None
                    line_item_details["Unit Price"] = None
                line_items.append(line_item_details)

        # FALLBACK METHOD: If no line items were found, parse SummaryFields
        if not line_items:
            logging.info(
                "No standard line items found. Checking SummaryFields for fallback items."
            )
            # Define summary fields that should NOT be treated as line items
            excluded_summary_types = {
                "TOTAL",
                "TAX",
                "INVOICE_RECEIPT_ID",
                "INVOICE_RECEIPT_DATE",
                "VENDOR_NAME",
                "VENDOR_ADDRESS",
                "RECEIVER_NAME",
                "RECEIVER_ADDRESS",
                "DUE_DATE",
                "PAYMENT_TERMS",
                "TERMS",
                "SHIPPING_HANDLING_CHARGE",
                "GRATUITY",
                "ADDRESS",
                "STREET",
                "CITY",
                "STATE",
                "ZIP_CODE",
                "NAME",
                "ADDRESS_BLOCK",
                "CLIENT_MATTER",
                "CLIENT_ID",
            }
            for field in doc.get("SummaryFields", []):
                field_type_text = field.get("Type", {}).get("Text")
                amount = field.get("ValueDetection", {}).get("Text")

                # Use _parse_float for robust number check 
                if (
                    field_type_text
                    and field_type_text not in excluded_summary_types
                    and amount
                    and _parse_float(amount) is not None
                ):
                    description = (
                        field.get("LabelDetection", {}).get("Text") or field_type_text
                    )
                    if description:
                        line_items.append(
                            {"Description": description, "Amount": amount}
                        )

        extracted_data["Line Items"] = line_items
        all_invoices_data.append(extracted_data)
    return all_invoices_data


def display_results(file_name: str, invoice_list: List[Dict[str, Any]]) -> None:
    """
    Prints the extracted data in a structured, readable table format.

    Args:
        file_name: The name of the file being displayed.
        invoice_list: A list of parsed invoice data dictionaries.
    """
    print("\n" + "=" * 100)
    print(f"Extraction Results for: {file_name}")
    print("=" * 100)

    if not invoice_list:
        print("No data was extracted.")
        print("=" * 100 + "\n")
        return

    for idx, data in enumerate(invoice_list):
        if len(invoice_list) > 1:
            print(f"\n--- Invoice Document {idx + 1} of {len(invoice_list)} ---")

        print(f"Invoice Number: {data.get('Invoice Number', 'N/A')}")
        print(f"Invoice Date:   {data.get('Invoice Date', 'N/A')}")
        print(f"Invoice Total:  {data.get('Invoice Total', 'N/A')}")

        payment_terms = data.get("Payment Terms", "N/A")
        wrapped_terms = textwrap.fill(
            payment_terms,
            width=98,
            initial_indent="Payment Terms:  ",
            subsequent_indent="                ",
        )
        print(wrapped_terms)

        line_items = data.get("Line Items")
        if not line_items:
            print("\n--- Line Items ---")
            print("  No line items found.")
            continue

        print("\n--- Line Items ---")

        first_item = next((item for item in line_items if item), None)
        is_service_invoice = (
            first_item and "Hours" in first_item and first_item.get("Hours") is not None
        )

        desc_width = 50
        num_width = 4
        if is_service_invoice:
            header = f"{'#':<{num_width}} | {'Description':<{desc_width}} | {'Hours':<8} | {'Rate':<15} | {'Amount':<15}"
        else:
            header = f"{'#':<{num_width}} | {'Description':<{desc_width}} | {'Qty':<8} | {'Unit Price':<15} | {'Amount':<15}"

        print(header)
        print("-" * len(header))

        num_items = len(line_items)
        for i, item in enumerate(line_items):
            desc = (item.get("Description", "N/a") or "N/a").replace("\n", " ")
            amount_str = (
                str(item.get("Amount")) if item.get("Amount") is not None else "N/A"
            )
            item_num_str = f"{(i + 1):<{num_width}}"

            wrapped_desc_lines = textwrap.wrap(desc, width=desc_width)
            first_desc_line = wrapped_desc_lines[0] if wrapped_desc_lines else ""

            if is_service_invoice:
                hours = item.get("Hours")
                rate = item.get("Rate")
                hours_str = str(hours) if hours is not None else "N/A"
                rate_str = str(rate) if rate is not None else "N/A"
                print(
                    f"{item_num_str} | {first_desc_line:<{desc_width}} | {hours_str:<8} | {rate_str:<15} | {amount_str:<15}"
                )
            else:
                qty = item.get("Quantity")
                price = item.get("Unit Price")
                qty_str = str(qty) if qty is not None else "N/A"
                price_str = str(price) if price is not None else "N/A"
                print(
                    f"{item_num_str} | {first_desc_line:<{desc_width}} | {qty_str:<8} | {price_str:<15} | {amount_str:<15}"
                )

            if len(wrapped_desc_lines) > 1:
                for line in wrapped_desc_lines[1:]:
                    print(
                        f"{'':<{num_width}} | {line:<{desc_width}} | {'':<8} | {'':<15} | {'':<15}"
                    )

            if i < num_items - 1:
                print(
                    f"{'-'*num_width} | {'-'*desc_width} | {'-'*8} | {'-'*15} | {'-'*15}"
                )

    print("=" * 100 + "\n")


def main():
    """
    Main function to orchestrate the invoice processing.
    """
    parser = argparse.ArgumentParser(
        description="Process invoices using AWS Textract."
    )
    parser.add_argument(
        "files", metavar="FILE", nargs="+", help="Paths to invoice PDF files."
    )
    args = parser.parse_args()

    s3_client = boto3.client("s3")

    for file_path in args.files:
        file_name = os.path.basename(file_path)
        logging.info(f"--- Starting processing for {file_name} ---")

        try:
            # Upload file to S3
            logging.info(f"Uploading {file_name} to S3 bucket {S3_BUCKET_NAME}...")
            s3_client.upload_file(file_path, S3_BUCKET_NAME, file_name)
            logging.info("Upload successful.")

            # Run primary analysis (AnalyzeExpense)
            textract_response = analyze_invoice_primary(S3_BUCKET_NAME, file_name)

            if textract_response:
                # Parse the primary results
                parsed_data = parse_extracted_data(textract_response)

                # Check for payment terms and run fallback if necessary
                for invoice_data in parsed_data:
                    if not invoice_data.get("Payment Terms"):
                        logging.warning(
                            "Primary method failed for payment terms. Trying fallback."
                        )
                        fallback_terms = fallback_find_payment_terms(
                            S3_BUCKET_NAME, file_name
                        )
                        invoice_data["Payment Terms"] = (
                            fallback_terms or "Not available"
                        )
                    else:
                        # If primary method worked, ensure "Not available" is not the final value
                        invoice_data["Payment Terms"] = (
                            invoice_data.get("Payment Terms") or "Not available"
                        )

                display_results(file_name, parsed_data)
            else:
                logging.error(
                    f"Failed to get a valid response from Textract for {file_name}. Skipping."
                )

        except FileNotFoundError:
            logging.error(f"Error: The file was not found at {file_path}")
        except ClientError as e:
            logging.error(f"An AWS client error occurred: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        finally:
            # Clean up the S3 object in all cases 
            try:
                logging.info(f"Deleting {file_name} from S3 bucket {S3_BUCKET_NAME}.")
                s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=file_name)
            except ClientError as e:
                logging.warning(f"Could not delete {file_name} from S3: {e}")

        logging.info(f"--- Finished processing for {file_name} ---")
        print("-"*10 + "#"*100 + "-"*10 + "\n")
        
 


if __name__ == "__main__":
    main()
