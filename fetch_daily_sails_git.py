import os
import sys
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
import argparse

# -------------------------
# CONFIG
# -------------------------

# Tokens now come from environment variables
FOOD_TOKEN = os.getenv("FOOD_TOKEN")
BAR_TOKEN = os.getenv("BAR_TOKEN")

if not FOOD_TOKEN or not BAR_TOKEN:
    raise RuntimeError("FOOD_TOKEN and/or BAR_TOKEN environment variables are not set.")

# Base dir = repo root in GitHub Actions (current working dir)
BASE_DIR = Path(__file__).resolve().parent

ARCHIVE_FOLDER = BASE_DIR / "GoodtillSalesArchive"
ARCHIVE_FOLDER.mkdir(parents=True, exist_ok=True)


# -------------------------
# API Fetch Functions
# -------------------------

def fetch_all_sales_for_token(token, start_date, end_date, limit=50):
    headers = {"Authorization": f"Bearer {token}"}
    all_sales = []
    offset = 0

    while True:
        params = {
            "from": start_date,
            "to": end_date,
            "limit": limit,
            "offset": offset
        }

        resp = requests.get(
            "https://api.thegoodtill.com/api/external/get_sales_details",
            headers=headers,
            params=params
        )
        resp.raise_for_status()
        data = resp.json()

        # Handle dict or list response
        batch = data.get("data", []) if isinstance(data, dict) else data

        if not batch:
            break

        all_sales.extend(batch)

        if len(batch) < limit:
            break

        offset += limit

    return all_sales


def get_range_for_date(target_date: date):
    start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
    end   = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)
    return (
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S"),
        target_date
    )


def convert_sales_api_to_dataframe(json_data):
    """
    Convert nested sales API response to a flat DataFrame.
    Each row represents a product in a sale with all relevant financial information.
    """

    # Handle both full response object and direct list
    if isinstance(json_data, list):
        sales_list = json_data
    elif isinstance(json_data, dict) and 'data' in json_data:
        sales_list = json_data['data']
    else:
        raise ValueError("Expected dict with 'data' key or list of sales")

    if not sales_list:
        raise ValueError("No sales data found")

    rows = []

    # Process each sale
    for sale in sales_list:
        sales_items = sale.get('sales_details', {}).get('sales_items', [])

        for item in sales_items:
            # Get payment information
            payment_methods = list(sale.get('sales_payments', {}).keys())
            payment_total = sum(
                float(p.get('payment_total', 0))
                for p in sale.get('sales_payments', {}).values()
            )

            row = {
                # Sale information
                'sale_id': sale.get('id'),
                'outlet_id': sale.get('outlet_id'),
                'outlet_name': sale.get('outlet', {}).get('outlet_name', ''),
                'register_id': sale.get('register_id'),
                'register_name': sale.get('register', {}).get('register_name', ''),
                'staff_id': sale.get('staff_id'),
                'customer_id': sale.get('customer_id'),
                'order_no': sale.get('order_no'),
                'sale_type': sale.get('sale_type'),
                'order_status': sale.get('order_status'),
                'receipt_no': sale.get('receipt_no'),
                'sales_date_time': sale.get('sales_date_time'),

                # Item information
                'item_id': item.get('id'),
                'product_id': item.get('product_id'),
                'product_name': item.get('product_name'),
                'quantity': item.get('quantity'),
                'price_inc_vat_per_item': item.get('price_inc_vat_per_item'),

                # VAT information
                'vat_rate': item.get('vat_rate'),
                'vat_rate_id': item.get('vat_rate_id'),

                # Line totals (after line discount)
                'line_total_after_line_discount': item.get('line_total_after_line_discount'),
                'line_subtotal_after_line_discount': item.get('line_subtotal_after_line_discount'),
                'line_vat_after_line_discount': item.get('line_vat_after_line_discount'),

                # Line totals (after all discounts)
                'line_total_after_discount': item.get('line_total_after_discount'),
                'line_subtotal_after_discount': item.get('line_subtotal_after_discount'),
                'line_vat_after_discount': item.get('line_vat_after_discount'),

                # Discount information
                'has_discount': item.get('has_discount'),
                'discount_amount': item.get('discount_amount'),
                'discount_is_percentage': item.get('discount_is_percentage'),
                'discount_id': item.get('discount_id'),

                # Sale-level totals
                'sale_quantity_total': sale.get('sales_details', {}).get('quantity'),
                'sale_total': sale.get('sales_details', {}).get('total'),
                'sale_subtotal': sale.get('sales_details', {}).get('total_ex_vat'),
                'sale_vat_total': sale.get('sales_details', {}).get('total_vat'),
                'sale_line_discount': sale.get('sales_details', {}).get('line_discount'),

                # Payment information
                'payment_methods': ';'.join(payment_methods),
                'payment_total': payment_total,

                # Additional metadata
                'item_notes': item.get('item_notes'),
                'sequence_no': item.get('sequence_no'),
                'created_at': item.get('created_at'),
            }

            rows.append(row)

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Convert data types for better analysis
    df['sales_date_time'] = pd.to_datetime(df['sales_date_time'])
    df['created_at'] = pd.to_datetime(df['created_at'])

    # Convert numeric columns
    numeric_columns = [
        'quantity', 'price_inc_vat_per_item', 'vat_rate',
        'line_total_after_line_discount', 'line_subtotal_after_line_discount',
        'line_vat_after_line_discount', 'line_total_after_discount',
        'line_subtotal_after_discount', 'line_vat_after_discount',
        'discount_amount', 'payment_total'
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        help="Date to fetch in YYYY-MM-DD (defaults to yesterday)"
    )
    args = parser.parse_args()

    if args.date:
        target_date = date.fromisoformat(args.date)
    else:
        target_date = date.today() - timedelta(days=1)

    start_str, end_str, yday = get_range_for_date(target_date)

    # --- Fetch FOOD data ---
    food_json = fetch_all_sales_for_token(FOOD_TOKEN, start_str, end_str)
    food_df = convert_sales_api_to_dataframe(food_json)
    food_df["Area"] = "Food"

    # --- Fetch BAR data ---
    bar_json = fetch_all_sales_for_token(BAR_TOKEN, start_str, end_str)
    bar_df = convert_sales_api_to_dataframe(bar_json)
    bar_df["Area"] = "Bar"

    # Combine both
    full_df = pd.concat([food_df, bar_df], ignore_index=True)

    # Output filename
    filename = ARCHIVE_FOLDER / f"{yday:%Y-%m-%d}_sales.csv"

    # Avoid duplicates (optional):
    if filename.exists():
        print(f"File already exists, skipping: {filename}")
    else:
        full_df.to_csv(filename, index=False)
        print(f"Saved daily sales to {filename}")


if __name__ == "__main__":
    main()
