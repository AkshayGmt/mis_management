import pandas as pd
import xlsxwriter
from datetime import datetime
import re

# --- Read Excel ---
input_file = "count_data.xlsx"
df = pd.read_excel(input_file, header=4)

# --- Clean columns ---
df.columns = df.columns.str.strip()

# --- Numeric cleanup ---
numeric_cols = ['Total_Count', 'Count', 'Total_Avg_Pr_Ct', 'Avg_Pr_Ct']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# --- Clean text ---
for col in ['Size Range', 'Color', 'Clarity']:
    df[col] = df[col].astype(str).str.strip()

# --- Size Range order (your custom order) ---
size_order = [
        "10.000 - 10.999",
        "9.000 - 9.999",
        "8.000 - 8.999",
        "7.000 - 7.999",
        "6.000 - 6.999",
        "5.500 - 5.999",
        "5.000 - 5.499",
        "4.500 - 4.749",
        "4.250 - 4.499",
        "4.000 - 4.249",
        "3.750 - 3.999",
        "3.500 - 3.749",
        "3.250 - 3.499",
        "3.000 - 3.249",
        "2.750 - 2.999",
        "2.500 - 2.749",
        "2.250 - 2.499",
        "2.000 - 2.249",
        "1.900 - 1.999",
        "1.800 - 1.899",
        "1.700 - 1.799",
        "1.600 - 1.699",
        "1.500 - 1.599",
        "1.400 - 1.499",
        "1.300 - 1.399",
        "1.200 - 1.299",
        "1.100 - 1.199",
        "1.000 - 1.099",
        "0.960 - 0.999",
        "0.900 - 0.959",
        "0.700 - 0.799",
        "0.500 - 0.599",
        "0.400 - 0.459"
    ]
df['Size Range'] = pd.Categorical(df['Size Range'], categories=size_order, ordered=True)

# --- Color and Clarity order (use your custom order) ---
color_order = ['D', 'E', 'F', 'PRE', 'STD', 'G', 'I', 'YELLOW',
               'FANCY VIVID BLUE', 'FANCY VIVID PINK',
               'FANCY INTENSE PINK', 'FANCY INTENSE YELLOW', "FANCY INTENSE BLUE"]
clarity_order = ["FL", 'IF', 'VVS1', 'VVS2', 'VS1', 'VS2', 'SI1', 'PRE', 'STD']

df['Color'] = pd.Categorical(df['Color'], categories=color_order, ordered=True)
df['Clarity'] = pd.Categorical(df['Clarity'], categories=clarity_order, ordered=True)

# --- Group by Size Range, Color, Clarity ---
grouped = df.groupby(['Size Range', 'Color', 'Clarity'], observed=False).agg(
    Total_Count=('Total_Count', 'sum'),
    Count=('Count', 'sum'),
    Total_Avg_Pr_Ct_Sum=('Total_Avg_Pr_Ct', lambda x: (x * df.loc[x.index, 'Total_Count']).sum()),
    Avg_Pr_Ct_Sum=('Avg_Pr_Ct', lambda x: (x * df.loc[x.index, 'Count']).sum())
).reset_index()

# --- Weighted averages ---
grouped['Total_Avg_Pr_Ct'] = grouped.apply(
    lambda r: r['Total_Avg_Pr_Ct_Sum'] / r['Total_Count'] if r['Total_Count'] != 0 else 0, axis=1
)

grouped['Avg_Pr_Ct'] = grouped.apply(
    lambda r: r['Avg_Pr_Ct_Sum'] / r['Count'] if r['Count'] != 0 else 0, axis=1
)

# --- Final summary table ---
summary = grouped[['Size Range', 'Color', 'Clarity', 'Total_Count', 'Count', 'Total_Avg_Pr_Ct', 'Avg_Pr_Ct']]

# --- Remove rows with zero counts ---
summary = summary[(summary['Total_Count'] != 0) | (summary['Count'] != 0)]

# --- Output file ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f'size_range_color_clarity_report_{timestamp}.xlsx'
workbook = xlsxwriter.Workbook(output_file)

# --- Create one worksheet per Size Range (ordered) ---
for size in size_order:
    size_df = summary[summary['Size Range'] == size].copy()
    if size_df.empty:
        continue

    size_df['Label'] = size_df['Color'].astype(str) + "-" + size_df['Clarity'].astype(str)
    sheet_name = str(size)[:31]  # Excel sheet name limit

    worksheet = workbook.add_worksheet(sheet_name)
    headers = ['Label', 'Color', 'Clarity', 'Total_Count', 'Count', 'Total_Avg_Pr_Ct', 'Avg_Pr_Ct']
    worksheet.write_row('A1', headers)

    # Write data rows
    for row_num, row in enumerate(size_df.itertuples(index=False), start=1):
        worksheet.write_row(row_num, 0, [
            row.Label,
            row.Color,
            row.Clarity,
            int(row.Total_Count),
            int(row.Count),
            round(row.Total_Avg_Pr_Ct, 2),
            round(row.Avg_Pr_Ct, 2),
        ])

    # --- Create chart ---
    chart = workbook.add_chart({'type': 'column'})
    last_row = len(size_df)
    categories = f'={sheet_name}!$A$2:$A${last_row+1}'

    # Add count series on primary y-axis
    chart.add_series({
        'name': 'Total_Count',
        'categories': categories,
        'values': f'={sheet_name}!$D$2:$D${last_row+1}',
    })
    chart.add_series({
        'name': 'Count',
        'categories': categories,
        'values': f'={sheet_name}!$E$2:$E${last_row+1}',
    })

    # Add average price series on secondary y-axis
    chart.add_series({
        'name': 'Total_Avg_Pr_Ct',
        'categories': categories,
        'values': f'={sheet_name}!$F$2:$F${last_row+1}',
        'y2_axis': True,
    })
    chart.add_series({
        'name': 'Avg_Pr_Ct',
        'categories': categories,
        'values': f'={sheet_name}!$G$2:$G${last_row+1}',
        'y2_axis': True,
    })

    chart.set_title({'name': f'Size Range: {size} - Color & Clarity Analysis'})
    chart.set_x_axis({'num_font': {'rotation': -45}})
    chart.set_y_axis({'name': 'Count'})
    chart.set_y2_axis({'name': 'Price'})
    chart.set_legend({'position': 'top'})

    worksheet.insert_chart('I2', chart, {'x_scale': 1.5, 'y_scale': 1.5})

workbook.close()
print(f"✅ File created: {output_file}")