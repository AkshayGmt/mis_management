import pandas as pd
import xlsxwriter
from datetime import datetime
import numpy as np

# --- Read Excel ---
input_file = "ECO STAR D-Z COLOR.xlsx"
df = pd.read_excel(input_file, header=4)

# --- Clean columns ---
df.columns = df.columns.str.strip()
print("Columns detected:", df.columns.tolist())

# --- Determine Carat column ---
if 'Carat' in df.columns:
    carat_col = 'Carat'
elif 'Total Carat' in df.columns:
    carat_col = 'Total Carat'
else:
    carat_col = 'Carat'
    df[carat_col] = 0
    print("⚠️ No carat column found, defaulting to 0.")

# --- Numeric cleanup ---
numeric_cols = ['Total Stock','Stock Pr/Ct','Sold','Sold Pr/Ct', carat_col]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# --- Fill quantity columns with 0 ---
df['Total Stock'] = df['Total Stock'].fillna(0)
df['Sold'] = df['Sold'].fillna(0)
df[carat_col] = df[carat_col].fillna(0)

# --- Clean text ---
for col in ['Shape','Color','Clarity']:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    else:
        df[col] = 'Unknown'

# --- CUSTOM ORDER ---
shape_order = ["ROUND", "ROU.MOD.", "OVAL", "OV.MOD.", "PEAR", "PE.MOD.", 
               "MARQUISE", "MQ.MOD", "PRINCESS", "RAD", "SQ.RAD", 
               "EMERALD", "SQ.CUS.", "SQ.CUS.MOD.", "LO.CUS.", "ASSCHER", 
               "HEXAGON", "HEART"]

color_order = ['D', 'E', 'F', 'PRE', 'STD','G', 'I', 'YELLOW',
               'FANCY VIVID BLUE', 'FANCY VIVID PINK',
               'FANCY INTENSE PINK', 'FANCY INTENSE YELLOW',"FANCY INTENSE BLUE"]

clarity_order = ["FL",'IF', 'VVS1', 'VVS2', 'VS1', 'VS2', 'SI1', 'PRE', 'STD']

# --- APPLY ORDER ---
df['Shape'] = pd.Categorical(df['Shape'], categories=shape_order, ordered=True)
df['Color'] = pd.Categorical(df['Color'], categories=color_order, ordered=True)
df['Clarity'] = pd.Categorical(df['Clarity'], categories=clarity_order, ordered=True)

# --- GROUP BY SHAPE, COLOR, CLARITY ---
grouped = df.groupby(['Shape','Color','Clarity'], observed=False).agg(
    Total_Stock=('Total Stock', 'sum'),
    Total_Carat=(carat_col, 'sum'),
    Stock_Pr_Ct=('Stock Pr/Ct', lambda x: x[x>0].mean() if any(x>0) else 0),
    Sold=('Sold', 'sum'),
    Sold_Pr_Ct=('Sold Pr/Ct', lambda x: x[x>0].mean() if any(x>0) else 0)
).reset_index()

# --- REMOVE ZERO ROWS ---
summary = grouped[(grouped['Total_Stock'] != 0) | (grouped['Sold'] != 0)].copy()

# --- Fill NaN and replace Inf ---
numeric_cols_grouped = ['Total_Stock','Total_Carat','Stock_Pr_Ct','Sold','Sold_Pr_Ct']
summary[numeric_cols_grouped] = summary[numeric_cols_grouped].fillna(0).replace([np.inf, -np.inf], 0)

# --- OUTPUT EXCEL ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f'shape_wise_report_{timestamp}.xlsx'

with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    workbook = writer.book

    for shape in shape_order:
        shape_df = summary[summary['Shape'] == shape].copy()
        if shape_df.empty:
            continue

        shape_df['Label'] = shape_df['Color'].astype(str) + "-" + shape_df['Clarity'].astype(str)
        sheet_name = str(shape)[:31]
        shape_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)

        worksheet = writer.sheets[sheet_name]

        headers = ['Label','Color','Clarity','Total Stock','Total Carat','Stock Pr/Ct','Sold','Sold Pr/Ct']
        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header)

        # --- Chart ---
        chart = workbook.add_chart({'type': 'column'})
        last_row = len(shape_df) + 1
        categories = f'={sheet_name}!$A$2:$A${last_row}'

        chart.add_series({'name': 'Total Stock','categories': categories,'values': f'={sheet_name}!$D$2:$D${last_row}'})
        chart.add_series({'name': 'Total Carat','categories': categories,'values': f'={sheet_name}!$E$2:$E${last_row}'})
        chart.add_series({'name': 'Sold','categories': categories,'values': f'={sheet_name}!$G$2:$G${last_row}'})
        chart.add_series({'name': 'Stock Pr/Ct','categories': categories,'values': f'={sheet_name}!$F$2:$F${last_row}','y2_axis': True})
        chart.add_series({'name': 'Sold Pr/Ct','categories': categories,'values': f'={sheet_name}!$H$2:$H${last_row}','y2_axis': True})

        chart.set_title({'name': f'{shape} - Analysis'})
        chart.set_x_axis({'num_font': {'rotation': -45}})
        chart.set_y_axis({'name': 'Stock/Sold/Carat'})
        chart.set_y2_axis({'name': 'Price per Ct'})
        chart.set_legend({'position': 'top'})
        worksheet.insert_chart('J2', chart, {'x_scale': 1.5, 'y_scale': 1.5})

print(f"✅ File created: {output_file}")
print("Final row count:", len(summary))