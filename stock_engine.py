import pandas as pd
import zipfile
import os
from io import BytesIO


# =========================================================
# 🔥 YOUR ORIGINAL CONFIG (UNCHANGED)
# =========================================================

HEADER_KEYWORDS = {
    'shape': ['shape'],
    'size_range': ['size', 'range', 'size range'],
    'color': ['color'],
    'clarity': ['clarity', 'quality'],
    'lab': ['lab'],
    'type': ['type'],
    'amount': ['amount', 'bestrateamt', 'rap amt', 'rap $/ct'],
    'carat': ['carat', 'cts'],
    'location': ['location', 'usa']
}

CATEGORY_KEYWORDS = ['dz', 'fancy', 'sold dz', 'sold fancy', 'sold d-z']


# =========================================================
# 🔥 CORE HELPERS (UNCHANGED LOGIC)
# =========================================================

def normalize_columns(df):
    df.columns = df.columns.str.strip().str.replace('.', '').str.replace('-', '').str.lower()
    return df


def normalize_size_range(val):
    if pd.isna(val):
        return val
    s = str(val)
    s = s.replace('–', '-')
    s = s.replace('—', '-')
    s = s.replace(' ', '')
    return s


def detect_header_row(temp_df):
    for i in range(min(10, len(temp_df))):
        row = temp_df.iloc[i].astype(str).str.lower().str.strip().tolist()
        if any('size' in col and 'range' in col for col in row):
            return i
    return 0


def process_excel_sheets(file_buffer, file_name):
    file_buffer.seek(0)
    xls = pd.ExcelFile(file_buffer)
    sheet_dfs = []

    for sheet in xls.sheet_names:
        temp_df = pd.read_excel(xls, sheet_name=sheet, header=None)
        header_row_index = detect_header_row(temp_df)
        df = pd.read_excel(xls, sheet_name=sheet, header=header_row_index)
        df = normalize_columns(df)
        sheet_dfs.append((df, f"{file_name}_{sheet}"))

    return sheet_dfs


def process_csv(file_buffer, file_name):
    file_buffer.seek(0)
    temp_df = pd.read_csv(file_buffer, header=None)
    header_row_index = detect_header_row(temp_df)
    df = pd.read_csv(file_buffer, header=header_row_index)
    df = normalize_columns(df)
    return [(df, file_name)]


def process_file(file_buffer, file_name):
    if file_name.endswith(('.xlsx', '.xls')):
        return process_excel_sheets(file_buffer, file_name)
    elif file_name.endswith('.csv'):
        return process_csv(file_buffer, file_name)
    return []


# =========================================================
# 🔥 MAIN AGGREGATION (UNCHANGED LOGIC)
# =========================================================

def aggregate_category(df, category_name):

    normalized_cols = {
        col: col.strip().replace('.', '').replace('-', '').lower()
        for col in df.columns
    }

    shape_col = next((c for c, n in normalized_cols.items() if 'shape' in n), None)
    size_col = next((c for c, n in normalized_cols.items() if 'size' in n and 'range' in n), None)
    color_col = next((c for c, n in normalized_cols.items() if 'color' in n), None)
    clarity_col = next((c for c, n in normalized_cols.items() if 'clarity' in n or 'quality' in n), None)
    type_col = next((c for c, n in normalized_cols.items() if 'type' in n), None)
    lab_col = next((c for c, n in normalized_cols.items() if 'lab' in n), None)
    amount_col = next((c for c, n in normalized_cols.items() if 'amount' in n or 'bestrateamt' in n), None)
    carat_col = next((c for c, n in normalized_cols.items() if 'carat' in n or 'cts' in n), None)
    location_col = next((c for c, n in normalized_cols.items() if 'location' in n or 'usa' in n), None)

    if amount_col:
        df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)

    if carat_col:
        df[carat_col] = pd.to_numeric(df[carat_col], errors='coerce').fillna(0)

    if size_col:
        df[size_col] = df[size_col].apply(normalize_size_range)

    df['Location'] = 'India'

    filtered_df = df[df.apply(lambda r: category_name.lower() in r.to_string().lower(), axis=1)]
    if filtered_df.empty:
        return pd.DataFrame()

    group_cols = [
        c for c in [shape_col, size_col, color_col, clarity_col, type_col, lab_col, 'Location']
        if c
    ]

    agg_dict = {}
    if carat_col:
        agg_dict[carat_col] = 'sum'
    if amount_col:
        agg_dict[amount_col] = 'sum'

    grouped_df = filtered_df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()

    grouped_df['Count'] = filtered_df.groupby(group_cols).size().values

    rename_map = {
        shape_col: 'Shape',
        size_col: 'Size Range',
        color_col: 'Color',
        clarity_col: 'Clarity',
        type_col: 'Type',
        lab_col: 'Lab',
        carat_col: 'Carat',
        amount_col: 'Amount'
    }

    grouped_df = grouped_df.rename(columns={k: v for k, v in rename_map.items() if k in grouped_df.columns})

    final_cols = ['Shape','Size Range','Color','Clarity','Lab','Type','Location','Count','Carat','Amount']

    for col in final_cols:
        if col not in grouped_df.columns:
            grouped_df[col] = ''

    grouped_df = grouped_df[final_cols]

    # TOTAL ROW
    total_row = grouped_df.copy()

    for col in ['Count', 'Carat', 'Amount']:
        if col in total_row.columns:
            total_row[col] = pd.to_numeric(total_row[col], errors='coerce').sum()

    for col in ['Shape','Size Range','Color','Clarity','Lab','Type','Location']:
        total_row[col] = 'TOTAL'

    grouped_df = pd.concat([total_row.head(1), grouped_df], ignore_index=True)

    return grouped_df


# =========================================================
# 🚀 MAIN ENGINE FUNCTION (USED BY app.py)
# =========================================================

def run_stock(files):

    output_zip = BytesIO()

    with zipfile.ZipFile(output_zip, 'w') as zipf:

        for f in files:
            sheets = process_file(f, f.filename)

            for df, sheet_name in sheets:

                for cat in CATEGORY_KEYWORDS:
                    cat_df = aggregate_category(df, cat)

                    if not cat_df.empty:

                        base_file = os.path.splitext(f.filename)[0]
                        out_file = f"{base_file}_{sheet_name}_{cat.upper()}.xlsx"

                        mem = BytesIO()
                        cat_df.to_excel(mem, index=False)

                        zipf.writestr(out_file, mem.getvalue())

    output_zip.seek(0)
    return output_zip
