from flask import Flask, request, send_file, render_template_string
import pandas as pd
from io import BytesIO
import zipfile
import os
from openpyxl import Workbook

app = Flask(__name__)

CATEGORY_KEYWORDS = ['dz', 'fancy', 'sold dz', 'sold fancy', 'sold d-z']

# ✅ Store processed ZIP in memory
processed_zip_data = None


def normalize_columns(df):
    df.columns = df.columns.str.strip().str.replace('.', '').str.replace('-', '').str.lower()
    return df


def normalize_size_range(val):
    if pd.isna(val):
        return val
    return str(val).replace('–', '-').replace('—', '-').replace(' ', '')


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

        sheet_dfs.append((df, sheet))  # ✅ no duplication

    return sheet_dfs


def process_csv(file_buffer, file_name):
    file_buffer.seek(0)
    temp_df = pd.read_csv(file_buffer, header=None)
    header_row_index = detect_header_row(temp_df)

    df = pd.read_csv(file_buffer, header=header_row_index)
    df = normalize_columns(df)

    return [(df, "Sheet1")]


def process_file(file_buffer, file_name):
    if file_name.endswith(('.xlsx', '.xls')):
        return process_excel_sheets(file_buffer, file_name)
    elif file_name.endswith('.csv'):
        return process_csv(file_buffer, file_name)
    return []


def aggregate_category(df, category_name):
    cols = df.columns

    shape_col = next((c for c in cols if 'shape' in c), None)
    size_col = next((c for c in cols if 'size' in c and 'range' in c), None)
    color_col = next((c for c in cols if 'color' in c), None)
    clarity_col = next((c for c in cols if 'clarity' in c or 'quality' in c), None)
    type_col = next((c for c in cols if 'type' in c), None)
    lab_col = next((c for c in cols if 'lab' in c), None)
    amount_col = next((c for c in cols if 'amount' in c or 'bestrateamt' in c), None)
    carat_col = next((c for c in cols if 'carat' in c or 'cts' in c), None)
    location_col = next((c for c in cols if 'location' in c or 'usa' in c), None)

    if amount_col:
        df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)
    if carat_col:
        df[carat_col] = pd.to_numeric(df[carat_col], errors='coerce').fillna(0)

    if size_col:
        df[size_col] = df[size_col].apply(normalize_size_range)

    df['Location'] = df[location_col].apply(
        lambda x: 'USA' if location_col and 'usa' in str(x).lower() else 'India'
    ) if location_col else 'India'

    filtered_df = df[df.apply(lambda r: category_name.lower() in r.to_string().lower(), axis=1)]
    if filtered_df.empty:
        return pd.DataFrame()

    group_cols = [c for c in [shape_col, size_col, color_col, clarity_col, type_col, lab_col, 'Location'] if c]

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

    grouped_df.rename(columns={k: v for k, v in rename_map.items() if k in grouped_df.columns}, inplace=True)

    final_cols = ['Shape','Size Range','Color','Clarity','Lab','Type','Location','Count','Carat','Amount']
    for col in final_cols:
        if col not in grouped_df.columns:
            grouped_df[col] = ''

    return grouped_df[final_cols]


def write_custom_excel(df):
    wb = Workbook()
    ws = wb.active

    # ✅ TOTAL row first
    total_row = []
    for col in df.columns:
        if col in ['Count', 'Carat', 'Amount']:
            total_row.append(pd.to_numeric(df[col], errors='coerce').sum())
        else:
            total_row.append('TOTAL')

    ws.append(total_row)

    # ✅ Header second
    ws.append(list(df.columns))

    # ✅ Data
    for _, row in df.iterrows():
        ws.append(row.tolist())

    stream = BytesIO()
    wb.save(stream)
    stream.seek(0)
    return stream


@app.route('/', methods=['GET', 'POST'])
def upload():
    global processed_zip_data

    html = '''
    <h2>Upload Stock Excel or CSV Files</h2>
    <form method="post" enctype="multipart/form-data">
      <input type="file" name="files" multiple accept=".xlsx,.xls,.csv">
      <br><br>
      <input type="submit" value="Upload">
    </form>
    '''

    if request.method == 'POST':
        uploaded_files = request.files.getlist("files")
        output_zip = BytesIO()

        preview_html = "<h3>✅ Files processed successfully</h3>"
        preview_html += "<h4>🔍 Preview (first 10 rows)</h4>"

        with zipfile.ZipFile(output_zip, 'w') as zipf:
            for f in uploaded_files:
                sheets = process_file(f, f.filename)

                for df, sheet_name in sheets:
                    for cat in CATEGORY_KEYWORDS:
                        cat_df = aggregate_category(df, cat)

                        if not cat_df.empty:
                            # ✅ clean filename
                            base_file = os.path.splitext(f.filename)[0]
                            base_file = base_file.replace('(1)', '').strip()

                            clean_sheet = os.path.splitext(sheet_name)[0].strip()

                            out_file = f"{base_file} {clean_sheet}.xlsx"
                            out_file = " ".join(out_file.split())

                            # ✅ preview
                            preview_html += f"<h5>{out_file}</h5>"
                            preview_html += cat_df.head(10).to_html(index=False, border=1)

                            # ✅ write file
                            excel_stream = write_custom_excel(cat_df)
                            zipf.writestr(out_file, excel_stream.getvalue())

        output_zip.seek(0)
        processed_zip_data = output_zip.getvalue()

        preview_html += '''
        <br><br>
        <form action="/download" method="get">
            <button type="submit">⬇️ Download ZIP</button>
        </form>
        '''

        return preview_html

    return render_template_string(html)


@app.route('/download', methods=['GET'])
def download():
    global processed_zip_data

    if not processed_zip_data:
        return "No file available. Please upload first."

    return send_file(
        BytesIO(processed_zip_data),
        download_name="processed_files.zip",
        as_attachment=True
    )


if __name__ == "__main__":
    app.run(debug=True)
