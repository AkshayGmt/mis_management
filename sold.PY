from flask import Flask, request, send_file, render_template_string
import pandas as pd
from io import BytesIO
import zipfile
import os
import uuid

app = Flask(__name__)

GROUP_COLS = ["Shape", "Size Range", "Color", "Clarity", "Lab", "Type"]

ZIP_STORE = {}

def safe_col(df, col_name):
    for c in df.columns:
        if c.lower() == col_name.lower():
            return df[c]
    return ""

def process_file(file_stream, filename):

    df = pd.read_excel(file_stream, header=4)
    df.columns = df.columns.str.strip()

    # detect color column
    color_col = None
    for col in df.columns:
        if col.lower() == "color":
            color_col = col
            break

    if color_col is None:
        raise ValueError(f"'Color' column not found in {filename}")

    # ✅ FORCE COLOR UPPERCASE
    df[color_col] = (
        df[color_col]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # ✅ FIXED FANCY FILTER
    fancy_df = df[df[color_col].str.startswith("FANCY")].copy()
    dz_df = df[~df.index.isin(fancy_df.index)].copy()

    def build_grouped(data, label):

        temp = pd.DataFrame()

        temp["Shape"] = safe_col(data, "Shape")
        temp["Size Range"] = safe_col(data, "Size Range")
        temp["Color"] = data[color_col]
        temp["Clarity"] = safe_col(data, "Clarity")
        temp["Lab"] = safe_col(data, "Lab")
        temp["Type"] = safe_col(data, "Type")
        temp["Location"] = safe_col(data, "Location")

        # ✅ extra safety uppercase
        temp["Color"] = temp["Color"].astype(str).str.upper()

        temp["Carat"] = pd.to_numeric(safe_col(data, "Carat"), errors="coerce").fillna(0)
        temp["Amount"] = pd.to_numeric(safe_col(data, "Amount"), errors="coerce").fillna(0)

        temp["Category"] = label
        temp = temp.fillna("")

        grouped = temp.groupby(
            ["Category"] + GROUP_COLS + ["Location"],
            dropna=False
        ).agg(
            Count=("Shape", "size"),
            Carat=("Carat", "sum"),
            Amount=("Amount", "sum")
        ).reset_index()

        return grouped

    return build_grouped(fancy_df, "FANCY"), build_grouped(dz_df, "D-Z")


# ---------------- EXCEL FORMAT FIX ----------------
def write_excel_with_total(df):

    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:

        sheet_name = "Report"

        # TOTAL ROW
        total_row = {col: "" for col in df.columns}
        total_row["Category"] = "TOTAL"

        for col in ["Count", "Carat", "Amount"]:
            if col in df.columns:
                total_row[col] = df[col].sum()

        total_df = pd.DataFrame([total_row])

        # DATA (start row 3)
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=2, header=False)

        ws = writer.sheets[sheet_name]

        # HEADER (row 2)
        for c, name in enumerate(df.columns, 1):
            ws.cell(row=2, column=c, value=name)

        # TOTAL (row 1)
        for c, name in enumerate(df.columns, 1):
            ws.cell(row=1, column=c, value=total_df.iloc[0][name])

    output.seek(0)
    return output


# ---------------- UI ----------------
@app.route("/", methods=["GET", "POST"])
def upload():

    html = """
    <html>
    <head>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>

    <body class="container mt-4">

        <h2 class="text-primary">Diamond MIS Report Generator</h2>

        <form method="post" enctype="multipart/form-data" class="card p-3 mt-3">
            <input class="form-control" type="file" name="files" multiple>
            <br>
            <button class="btn btn-success">Generate Review</button>
        </form>

        {% if preview %}

            <h3 class="mt-4">Review Output</h3>

            {% for item in preview %}
                <h5>{{ item.name }}</h5>
                {{ item.table|safe }}
            {% endfor %}

            <a href="/download/{{ uid }}" class="btn btn-primary mt-3">
                Download ZIP
            </a>

        {% endif %}

    </body>
    </html>
    """

    if request.method == "POST":

        files = request.files.getlist("files")

        output_zip = BytesIO()
        preview_data = []

        with zipfile.ZipFile(output_zip, "w") as zipf:

            for file in files:
                try:
                    fancy_df, dz_df = process_file(file, file.filename)
                    base = os.path.splitext(file.filename)[0]

                    # preview
                    preview_data.append({
                        "name": f"{base} - FANCY",
                        "table": fancy_df.head(10).to_html(classes="table table-bordered")
                    })

                    preview_data.append({
                        "name": f"{base} - D-Z",
                        "table": dz_df.head(10).to_html(classes="table table-bordered")
                    })

                    # Excel outputs
                    buf1 = write_excel_with_total(fancy_df)
                    zipf.writestr(f"{base}_FANCY_REPORT.xlsx", buf1.getvalue())

                    buf2 = write_excel_with_total(dz_df)
                    zipf.writestr(f"{base}_DZ_REPORT.xlsx", buf2.getvalue())

                except Exception as e:
                    zipf.writestr(f"{file.filename}_ERROR.txt", str(e))

        output_zip.seek(0)

        uid = str(uuid.uuid4())
        ZIP_STORE[uid] = output_zip

        return render_template_string(html, preview=preview_data, uid=uid)

    return render_template_string(html, preview=None)


@app.route("/download/<uid>")
def download(uid):
    return send_file(
        ZIP_STORE[uid],
        download_name="MIS_GROUPED_REPORT.zip",
        as_attachment=True
    )


if __name__ == "__main__":
    app.run(debug=True)
