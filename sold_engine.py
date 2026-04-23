import pandas as pd
import zipfile
import os
from io import BytesIO


# =========================================================
# 🔥 CONFIG (UNCHANGED)
# =========================================================

GROUP_COLS = ["Shape", "Size Range", "Color", "Clarity", "Lab", "Type"]


# =========================================================
# 🔥 SAFE COLUMN READER (UNCHANGED)
# =========================================================

def safe_col(df, col_name):
    for c in df.columns:
        if c.lower() == col_name.lower():
            return df[c]
    return ""


# =========================================================
# 🔥 CORE PROCESSING (UNCHANGED LOGIC)
# =========================================================

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

    # normalize color
    df[color_col] = (
        df[color_col]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    fancy_df = df[df[color_col].str.startswith("fancy")].copy()
    dz_df = df[~df.index.isin(fancy_df.index)].copy()

    # =====================================================
    # GROUP FUNCTION (UNCHANGED)
    # =====================================================

    def build_grouped(data, label):

        temp = pd.DataFrame()

        temp["Shape"] = safe_col(data, "Shape")
        temp["Size Range"] = safe_col(data, "Size Range")
        temp["Color"] = data[color_col]
        temp["Clarity"] = safe_col(data, "Clarity")
        temp["Lab"] = safe_col(data, "Lab")
        temp["Type"] = safe_col(data, "Type")
        temp["Location"] = safe_col(data, "Location")

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


# =========================================================
# 🚀 ENGINE FUNCTION (USED BY app.py)
# =========================================================

def run_sold(files):

    output_zip = BytesIO()

    with zipfile.ZipFile(output_zip, "w") as zipf:

        for file in files:

            try:
                fancy_df, dz_df = process_file(file, file.filename)
                base = os.path.splitext(file.filename)[0]

                # ---------------- FANCY OUTPUT ----------------
                buf1 = BytesIO()
                fancy_df.to_excel(buf1, index=False)
                zipf.writestr(f"{base}_FANCY_REPORT.xlsx", buf1.getvalue())

                # ---------------- DZ OUTPUT ----------------
                buf2 = BytesIO()
                dz_df.to_excel(buf2, index=False)
                zipf.writestr(f"{base}_DZ_REPORT.xlsx", buf2.getvalue())

            except Exception as e:
                zipf.writestr(f"{file.filename}_ERROR.txt", str(e))

    output_zip.seek(0)
    return output_zip
