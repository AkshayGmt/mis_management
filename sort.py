from flask import Flask, request, send_file, render_template_string
import pandas as pd
from io import BytesIO
import zipfile
import re

app = Flask(__name__)

# ---------------- ORDER TABLES ---------------- #

SHAPE_ORDER = [
    "ROUND","ROU.MOD.","OVAL","OV.MOD.","STEP OVAL",
    "PEAR","PE.MOD.","STEP PEAR",
    "MARQUISE","MQ.MOD","STEP MARQUISE",
    "PRINCESS","RAD","SQ.RAD",
    "EMERALD","ASSCHER","SQ.CUS.","SQ.CUS.MOD.","LO.CUS.",
    "HEXAGON","HEART"
]

COLOR_ORDER = ["D","E","F","G","H","I","J","K","L","M","N","O"]

CLARITY_ORDER = ["FL","IF","VVS1","VVS2","VS1","VS2","SI1","SI2","SI3"]

# ---------------- CLEAN FUNCTIONS ---------------- #

def clean_text(x):
    if pd.isna(x) or str(x).strip() == "":
        return "0"
    return str(x).strip().upper()

# ✅ IMPROVED COLOR NORMALIZATION
def normalize_color(x):
    if pd.isna(x):
        return "0"
    x = str(x).upper().strip()

    # Remove non-letters (/, +, spaces, etc.)
    x = re.sub(r'[^A-Z]', '', x)

    # If multiple letters (like GH), take first
    if len(x) > 1:
        x = x[0]

    return x

# ✅ FIXED CLARITY NORMALIZATION
def normalize_clarity(x):
    if pd.isna(x):
        return "0"
    x = str(x).upper().strip()
    x = re.sub(r'[^A-Z0-9]', '', x)
    return x

# ✅ SIZE PARSER
def size_max_value(x):
    if pd.isna(x) or str(x).strip() == "":
        return -1

    x = str(x).upper().strip()
    x = x.replace("–", "-").replace("—", "-").replace(" ", "")

    nums = re.findall(r"\d+\.?\d*", x)

    if len(nums) >= 2:
        return float(nums[1])
    elif len(nums) == 1:
        return float(nums[0])

    return -1

# ---------------- SMART SORT ---------------- #

def apply_sort(df):

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # ---------------- SHAPE ---------------- #
    if "shape" in df.columns:
        df["shape_clean"] = df["shape"].apply(clean_text)
        df["shape_rank"] = df["shape_clean"].apply(
            lambda x: SHAPE_ORDER.index(x) if x in SHAPE_ORDER else 9999
        )
    else:
        df["shape_rank"] = 9999

    # ---------------- SIZE ---------------- #
    if "size range" in df.columns:
        df["size_key"] = df["size range"].apply(size_max_value)
    else:
        df["size_key"] = -1

    # ---------------- COLOR (FIXED) ---------------- #
    if "color" in df.columns:
        df["color_clean"] = df["color"].apply(normalize_color)
        df["color_rank"] = df["color_clean"].apply(
            lambda x: COLOR_ORDER.index(x) if x in COLOR_ORDER else 9999
        )
    else:
        df["color_rank"] = 9999

    # ---------------- CLARITY (FIXED) ---------------- #
    if "clarity" in df.columns:
        df["clarity_clean"] = df["clarity"].apply(normalize_clarity)
        df["clarity_rank"] = df["clarity_clean"].apply(
            lambda x: CLARITY_ORDER.index(x) if x in CLARITY_ORDER else 9999
        )
    else:
        df["clarity_rank"] = 9999

    # ---------------- FINAL SORT ---------------- #
    df = df.sort_values(
        by=["shape_rank", "size_key", "color_rank", "clarity_rank"],
        ascending=[True, False, True, True]
    )

    # ---------------- ROUND NUMERIC ---------------- #
    for col in df.columns:
        col_lower = col.lower()

        if "avg" in col_lower or "average" in col_lower:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

        if "amount" in col_lower or "amt" in col_lower:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # ---------------- FILL MISSING ---------------- #
    df = df.fillna("0")

    # ---------------- DROP TEMP ---------------- #
    df.drop(
        columns=[c for c in df.columns if any(k in c for k in ["rank", "clean", "key"])],
        inplace=True,
        errors="ignore"
    )

    return df

# ---------------- HTML ---------------- #

HTML = """
<h2>Universal Multi File Sorter</h2>

<form method="post" enctype="multipart/form-data">
    <input type="file" name="files" multiple required>
    <button type="submit">Sort Files</button>
</form>

<p>Upload Excel/CSV → Download sorted ZIP</p>
"""

# ---------------- ROUTE ---------------- #

@app.route("/", methods=["GET", "POST"])
def home():

    if request.method == "POST":

        files = request.files.getlist("files")
        memory = BytesIO()

        with zipfile.ZipFile(memory, "w") as z:

            for f in files:

                if f.filename.lower().endswith(".csv"):
                    df = pd.read_csv(f)
                else:
                    df = pd.read_excel(f)

                df = apply_sort(df)

                out = BytesIO()
                df.to_excel(out, index=False)
                out.seek(0)

                z.writestr(f"sorted_{f.filename}.xlsx", out.read())

        memory.seek(0)

        return send_file(
            memory,
            download_name="sorted_output.zip",
            as_attachment=True
        )

    return render_template_string(HTML)

# ---------------- RUN ---------------- #

if __name__ == "__main__":
    app.run(debug=True)
