from flask import Flask, request, send_file, render_template_string
import pandas as pd
from io import BytesIO

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

SIZE_ORDER = [
    "10.000-10.999","9.000-9.999","8.000-8.999","7.000-7.999",
    "6.000-6.999","5.500-5.999","5.000-5.499","4.500-4.749",
    "4.250-4.499","4.000-4.249","3.750-3.999","3.500-3.749",
    "3.250-3.499","3.000-3.249","2.750-2.999","2.500-2.749",
    "2.250-2.499","2.000-2.249","1.900-1.999","1.800-1.899",
    "1.700-1.799","1.600-1.699","1.500-1.599","1.400-1.499",
    "1.300-1.399","1.200-1.299","1.100-1.199","1.000-1.099",
    "0.960-0.999","0.900-0.959","0.700-0.799","0.500-0.599","0.400-0.459"
]

# ---------------- CLEAN FUNCTIONS ---------------- #

def clean_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip().upper()

def normalize_size(x):
    if pd.isna(x):
        return ""

    x = str(x).upper().strip()

    # FIX ALL ISSUES:
    x = x.replace(" ", "")
    x = x.replace("–", "-")
    x = x.replace("—", "-")

    return x


# ---------------- SMART SORT ---------------- #

def apply_sort(df):

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
        df["size_clean"] = df["size range"].apply(normalize_size)

        df["size_rank"] = df["size_clean"].apply(
            lambda x: SIZE_ORDER.index(x) if x in SIZE_ORDER else 9999
        )

    else:
        df["size_rank"] = 9999

    # ---------------- CLARITY (optional safe) ---------------- #
    if "clarity" in df.columns:
        df["clarity"] = df["clarity"].apply(clean_text)

    # ---------------- COLOR ---------------- #
    if "color" in df.columns:
        df["color"] = df["color"].apply(clean_text)

    # ---------------- FINAL SORT ---------------- #
    sort_cols = ["shape_rank", "size_rank"]

    df = df.sort_values(sort_cols)

    # CLEAN TEMP COLUMNS
    df.drop(columns=[c for c in df.columns if "rank" in c or "_clean" in c], inplace=True, errors="ignore")

    return df


# ---------------- HTML ---------------- #

HTML = """
<h2>Universal Multi File Sorter</h2>

<form method="post" enctype="multipart/form-data">
    <input type="file" name="files" multiple required>
    <button type="submit">Sort Files</button>
</form>

<p>Upload multiple Excel/CSV → Download sorted files ZIP</p>
"""

# ---------------- PROCESS MULTI FILE ---------------- #

@app.route("/", methods=["GET", "POST"])
def home():

    if request.method == "POST":

        files = request.files.getlist("files")

        memory = BytesIO()
        import zipfile

        with zipfile.ZipFile(memory, "w") as z:

            for f in files:

                if f.filename.endswith(".csv"):
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
