from flask import Flask, request, jsonify, render_template
import pandas as pd
import os

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

files_data = []

# ---------------- HOME ----------------
@app.route("/")
def home():
    return render_template("in.html")


# ---------------- HEADER DETECT ----------------
def detect_header(df):
    keywords = ["shape", "color", "clarity", "location", "country", "origin"]

    for i, row in df.iterrows():
        row_values = [str(x).lower() for x in row.values]
        matches = sum(1 for k in keywords if any(k in cell for cell in row_values))
        if matches >= 2:
            return i

    return None


# ---------------- PROCESS FILE ----------------
def process_file(filepath):
    try:
        df = pd.read_excel(filepath, header=None)
        header_row = detect_header(df)

        if header_row is None:
            return None

        df = pd.read_excel(filepath, header=header_row)
        return df

    except Exception as e:
        print("ERROR:", e)
        return None


# ---------------- PROCESS ----------------
@app.route("/process")
def process_all():

    categories = ["SOLD", "TOTAL", "CURRENT"]
    kinds = ["DZ", "FANCY"]
    locations = ["USA", "INDIA"]

    result = {
        c: {k: {"USA": [], "INDIA": []} for k in kinds}
        for c in categories
    }

    # -------- FILE LOOP --------
    for f in files_data:

        df = process_file(f["path"])
        if df is None:
            continue

        category = f["type"]
        kind = f["kind"]

        cols = [c.lower().strip() for c in df.columns]

        loc_col = None
        for x in ["location", "country", "origin"]:
            if x in cols:
                loc_col = df.columns[cols.index(x)]
                break

        if loc_col is None:
            result[category][kind]["INDIA"].append(df)
            continue

        df[loc_col] = df[loc_col].astype(str).str.lower()

        df_usa = df[df[loc_col].str.contains("usa|united states|us|america", na=False)]
        df_india = df[~df[loc_col].str.contains("usa|united states|us|america", na=False)]

        if not df_usa.empty:
            result[category][kind]["USA"].append(df_usa)

        if not df_india.empty:
            result[category][kind]["INDIA"].append(df_india)

    # -------- HELPERS --------
    def find(df, names):
        for n in names:
            for c in df.columns:
                if n in c:
                    return c
        return None

    def group_df(df):
        df.columns = [c.lower().strip() for c in df.columns]

        gcols = []
        for key in ["shape","size","color","clarity","lab","type"]:
            for c in df.columns:
                if key in c:
                    gcols.append(c)
                    break

        count = find(df, ["pcs","qty","count"])
        carat = find(df, ["carat","cts","weight"])
        amount = find(df, ["amount","value","price"])

        agg = {}
        if count: agg[count] = "sum"
        if carat: agg[carat] = "sum"
        if amount: agg[amount] = "sum"

        if not gcols or not agg:
            return pd.DataFrame()

        df = df.groupby(gcols, dropna=False).agg(agg).reset_index()

        rename = {}
        if count: rename[count] = "count"
        if carat: rename[carat] = "carat"
        if amount: rename[amount] = "amount"

        return df.rename(columns=rename)

    final_output = {}

    # -------- MAIN LOOP --------
    for k in kinds:
        final_output[k] = {}

        for l in locations:

            merged = None

            for c in categories:

                dfs = result[c][k][l]
                if not dfs:
                    continue

                df = pd.concat(dfs, ignore_index=True)
                g = group_df(df)

                if g.empty:
                    continue

                g = g.rename(columns={
                    "count": f"{c.lower()} count",
                    "carat": f"{c.lower()} carat",
                    "amount": f"{c.lower()} amount"
                })

                if merged is None:
                    merged = g
                else:
                    merged = pd.merge(merged, g, how="outer")

            if merged is None:
                final_output[k][l] = []
                continue

            merged.fillna(0, inplace=True)

            # -------- TOTAL ROW --------
            total = {}
            for col in merged.columns:
                if merged[col].dtype != "object":
                    total[col] = merged[col].sum()
                else:
                    total[col] = "TOTAL"

            merged = pd.concat([pd.DataFrame([total]), merged], ignore_index=True)

            # -------- SAVE FILE --------
            file_path = f"{OUTPUT_FOLDER}/{k}_{l}.xlsx"
            merged.to_excel(file_path, index=False)

            final_output[k][l] = {
                "file": file_path,
                "rows": len(merged),
                "preview": merged.head(10).to_dict(orient="records")
            }

    return jsonify({
        "status": "DONE",
        "data": final_output
    })


# ---------------- UPLOAD ----------------
@app.route("/upload", methods=["POST"])
def upload():

    uploaded_files = request.files.getlist("files")
    file_type = request.form.get("type")

    for file in uploaded_files:

        filename = file.filename
        path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(path)

        kind = "DZ" if "dz" in filename.lower() else "FANCY"

        files_data.append({
            "name": filename,
            "type": file_type,
            "kind": kind,
            "path": path
        })

    return "OK"


# ---------------- FILE LIST ----------------
@app.route("/files")
def get_files():
    return jsonify(files_data)


# ---------------- MOVE ----------------
@app.route("/move", methods=["POST"])
def move():
    data = request.json

    for f in files_data:
        if f["name"] == data["name"]:
            f["type"] = data["type"]
            f["kind"] = data["kind"]

    return "OK"


# ---------------- DELETE ----------------
@app.route("/delete", methods=["POST"])
def delete():
    data = request.json

    global files_data
    files_data = [f for f in files_data if f["name"] != data["name"]]

    return "OK"


# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(debug=True)