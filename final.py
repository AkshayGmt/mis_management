from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import zipfile
import os
import re
import uuid
from io import BytesIO
from threading import Lock

app = Flask(__name__)

# =========================================================
# GLOBAL STORAGE
# =========================================================

files_data = []
lock = Lock()

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# =========================================================
# HELPERS
# =========================================================

def clean_columns(df):

    df.columns = [
        re.sub(r"\s+", " ", str(c).strip().lower())
        for c in df.columns
    ]

    # remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    return df


# =========================================================
# TEXT NORMALIZATION
# =========================================================

def normalize_text(value):

    if pd.isna(value):
        return None

    v = str(value).strip()

    # remove extra spaces
    v = re.sub(r"\s+", " ", v)

    # uppercase
    return v.upper()


# =========================================================
# SIZE NORMALIZATION
# =========================================================

def normalize_size(value):

    if pd.isna(value):
        return None

    v = str(value).strip().lower()

    # remove spaces
    v = re.sub(r"\s+", "", v)

    # normalize separators
    v = (
        v.replace("–", "-")
         .replace("—", "-")
         .replace("_", "-")
         .replace("to", "-")
    )

    # keep valid chars only
    v = re.sub(r"[^0-9.\-+]", "", v)

    if not v:
        return None

    # range
    match = re.fullmatch(
        r"(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)",
        v
    )

    if match:

        a, b = match.groups()

        # sort numerically
        if float(a) > float(b):
            a, b = b, a

        return f"{a}-{b}"

    # plus
    match = re.fullmatch(
        r"(\d+(?:\.\d+)?)\+?",
        v
    )

    if match:
        return f"{match.group(1)}+"

    return v


# =========================================================
# HEADER DETECTION
# =========================================================

def detect_header(df):

    keywords = [
        "shape",
        "color",
        "clarity",
        "size",
        "country",
        "origin",
        "location"
    ]

    for i, row in df.iterrows():

        vals = [
            str(x).strip().lower()
            for x in row.values
        ]

        matches = sum(
            any(k == v for k in keywords)
            for v in vals
        )

        if matches >= 2:
            return i

    return None


# =========================================================
# COLUMN FINDER
# =========================================================

def find(df, names):

    for n in names:
        for c in df.columns:
            if n in c.lower():
                return c

    return None


# =========================================================
# FILE PROCESSING
# =========================================================

def process_file(path):

    try:

        raw = pd.read_excel(
            path,
            header=None,
            engine="openpyxl"
        )

        header_row = detect_header(raw)

        if header_row is None:
            print("HEADER NOT FOUND:", path)
            return None

        df = pd.read_excel(
            path,
            header=header_row,
            engine="openpyxl"
        )

        df = clean_columns(df)

        # normalize columns
        for c in df.columns:

            cl = c.lower()

            # size normalization
            if "size" in cl:
                df[c] = df[c].apply(normalize_size)

            # text normalization
            elif any(x in cl for x in [
                "color",
                "clarity",
                "shape",
                "lab",
                "type"
            ]):
                df[c] = df[c].apply(normalize_text)

        print("PROCESSED:", path)
        print("COLUMNS:", df.columns.tolist())

        return df

    except Exception as e:

        print("PROCESS FILE ERROR:", path)
        print(e)

        return None


# =========================================================
# BUILD DATA
# =========================================================

def build_data():

    categories = list(
        set(f["type"] for f in files_data)
    )

    kinds = ["DZ", "FANCY"]

    result = {
        c: {
            k: {
                "USA": [],
                "INDIA": []
            }
            for k in kinds
        }
        for c in categories
    }

    for f in files_data:

        df = process_file(f["path"])

        if df is None or df.empty:
            continue

        category = f["type"]
        kind = f["kind"]

        loc_col = find(df, [
            "location",
            "country",
            "origin"
        ])

        # no location column
        if loc_col is None:
            result[category][kind]["INDIA"].append(df)
            continue

        df[loc_col] = (
            df[loc_col]
            .astype(str)
            .str.lower()
        )

        usa = df[
            df[loc_col]
            .str.contains(
                "usa|us|america",
                na=False
            )
        ]

        india = df[
            ~df[loc_col]
            .str.contains(
                "usa|us|america",
                na=False
            )
        ]

        if not usa.empty:
            result[category][kind]["USA"].append(usa)

        if not india.empty:
            result[category][kind]["INDIA"].append(india)

    return result, categories


# =========================================================
# GROUPING
# =========================================================

def group_df(df):

    try:

        if df.empty:
            return pd.DataFrame()

        df = clean_columns(df)

        print("GROUPING COLUMNS:")
        print(df.columns.tolist())

        group_cols = []

        keys = [
            "shape",
            "size",
            "color",
            "clarity",
            "lab",
            "type"
        ]

        for key in keys:
            for c in df.columns:
                if key in c:
                    group_cols.append(c)
                    break

        count_col = find(df, [
            "pcs",
            "qty",
            "count"
        ])

        carat_col = find(df, [
            "carat",
            "cts",
            "weight"
        ])

        amount_col = find(df, [
            "amount",
            "value",
            "price"
        ])

        agg = {}

        if count_col:
            agg[count_col] = "sum"

        if carat_col:
            agg[carat_col] = "sum"

        if amount_col:
            agg[amount_col] = "sum"

        if not group_cols:
            print("NO GROUP COLUMNS")
            return pd.DataFrame()

        if not agg:
            print("NO AGGREGATION COLUMNS")
            return pd.DataFrame()

        grouped = (
            df.groupby(group_cols, dropna=False)
            .agg(agg)
            .reset_index()
        )

        rename = {}

        if count_col:
            rename[count_col] = "count"

        if carat_col:
            rename[carat_col] = "carat"

        if amount_col:
            rename[amount_col] = "amount"

        grouped.rename(
            columns=rename,
            inplace=True
        )

        # average
        if (
            "amount" in grouped.columns
            and "carat" in grouped.columns
        ):

            grouped["avg"] = (
                grouped["amount"] /
                grouped["carat"].replace(
                    0,
                    pd.NA
                )
            )

        # round financial columns
        if "amount" in grouped.columns:
            grouped["amount"] = (
                grouped["amount"].round(2)
            )

        if "avg" in grouped.columns:
            grouped["avg"] = (
                grouped["avg"].round(2)
            )

        return grouped

    except Exception as e:

        print("GROUP ERROR:")
        print(e)

        return pd.DataFrame()


# =========================================================
# COMBINE ENGINE
# =========================================================

def run_combine():

    kinds = ["DZ", "FANCY"]
    locations = ["USA", "INDIA"]

    data, categories = build_data()

    order = [
        "TOTAL",
        "SOLD",
        "CURRENT"
    ]

    categories = sorted(
        set(categories),
        key=lambda x:
        order.index(x)
        if x in order else 999
    )

    memory_zip = BytesIO()

    with zipfile.ZipFile(memory_zip, "w") as z:

        for kind in kinds:

            for location in locations:

                merged = None

                for category in categories:

                    dfs = data[category][kind][location]

                    if not dfs:
                        continue

                    combined = pd.concat(
                        dfs,
                        ignore_index=True
                    )

                    grouped = group_df(combined)

                    if grouped.empty:
                        continue

                    grouped = grouped.rename(columns={
                        "count":
                        f"{category.lower()} count",

                        "carat":
                        f"{category.lower()} carat",

                        "amount":
                        f"{category.lower()} amount",

                        "avg":
                        f"{category.lower()} avg"
                    })

                    if merged is None:

                        merged = grouped

                    else:

                        merge_keys = [
                            c for c in merged.columns
                            if c in grouped.columns
                            and not any(
                                x in c
                                for x in [
                                    "count",
                                    "carat",
                                    "amount",
                                    "avg"
                                ]
                            )
                        ]

                        merged = pd.merge(
                            merged,
                            grouped,
                            how="outer",
                            on=merge_keys
                        )

                if merged is None or merged.empty:
                    continue

                merged.fillna(0, inplace=True)

                # TOTAL ROW
                total = {}

                for c in merged.columns:

                    if pd.api.types.is_numeric_dtype(
                        merged[c]
                    ):
                        total[c] = merged[c].sum()
                    else:
                        total[c] = "TOTAL"

                merged = pd.concat([
                    pd.DataFrame([total]),
                    merged
                ], ignore_index=True)

                # SORT SIZE
                for c in merged.columns:

                    if "size" in c:

                        try:
                            merged = merged.sort_values(
                                by=c
                            )

                        except Exception as e:
                            print("SORT ERROR:", e)

                # EXPORT
                excel_buffer = BytesIO()

                merged.to_excel(
                    excel_buffer,
                    index=False,
                    engine="openpyxl"
                )

                z.writestr(
                    f"{kind}_{location}.xlsx",
                    excel_buffer.getvalue()
                )

    memory_zip.seek(0)

    return memory_zip


# =========================================================
# ROUTES
# =========================================================

@app.route("/")
def home():
    return render_template("in.html")


@app.route("/upload", methods=["POST"])
def upload():

    file_type = request.form.get("type")

    uploaded = []

    for f in request.files.getlist("files"):

        unique_name = (
            f"{uuid.uuid4()}_{f.filename}"
        )

        path = os.path.join(
            UPLOAD_FOLDER,
            unique_name
        )

        f.save(path)

        data = {
            "name": f.filename,
            "saved_name": unique_name,
            "type": file_type,
            "kind": (
                "DZ"
                if "dz" in f.filename.lower()
                else "FANCY"
            ),
            "path": path
        }

        with lock:
            files_data.append(data)

        uploaded.append(data)

    return jsonify({
        "status": "success",
        "uploaded": uploaded
    })


@app.route("/files")
def get_files():
    return jsonify(files_data)


@app.route("/delete", methods=["POST"])
def delete():

    global files_data

    name = request.json["name"]

    with lock:

        new_files = []

        for f in files_data:

            if f["name"] == name:

                try:

                    if os.path.exists(f["path"]):
                        os.remove(f["path"])

                except Exception as e:
                    print("DELETE ERROR:", e)

            else:
                new_files.append(f)

        files_data = new_files

    return jsonify({
        "status": "deleted"
    })


@app.route("/move", methods=["POST"])
def move():

    data = request.json

    with lock:

        for f in files_data:

            if f["name"] == data["name"]:

                f["type"] = data["type"]
                f["kind"] = data["kind"]

    return jsonify({
        "status": "updated"
    })


@app.route("/process-preview")
def preview():

    data, categories = build_data()

    preview_data = {
        k: {
            "USA": [],
            "INDIA": []
        }
        for k in ["DZ", "FANCY"]
    }

    for kind in preview_data:

        for location in preview_data[kind]:

            for category in categories:

                dfs = data[category][kind][location]

                for df in dfs:

                    preview_data[kind][location].append(
                        df.head(5).to_dict(
                            orient="records"
                        )
                    )

    return jsonify({
        "data": preview_data
    })


@app.route("/download")
def download():

    zip_file = run_combine()

    return send_file(
        zip_file,
        download_name="MIS_Output.zip",
        as_attachment=True
    )


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    app.run(
        debug=True,
        host="0.0.0.0",
        port=5000
    )
