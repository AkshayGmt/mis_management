from flask import Flask, render_template, request, send_file, jsonify
import os

# 🔥 IMPORT YOUR ENGINES
from engines.stock_engine import run_stock
from engines.sold_engine import run_sold
from engines.combine_engine import run_combine
from engines.graph_engine import run_graph

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# =========================================================
# 🔷 DASHBOARD
# =========================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/stock")
def stock_page():
    return render_template("stock.html")


@app.route("/sold")
def sold_page():
    return render_template("sold.html")


@app.route("/combine")
def combine_page():
    return render_template("combine.html")


@app.route("/graph")
def graph_page():
    return render_template("graph.html")


@app.route("/location")
def location_page():
    return "<h2>Location Wise Page (Coming Soon)</h2>"


# =========================================================
# 🔥 STOCK API
# =========================================================
@app.route("/stock-upload", methods=["POST"])
def stock_upload():
    files = request.files.getlist("files")
    output = run_stock(files)

    return send_file(
        output,
        download_name="stock_output.zip",
        as_attachment=True
    )


# =========================================================
# 🔥 SOLD API
# =========================================================
@app.route("/sold-upload", methods=["POST"])
def sold_upload():
    files = request.files.getlist("files")
    output = run_sold(files)

    return send_file(
        output,
        download_name="sold_output.zip",
        as_attachment=True
    )


# =========================================================
# 🔥 COMBINE API
# =========================================================
@app.route("/combine-upload", methods=["POST"])
def combine_upload():
    files = request.files.getlist("files")
    output = run_combine(files)

    return send_file(
        output,
        download_name="combine_output.zip",
        as_attachment=True
    )


# =========================================================
# 🔥 GRAPH API
# =========================================================
@app.route("/graph-data", methods=["GET"])
def graph_data():
    data = run_graph()
    return jsonify(data)


# =========================================================
# RUN APP
# =========================================================
if __name__ == "__main__":
    app.run(debug=True)
