from fastapi import FastAPI, Query
from fastapi.responses import Response
import pandas as pd
import psycopg2
import requests
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from io import BytesIO
from datetime import datetime
from PIL import Image as PILImage  # <-- tambahan penting

app = FastAPI()

# =========================
# KONFIG REDSHIFT
# =========================
DB_HOST = "pos-redshift.cwig526q7i0q.ap-southeast-3.redshift.amazonaws.com"
DB_PORT = "5439"
DB_NAME = "posind_kurlog"
DB_USER = "rda_analis"
DB_PASSWORD = "GcTz69eZ6UwNnRhypjx9Ysk8"

CHUNK_SIZE = 100  # maksimal 100 row per sheet


@app.get("/download")
def download_excel(
    customer_code: str = Query(..., description="Customer Code"),
    start_date: str = Query(..., description="Format: YYYYMMDD")
):

    # =========================
    # VALIDASI TANGGAL
    # =========================
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        start_date_sql = start_dt.strftime("%Y-%m-%d")
    except ValueError:
        return {"error": "Format tanggal harus YYYYMMDD"}

    # =========================
    # CONNECT REDSHIFT
    # =========================
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    query = """
    SELECT 
        t1.connote__connote_code,
        t1.customer_code,
        t1.connote__connote_receiver_name,
        t1.connote__connote_receiver_address_detail,
        t1.connote__connote_state,
        t1.pod__timereceive,
        t2.pod__photo,
        t2.pod__signature
    FROM nipos.nipos t1
    JOIN nipos.nipos_pod_url t2
        ON t1.connote__connote_code = t2.connote__connote_code
    WHERE t1.customer_code = %s
        AND date(t1.connote__created_at) = %s
    """

    df = pd.read_sql(query, conn, params=(customer_code, start_date_sql))
    conn.close()

    if df.empty:
        return {"message": "Data tidak ditemukan"}

    # =========================
    # BUAT WORKBOOK
    # =========================
    wb = Workbook()
    wb.remove(wb.active)

    chunks = [df[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]
    session = requests.Session()

    for sheet_index, chunk in enumerate(chunks, start=1):

        ws = wb.create_sheet(title=f"Data_{sheet_index}")
        ws.append(chunk.columns.tolist())

        # Tulis data tanpa gambar dulu
        for _, row in chunk.iterrows():
            row_data = []
            for col in chunk.columns:
                if col in ["pod__photo", "pod__signature"]:
                    row_data.append("")
                else:
                    row_data.append(row[col])
            ws.append(row_data)

        # =========================
        # INSERT GAMBAR (VERSI COMPRESS)
        # =========================
        def insert_image_from_url(url, cell):
            if pd.isna(url):
                return
            try:
                response = session.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=5
                )

                if response.status_code == 200:

                    # Buka pakai Pillow
                    img = PILImage.open(BytesIO(response.content))

                    # Convert agar aman untuk JPEG
                    if img.mode in ("RGBA", "P"):
                        img = img.convert("RGB")

                    # Resize fisik (bukan cuma tampilan)
                    img.thumbnail((700, 700))  # max pixel

                    # Compress
                    compressed = BytesIO()
                    img.save(
                        compressed,
                        format="JPEG",
                        quality=60,       # turunkan kalau mau lebih kecil
                        optimize=True
                    )
                    compressed.seek(0)

                    excel_img = Image(compressed)

                    # Resize tampilan di Excel
                    max_display = 90
                    ratio = min(
                        max_display / excel_img.width,
                        max_display / excel_img.height
                    )
                    excel_img.width = int(excel_img.width * ratio)
                    excel_img.height = int(excel_img.height * ratio)

                    ws.add_image(excel_img, cell)

            except Exception:
                pass

        photo_col = chunk.columns.get_loc("pod__photo") + 1
        sign_col = chunk.columns.get_loc("pod__signature") + 1

        for i in range(len(chunk)):
            excel_row = i + 2
            ws.row_dimensions[excel_row].height = 85

            photo_url = chunk.iloc[i]["pod__photo"]
            sign_url = chunk.iloc[i]["pod__signature"]

            photo_cell = ws.cell(row=excel_row, column=photo_col).coordinate
            sign_cell = ws.cell(row=excel_row, column=sign_col).coordinate

            insert_image_from_url(photo_url, photo_cell)
            insert_image_from_url(sign_url, sign_cell)

        ws.column_dimensions[
            ws.cell(row=1, column=photo_col).column_letter
        ].width = 20

        ws.column_dimensions[
            ws.cell(row=1, column=sign_col).column_letter
        ].width = 20

    # =========================
    # RETURN FILE
    # =========================
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"data_{customer_code}_{start_date}.xlsx"

    return Response(
        content=output.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )
