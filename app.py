# app.py
import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime

from idm_core import generar_idm

st.set_page_config(
    page_title="Inventory Digital Map",
    layout="centered"
)

st.title("📦 Inventory Digital Map (IDM)")
st.caption("Generador del reporte IDM")

st.divider()

# Uploaders
mrp_file = st.file_uploader("📄 Subir archivo MRP", type=["xlsx"])
retail_file = st.file_uploader("🏬 Subir archivo STOCK_RETAIL", type=["xlsx"])
ms_file = st.file_uploader("🧾 Subir archivo MS_ARTICULOS", type=["xlsx"])

st.divider()

if mrp_file and retail_file and ms_file:
    if st.button("🚀 Generar IDM"):
        with st.spinner("Procesando archivos, por favor esperá..."):
            df_final = generar_idm(
                mrp_file=mrp_file,
                retail_file=retail_file,
                ms_file=ms_file
            )

            buffer = BytesIO()
            file_name = f"IDM_Base_{datetime.now():%Y%m%d}.xlsx"

            df_final.to_excel(
                buffer,
                index=False,
                engine="xlsxwriter"
            )
            buffer.seek(0)

        st.success("✅ IDM generado correctamente")

        st.download_button(
            label="⬇️ Descargar IDM",
            data=buffer,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("⬆️ Subí los 3 archivos para habilitar la generación")
