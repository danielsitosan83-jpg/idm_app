# idm_core.py
from pathlib import Path
import pandas as pd
from datetime import datetime

# imports del proyecto
from src.load_mrp import load_mrp
from src.load_stock_retail import load_stock_retail
from src.load_ms_articulos import load_ms_articulos
from src.apply_mrp_desc import apply_mrp_desc
from src.apply_canal import apply_canal
from src.rules.bloqueo_engine import apply_bloqueo_engine


def generar_idm(mrp_file, retail_file, ms_file):
    """
    Recibe 3 archivos (file-like) y devuelve df_final
    """

    # ==============================================================
    # ============================ CARGA ===========================
    # ==============================================================
    df_ms = load_ms_articulos(ms_file)
    df_mrp = load_mrp(mrp_file)
    df_retail = load_stock_retail(retail_file)

    # ==============================================================
    # ============================ MRP =============================
    # ==============================================================
    df_mrp = df_mrp.merge(df_ms, on="Material", how="left")

    # SAP season (MRP manda)
    if "SAP season_x" in df_mrp.columns:
        df_mrp["SAP season"] = df_mrp["SAP season_x"]

    # Division definitiva desde MRP
    if "Division_x" in df_mrp.columns:
        df_mrp["Division"] = (
            df_mrp["Division_x"]
            .astype(str)
            .str.strip()
            .replace({
                "01": "FOOTWEAR",
                "02": "APPAREL",
                "03": "HARDWARE",
            })
        )

    # Columnas de producto desde MS_ARTICULOS
    PRODUCT_COLS = [
        "Carry over pasado",
        "Block MOPS",
        "CTC",
        "Categoría",
        "CLE",
        "% CLE",
        "Age Group",
        "Gender",
        "Product group",
        "SportsCode",
        "RID",
        "RED",
        "Simbolo",
        "ORIGEN",
        "Global Season",
        "Business Seg. Text",
    ]

    for col in PRODUCT_COLS:
        col_y = f"{col}_y"
        if col_y in df_mrp.columns:
            if col not in df_mrp.columns:
                df_mrp[col] = df_mrp[col_y]
            else:
                df_mrp[col] = df_mrp[col].fillna(df_mrp[col_y])

    # MRP Desc
    df_mrp_status_map = pd.read_excel(
        Path("data/AUXILIAR_RULES.xlsx"), sheet_name="mrp_status_map", dtype=str
    )
    df_mrp = apply_mrp_desc(df_mrp, df_mrp_status_map)

    # Canal (MRP)
    df_canal_rules = pd.read_excel(
        Path("data/AUXILIAR_RULES.xlsx"), sheet_name="canal_rules", dtype=str
    )

    for col in ["Order Type", "Cust. Name"]:
        if col in df_mrp.columns:
            df_mrp[col] = (
                df_mrp[col]
                .replace("(blank)", "")
                .replace("BLANK", "")
                .fillna("")
                .astype(str)
                .str.strip()
            )

    df_mrp = apply_canal(df_mrp, df_canal_rules)

    # SC10 + Cust. Name vacío → EXCESS (post canal)
    mask_sc10_empty = (
        (df_mrp["StockCategory"] == "A73000010") &
        ((df_mrp["Cust. Name"].isna()) | (df_mrp["Cust. Name"].astype(str).str.strip() == ""))
    )
    df_mrp.loc[mask_sc10_empty, "Canal"] = "EXCESS"

    # ==============================================================
    # =========================== RETAIL ===========================
    # ==============================================================
    df_retail = df_retail.merge(
        df_ms[
            [
                "Material",
                "Carry over pasado",
                "Block MOPS",
                "CTC",
                "Categoría",
                "CLE",
                "% CLE",
                "Age Group",
                "Gender",
                "Product group",
                "SportsCode",
                "RID",
                "ORIGEN",
                "Global Season",
                "Business Seg. Text",
                "RED",
                "SAP season",
                "WHSP VIGENTE (CON CLE)",
                "Simbolo",
            ]
        ],
        on="Material",
        how="left",
    )

    if "SAP season_y" in df_retail.columns:
        df_retail["SAP season"] = df_retail["SAP season_y"]

    for col in PRODUCT_COLS:
        col_y = f"{col}_y"
        if col_y in df_retail.columns:
            if col not in df_retail.columns:
                df_retail[col] = df_retail[col_y]
            else:
                df_retail[col] = df_retail[col].fillna(df_retail[col_y])

    if "Global Season" in df_retail.columns:
        df_retail["Global Season"] = (
            df_retail["Global Season"].replace("", pd.NA).fillna("BULK!!!")
        )

    # ==============================================================
    # ========================= BLOQUEADO ==========================
    # ==============================================================
    df_bloqueos_rules = pd.read_excel(
        Path("data/AUXILIAR_RULES.xlsx"), sheet_name="bloqueos_por_canal", dtype=str
    )
    df_bloqueos_rules["canal"] = df_bloqueos_rules["canal"].str.upper().str.strip()
    df_bloqueos_rules["bloqueo_code"] = df_bloqueos_rules["bloqueo_code"].str.upper().str.strip()
    df_bloqueos_rules["bloqueado_result"] = df_bloqueos_rules["bloqueado_result"].str.strip()

    def aplicar_bloqueado(df):
        if "Block MOPS" not in df.columns:
            df["Block MOPS"] = ""

        mask_no_bloq = (
            df["Block MOPS"]
            .fillna("")
            .astype(str)
            .str.upper()
            .isin(["NO BLOQUEADO", ""])
        )

        df_no = df[mask_no_bloq].copy()
        df_ok = df[~mask_no_bloq].copy()

        df_no["Bloqueado"] = "No Bloqueado"
        df_no["bloqueos_raw"] = ""

        df_ok["bloqueos_raw"] = (
            df_ok["Block MOPS"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.replace("SI -", "", regex=False)
            .str.replace("ALL CH", "ALL", regex=False)
            .str.replace(" ", ",", regex=False)
            .str.strip(",")
        )

        df_ok = apply_bloqueo_engine(df_ok, df_bloqueos_rules)
        return pd.concat([df_no, df_ok], ignore_index=True)

    df_mrp = aplicar_bloqueado(df_mrp)
    df_retail = aplicar_bloqueado(df_retail)

    # ==============================================================
    # ======================== CONCAT FINAL =========================
    # ==============================================================
    df_final = pd.concat([df_mrp, df_retail], ignore_index=True)

    # Eliminar StockCategory no deseados
    df_final = df_final[
        ~df_final["StockCategory"].isin(["A73000015", "B73000001"])
    ].copy()

    # Formato fechas
    for col in [
        "Confirmed Deliv. Dt.",
        "Req. Delivery Dt.",
        "Planned Receipt Dt.",
        "RID",
        "RED",
    ]:
        if col in df_final.columns:
            df_final[col] = (
                pd.to_datetime(df_final[col], errors="coerce").dt.strftime("%d/%m/%Y")
            )

    # Simbolo = OTHER por LICENSED
    if "Business Seg. Text" in df_final.columns:
        mask_licenced = (
            df_final["Business Seg. Text"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.contains("LICENSED")
        )
        df_final.loc[
            df_final["Simbolo"].fillna("").eq("") & mask_licenced,
            "Simbolo"
        ] = "OTHER"

    # Brand y País
    df_final["Brand"] = "adidas"
    df_final["Pais"] = "AR"

    # Fallbacks
    if "Global Season" in df_final.columns:
        df_final["Global Season"] = (
            df_final["Global Season"].replace("", pd.NA).fillna("BULK!!!")
        )

    if "SAP season" in df_final.columns:
        df_final["SAP season"] = (
            df_final["SAP season"].replace("", pd.NA).fillna("BULK!!!")
        )

    # Precio WHSP formateado
    if "WHSP VIGENTE (CON CLE)" in df_final.columns:
        precio_num = pd.to_numeric(df_final["WHSP VIGENTE (CON CLE)"], errors="coerce")
        df_final["WHSP PRICE VIGENTE (CON CLE)"] = precio_num.map(
            lambda x: f"${x:,.2f}" if pd.notna(x) else None
        )
        df_final["WHSP PRICE VIGENTE (CON CLE)"] = (
            df_final["WHSP PRICE VIGENTE (CON CLE)"]
            .str.replace(",", "X", regex=False)
            .str.replace(".", ",", regex=False)
            .str.replace("X", ".", regex=False)
        )
        mask_tbd = df_final["WHSP PRICE VIGENTE (CON CLE)"].isna()
        df_final.loc[mask_tbd, "WHSP PRICE VIGENTE (CON CLE)"] = (
            df_final.loc[mask_tbd, "WHSP VIGENTE (CON CLE)"]
        )

    # Fecha de exportación
    df_final["Fecha"] = datetime.now().strftime("%d/%m/%Y")

    # Limpieza técnica
    df_final.drop(
        columns=[c for c in df_final.columns if c.endswith("_x") or c.endswith("_y")],
        inplace=True,
        errors="ignore",
    )

    # Columnas finales
    final_columns = [
        "Brand", "Pais", "Division", "ORIGEN", "SportsCode", "Product group",
        "RID", "Bloqueado", "Material", "Description", "SO Item", "Schedule Line",
        "Size", "Quantity", "Business Seg. Text", "Req.Category", "StockCategory",
        "MRP Stat", "Stock Type", "Storage Location", "Sales Org.",
        "Confirmed Deliv. Dt.", "Customer No", "Cust. Name", "Order Type",
        "Order No.", "Req. Delivery Dt.", "Stock/SupNo", "PO Delivery Date",
        "Canal", "MRP Desc", "SAP season", "Carry over pasado", "Categoría",
        "CLE", "% CLE", "WHSP PRICE VIGENTE (CON CLE)", "Global Season",
        "Age Group", "Gender", "Block MOPS", "Simbolo", "Fecha",
    ]

    df_final = df_final[[c for c in final_columns if c in df_final.columns]]

    return df_final