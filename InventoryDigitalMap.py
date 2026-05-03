import sys
from pathlib import Path

# Ruta del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent

# Agregar explícitamente la carpeta src al PYTHONPATH
SRC_PATH = PROJECT_ROOT / "src"

import pandas as pd

from load_mrp import load_mrp
from load_stock_retail import load_stock_retail
from load_ms_articulos import load_ms_articulos
from apply_mrp_desc import apply_mrp_desc
from apply_canal import apply_canal
from rules.bloqueo_engine import apply_bloqueo_engine


def main():
    print("Inicio del generador de reporte Inventory Digital Map")

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------
    base_path = Path("data")
    mrp_path = base_path / "MRP.xlsx"
    retail_path = base_path / "STOCK_RETAIL.xlsx"
    ms_articulos_path = base_path / "MS_ARTICULOS.xlsx"
    auxiliar_path = base_path / "AUXILIAR_RULES.xlsx"

    # ------------------------------------------------------------------
    # Cargar maestro
    # ------------------------------------------------------------------
    df_ms = load_ms_articulos(ms_articulos_path)

    # ==============================================================
    # ============================ MRP ==============================
    # ==============================================================
    print("Cargando MRP...")
    df_mrp = load_mrp(mrp_path)

    print("Join MRP + MS_ARTICULOS...")
    df_mrp = df_mrp.merge(df_ms, on="Material", how="left")

    # --------------------------------------------------
    # SAP season: respetar SIEMPRE el valor del MRP
    # --------------------------------------------------
    if "SAP season_x" in df_mrp.columns:
        df_mrp["SAP season"] = df_mrp["SAP season_x"]

    # --------------------------------------------------
    # Traducción DEFINITIVA de Division (MRP)
    # --------------------------------------------------
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

    # ---- Completar TODAS las columnas de producto desde MS_ARTICULOS
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

    # ---- MRP Desc
    df_mrp_status_map = pd.read_excel(
        auxiliar_path, sheet_name="mrp_status_map", dtype=str
    )
    df_mrp = apply_mrp_desc(df_mrp, df_mrp_status_map)

    # ---- Canal
    df_canal_rules = pd.read_excel(
        auxiliar_path, sheet_name="canal_rules", dtype=str
    )

    # Normalización "(blank)"
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

    # --------------------------------------------------
    # SC10 + Cust. Name vacío → EXCESS (solo MRP)
    # --------------------------------------------------
    mask_sc10_empty_name = (
        (df_mrp["StockCategory"] == "A73000010") &
        (
            df_mrp["Cust. Name"].isna() |
            (df_mrp["Cust. Name"].astype(str).str.strip() == "")
        )
    )

    df_mrp.loc[mask_sc10_empty_name, "Canal"] = "EXCESS"

    # ==============================================================
    # =========================== RETAIL ============================
    # ==============================================================
    print("Cargando Retail...")
    df_retail = load_stock_retail(retail_path)

    print("Join Retail + MS_ARTICULOS...")
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
                "Simbolo"
            ]
        ],
        on="Material",
        how="left",
    )

    # --------------------------------------------------
    # SAP season para Retail desde MS_ARTICULOS
    # --------------------------------------------------
    if "SAP season_y" in df_retail.columns:
        df_retail["SAP season"] = df_retail["SAP season_y"]

    # ---- Completar columnas de producto exactamente igual que MRP
    for col in PRODUCT_COLS:
        col_y = f"{col}_y"
        if col_y in df_retail.columns:
            if col not in df_retail.columns:
                df_retail[col] = df_retail[col_y]
            else:
                df_retail[col] = df_retail[col].fillna(df_retail[col_y])

    # ---- Global Season fallback
    if "Global Season" in df_retail.columns:
        df_retail["Global Season"] = (
            df_retail["Global Season"]
            .replace("", pd.NA)
            .fillna("BULK!!!")
        )

    # ==============================================================
    # ===================== BLOQUEADO (COMÚN) ======================
    # ==============================================================
    df_bloqueos_rules = pd.read_excel(
        auxiliar_path, sheet_name="bloqueos_por_canal", dtype=str
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

    # -------------------------------------------------
    # SKU = Material + Size
    # --------------------------------------------------
    df_final["SKU"] = (
        df_final["Material"].astype(str).str.strip()
        + df_final["Size"].astype(str).str.strip()
    )
    
    # --------------------------------------------------
    # CLAVE = Order No. + Material + SO Item (sin separador)
    # --------------------------------------------------
    df_final["Clave"] = (
        df_final["Order No."].astype(str).str.strip()
        + df_final["Material"].astype(str).str.strip()
        + df_final["SO Item"].astype(str).str.strip()
    )


    # --------------------------------------------------
    # Eliminar StockCategory no deseados antes del export
    # --------------------------------------------------
    df_final = df_final[
        ~df_final["StockCategory"].isin(["A73000015", "B73000001"])
    ]

    
    # ---- Formato fechas
    for col in [
        "Confirmed Deliv. Dt.",
        "Req. Delivery Dt.",
        "Planned Receipt Dt.",
        "RID",
        "RED",
    ]:
        if col in df_final.columns:
            df_final[col] = (
                pd.to_datetime(df_final[col], errors="coerce")
                .dt.strftime("%d/%m/%Y")
            )

    # --------------------------------------------------
    # Regla final para completar Simbolo = OTHER
    # --------------------------------------------------
    # Asegurar mayúsculas para evaluar el texto
    if "Business Seg. Text" in df_final.columns:
        mask_licenced = (
            df_final["Business Seg. Text"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.contains("LICENSED")
        )
        
        # Cuando Simbolo está vacío y es LICENCED → OTHER
        df_final.loc[
            df_final["Simbolo"].fillna("").eq("") & mask_licenced,
            "Simbolo"
        ] = "OTHER"
      
    # --------------------------------------------------
    # Reglas finales de columnas fijas (Brand, Pais, Division)
    # --------------------------------------------------

    # BRAND: siempre adidas
    df_final["Brand"] = "adidas"

    # PAIS: siempre AR
    df_final["Pais"] = "AR"

    # --------------------------------------------------
    # Fallback GLOBAL para Global Season
    # --------------------------------------------------
    if "Global Season" in df_final.columns:
        df_final["Global Season"] = (
            df_final["Global Season"]
            .replace("", pd.NA)
            .fillna("BULK!!!")
        )
    
    # --------------------------------------------------
    # Fallback GLOBAL para SAP season
    # --------------------------------------------------
    if "SAP season" in df_final.columns:
        df_final["SAP season"] = (
            df_final["SAP season"]
            .replace("", pd.NA)
            .fillna("BULK!!!")
        )
        

    # --------------------------------------------------
    # PRECIO: completar desde MS_ARTICULOS
    # --------------------------------------------------
    if "WHSP VIGENTE (CON CLE)" in df_final.columns:
        precio_num = pd.to_numeric(
            df_final["WHSP VIGENTE (CON CLE)"],
            errors="coerce"
        )

        df_final["WHSP PRICE VIGENTE (CON CLE)"] = (
            precio_num
            .map(lambda x: f"${x:,.2f}" if pd.notna(x) else None)
        )

        # Reemplazar separadores a formato argentino
        df_final["WHSP PRICE VIGENTE (CON CLE)"] = (
            df_final["WHSP PRICE VIGENTE (CON CLE)"]
            .str.replace(",", "X", regex=False)
            .str.replace(".", ",", regex=False)
            .str.replace("X", ".", regex=False)
        )

        # Para los casos no numéricos (ej: TBD), conservar el valor original
        mask_tbd = df_final["WHSP PRICE VIGENTE (CON CLE)"].isna()
        df_final.loc[mask_tbd, "WHSP PRICE VIGENTE (CON CLE)"] = (
            df_final.loc[mask_tbd, "WHSP VIGENTE (CON CLE)"]
        )
      
    
    # --------------------------------------------------
    # FECHA: fecha de exportación (DD/MM/YYYY)
    # --------------------------------------------------
    from datetime import datetime

    df_final["Fecha"] = datetime.now().strftime("%d/%m/%Y")
    
    # ---- Limpieza técnica
    df_final.drop(
        columns=[c for c in df_final.columns if c.endswith("_x") or c.endswith("_y")],
        inplace=True,
        errors="ignore",
    )

    # ---- Columnas que NO van en output
    df_final.drop(
        columns=[
            "Plant",
            "Brand Code",
            "Req. Type",
            "Ex. Factory Date",
            "MODEL NAME",
            "IMPORTANTE",
            "Prioridad",
            "Campaña",
            "Fecha Campaña",
            "WHSP FULL",
            "RRP SUG FULL",
            "WHSP VIGENTE (CON CLE)",
            "RRP SUG VIGENTE (CON CLE)",
            "CONCEPT",
            "COMENTARIOS",
            "C03   BDSS",
            "C04   BDSG",
            "C05   CDSG",
            "C06   BDSD",
            "C09   BDFS",
            "C10      BCS",
            "C11     OCS",
            "C12 Internal",
            "C13",
            "FOUNDATION RANGE",
            "Last Season",
            "WHSP VIGENTE (CON CLE)",
            "bloqueos_raw",
            "Precio",
            "Carry over futuro",
        ],
        inplace=True,
        errors="ignore",
    )

    if "BRAND" in df_final.columns:
        df_final.drop(columns=["BRAND"], inplace=True)
    

    # --------------------------------------------------
    # Reordenar columnas según layout final
    # --------------------------------------------------
    final_columns = [
        "Brand",
        "Pais",
        "Division",
        "ORIGEN",
        "SportsCode",
        "Product group",
        "RID",
        "Bloqueado",
        "Material",
        "Description",
        "SO Item",
        "Schedule Line",
        "Size",
        "Quantity",
        "Business Seg. Text",
        "Req.Category",
        "StockCategory",
        "MRP Stat",
        "Stock Type",
        "Storage Location",
        "Sales Org.",
        "Confirmed Deliv. Dt.",
        "Customer No",
        "Cust. Name",
        "Order Type",
        "Order No.",
        "Req. Delivery Dt.",
        "Stock/SupNo",
        "PO Delivery Date",
        "Canal",
        "MRP Desc",
        "SAP season",
        "Carry over pasado",
        "Categoría",
        "CLE",
        "% CLE",
        "WHSP PRICE VIGENTE (CON CLE)",
        "Global Season",
        "Age Group",
        "Gender",
        "Block MOPS",
        "Simbolo",
        "SKU",
        "Clave",
        "Fecha",
    ]

    df_final = df_final[[c for c in final_columns if c in df_final.columns]]
    
    # ==============================================================
    # ======================== EXPORT ===============================
    # ==============================================================
    from datetime import datetime
    import winsound

    output_path = Path("output")
    output_path.mkdir(exist_ok=True)

    output_file = output_path / f"IDM_Base_{datetime.now():%Y%m%d}.xlsx"
    
    df_final.to_excel(output_file, index=False)

    print(f"✅ Export generado: {output_file}")

    # 🔔 Alerta sonora de finalizació
    winsound.Beep(1000, 800)

if __name__ == "__main__":
    main()