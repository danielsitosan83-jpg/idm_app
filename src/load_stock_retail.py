import pandas as pd


def load_stock_retail(path):
    """
    Carga y arma la estructura base del stock Retail.
    NO aplica lógica de negocio.
    NO hace joins.
    Devuelve un DataFrame con el contrato de columnas del IDM.
    """
    print(f"Cargando STOCK_RETAIL desde: {path}")

    df = pd.read_excel(path, dtype=str)

    # --------------------------------------------------------------
    # Renombrar columnas base para alinearlas al IDM
    # --------------------------------------------------------------
    rename_map = {
        "ARTICLE": "Material",
        "ART DESCRIPTION": "Description",
        "STORE STOCK ON HAND": "Quantity",
        "PRODUCT DIVISION": "Division",
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # --------------------------------------------------------------
    # Normalización básica
    # --------------------------------------------------------------
    for col in ["Material", "Description", "Division"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    if "Quantity" in df.columns:
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")

    # --------------------------------------------------------------
    # Crear columnas del IDM con valores FIJOS
    # --------------------------------------------------------------
    df["Brand"] = "adidas"
    df["Pais"] = "AR"
    df["Canal"] = "OUTLET"
    df["Req.Category"] = "MBFO"
    df["StockCategory"] = "MBFO"
    df["MRP Desc"] = "MBFO"

    # --------------------------------------------------------------
    # Crear columnas del IDM VACÍAS (se completan en R2 / R3)
    # --------------------------------------------------------------
    empty_cols = [
        "ORIGEN",
        "SportsCode",
        "Product group",
        "RID",
        "Bloqueado",
        "SO Item",
        "Schedule Line",
        "Size",
        "Business Seg. Text",
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
        "SAP season",
        "Carry over pasado",
        "Carry over futuro",
        "Categoría",
        "CLE",
        "% CLE",
        "Precio",
        "Global Season",
        "Age Group",
        "Gender",
        "Block MOPS",
        "Simbolo",
        "Fecha",
    ]

    for col in empty_cols:
        if col not in df.columns:
            df[col] = ""

    print(f"STOCK_RETAIL base armado ({len(df)} filas)")
    return df
