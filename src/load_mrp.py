import pandas as pd

def load_mrp(path):
    """
    Carga y normaliza el archivo MRP (stock).
    Devuelve un DataFrame limpio y consistente.
    """
    print(f"Cargando MRP desde: {path}")

    df = pd.read_excel(path, dtype=str)

    rename_map = {
        "Confirmed Deliv Dt": "Confirmed Deliv. Dt.",
        "Req Delivery Dt": "Req. Delivery Dt.",
        "Cust Name": "Cust. Name",
        "ReqCategory": "Req.Category",
        "Sales Org": "Sales Org.",
        "SO Item": "SO Item",
        "SLNo": "Schedule Line"
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    upper_cols = [
        "Material", "StockCategory", "MRP Stat", "Stock Type",
        "Customer No", "Order Type", "Order No", "Size"
    ]
    for col in upper_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.upper()
                .replace({"": pd.NA})
            )

    text_cols = ["Description", "Cust. Name"]
    for col in text_cols:
        if col in text_cols and col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"": pd.NA})

    for col in ["Quantity", "SO Item", "Schedule Line"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Storage Location" in df.columns:
        df["Storage Location"] = (
            df["Storage Location"]
            .astype(str)
            .str.replace(".0", "", regex=False)
            .str.zfill(4)
            .replace({"0000": pd.NA})
        )

    date_cols = [
        "Confirmed Deliv. Dt.",
        "Req. Delivery Dt.",
        "PO Delivery Date"
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col].astype(str).str.replace("/", ".", regex=False),
                format="%d.%m.%Y",
                errors="coerce"
            )

    required_cols = ["Material", "Quantity", "StockCategory", "MRP Stat"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas obligatorias en MRP: {missing}")

    print(f"MRP cargado correctamente ({len(df)} filas)")
    return df

