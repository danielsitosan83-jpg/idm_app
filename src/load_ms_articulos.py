import pandas as pd


def load_ms_articulos(path):
    """
    Carga y normaliza el archivo MS_ARTICULOS (maestro de artículos).
    Deja el DataFrame listo para join por Material.
    """
    print(f"Cargando MS_ARTICULOS desde: {path}")

    df = pd.read_excel(path, dtype=str)

    # ------------------------------------------------------------------
    # Rename map (alineación con el MAPA)
    # ------------------------------------------------------------------
    rename_map = {
        "ARTICLE": "Material",
        "SPORT DESC": "SportsCode",
        "PRODUCT GROUP": "Product group",
        "BUSINESS SEGMENT": "Business Seg. Text",
        "CARRY OVER: PASADO": "Carry over pasado",
        "CARRY OVER: FUTURO": "Carry over futuro",
        "CATEGORY": "Categoría",
        "WHS (CLE P)": "CLE",
        "WHSP VIGENTE (CON CLE)": "WHSP VIGENTE (CON CLE)",
        "WHS CLE DISCOUNT": "% CLE",
        "LOCAL SEASON": "Global Season",
        "SAP Season": "SAP season",
        "AGE GROUP": "Age Group",
        "GENDER": "Gender",
        "DIVISION": "Division",
        "RID": "RID",
        "ORIGEN": "ORIGEN",
        "SIMBOLOS": "Simbolo",
        "BLOQUEOS: W=whs/fr | P=promo | R=retail | E=ecom": "Block MOPS",
    }

    df = df.rename(
        columns={k: v for k, v in rename_map.items() if k in df.columns}
    )

    # ------------------------------------------------------------------
    # Normalización de Material (clave de join)
    # ------------------------------------------------------------------
    if "Material" in df.columns:
        df["Material"] = (
            df["Material"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"": pd.NA})
        )

    # ------------------------------------------------------------------
    # Trim general de strings
    # ------------------------------------------------------------------
    for col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA})
        )

    print(f"MS_ARTICULOS cargado correctamente ({len(df)} filas)")
    return df