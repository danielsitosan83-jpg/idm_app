import pandas as pd


def apply_mrp_desc(df, mrp_status_map_df):
    """
    Aplica la descripción del MRP Status.
    Crea la columna 'MRP Desc' a partir de 'MRP Stat'.
    """
    # Normalizar clave en el map
    mrp_status_map_df = mrp_status_map_df.copy()
    mrp_status_map_df["MRP Stat"] = (
        mrp_status_map_df["mrp_stat"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    mrp_status_map_df["MRP Desc"] = (
        mrp_status_map_df["mrp_desc"]
        .astype(str)
        .str.strip()
    )

    # Join para traer la descripción
    df = df.merge(
        mrp_status_map_df[["MRP Stat", "MRP Desc"]],
        on="MRP Stat",
        how="left"
    )

    return df