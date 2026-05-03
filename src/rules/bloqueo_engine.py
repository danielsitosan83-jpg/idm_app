def apply_bloqueo_engine(df_base, df_bloqueos):
    """
    Aplica el motor de Bloqueado usando el AUXILIAR bloqueos_por_canal.

    Reglas:
    - Matching por Canal literal + bloqueo_code
    - NO BLOQUEADO se filtra antes (no entra aquí)
    - ALL solo aplica si Block MOPS = 'SI - ALL CH'
    """

    df = df_base.copy()

    # --------------------------------------------------------------
    # 0. Guardar índice original (CLAVE)
    # --------------------------------------------------------------
    df["_orig_idx"] = df.index

    # --------------------------------------------------------------
    # 1. Normalizar bloqueos crudos
    # --------------------------------------------------------------
    df["bloqueos_raw"] = (
        df["bloqueos_raw"]
        .fillna("")
        .astype(str)
        .str.upper()
    )

    # --------------------------------------------------------------
    # 2. ALL solo válido para SI - ALL CH
    # --------------------------------------------------------------
    mask_all_ch = (
        df["Block MOPS"]
        .fillna("")
        .astype(str)
        .str.upper()
        .str.contains("ALL", na=False)
    )

    df.loc[~mask_all_ch, "bloqueos_raw"] = (
        df.loc[~mask_all_ch, "bloqueos_raw"]
        .str.replace("ALL", "", regex=False)
        .str.replace(",,", ",", regex=False)
        .str.strip(",")
    )

    # --------------------------------------------------------------
    # 3. Explode de códigos
    # --------------------------------------------------------------
    df_exploded = (
        df
        .assign(bloqueo_code=df["bloqueos_raw"].str.split(","))
        .explode("bloqueo_code")
    )

    df_exploded["bloqueo_code"] = (
        df_exploded["bloqueo_code"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    # --------------------------------------------------------------
    # 4. Merge con AUXILIAR
    # --------------------------------------------------------------
    df_merge = df_exploded.merge(
        df_bloqueos,
        how="left",
        left_on=["Canal", "bloqueo_code"],
        right_on=["canal", "bloqueo_code"]
    )

    # --------------------------------------------------------------
    # 5. Resolver bloqueo por fila original
    # --------------------------------------------------------------
    bloqueado_por_linea = (
        df_merge
        .groupby("_orig_idx")["bloqueado_result"]
        .apply(lambda x: (x == "Bloqueado").any())
    )

    # --------------------------------------------------------------
    # 6. Resultado final
    # --------------------------------------------------------------
    df["Bloqueado"] = df["_orig_idx"].map(
        lambda i: "Bloqueado" if bloqueado_por_linea.get(i, False) else "No Bloqueado"
    )

    # Limpieza técnica
    df.drop(columns="_orig_idx", inplace=True)

    return df
