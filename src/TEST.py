import pandas as pd

from rules.bloqueo_engine import apply_bloqueo_engine
from load_stock_retail import load_stock_retail
from load_ms_articulos import load_ms_articulos

# ==============================================================
# CARGA DE FUENTES
# ==============================================================

# --------------------------------------------------------------
# Cargar MS_ARTICULOS (maestro)
# --------------------------------------------------------------
df_ms = load_ms_articulos("data/MS_ARTICULOS.xlsx")

print("\n--- Columnas en df_ms ---")
print(df_ms.columns.tolist())

# --------------------------------------------------------------
# Cargar STOCK_RETAIL (estructura base)
# --------------------------------------------------------------
df_retail = load_stock_retail("data/STOCK_RETAIL.xlsx")

print("\n--- Columnas en df_retail (base) ---")
print(df_retail.columns.tolist())
print("Filas Retail:", len(df_retail))


# ==============================================================
# PASO R2.1 – ENRIQUECER RETAIL DESDE MS_ARTICULOS
# ==============================================================

ms_cols = [
    "Material",
    "ORIGEN",
    "SportsCode",
    "Product group",
    "RID",
    "Business Seg. Text",
    "Carry over pasado",
    "Carry over futuro",
    "Categoría",
    "CLE",
    "% CLE",
    "Age Group",
    "Gender",
    "Block MOPS",
]

# Eliminar columnas vacías preexistentes para evitar _x / _y
df_retail = df_retail.drop(
    columns=[
        "ORIGEN",
        "SportsCode",
        "Product group",
        "RID",
        "Business Seg. Text",
        "Carry over pasado",
        "Carry over futuro",
        "Categoría",
        "CLE",
        "% CLE",
        "Age Group",
        "Gender",
        "Block MOPS",
    ],
    errors="ignore"
)

# Merge con maestro
df_retail = df_retail.merge(
    df_ms[ms_cols],
    on="Material",
    how="left",
)

print("\n✅ PASO R2.1 OK – Retail enriquecido desde MS_ARTICULOS")
print(
    df_retail[
        [
            "Material",
            "ORIGEN",
            "SportsCode",
            "Product group",
            "RID",
            "Business Seg. Text",
            "Carry over pasado",
            "Carry over futuro",
            "Categoría",
            "CLE",
            "% CLE",
            "Age Group",
            "Gender",
            "Block MOPS",
        ]
    ].head(10)
)


# ==============================================================
# PASO R2.2.1 – APLICAR BLOQUEADO A RETAIL (FORMA CORRECTA)
# ==============================================================

# --------------------------------------------------------------
# 1. Identificar explícitamente NO BLOQUEADO
# --------------------------------------------------------------
mask_no_bloqueado = (
    df_retail["Block MOPS"]
    .fillna("")
    .astype(str)
    .str.upper()
    .isin(["NO BLOQUEADO", ""])
)

# Separar universos
df_retail_no_bloq = df_retail[mask_no_bloqueado].copy()
df_retail_calc = df_retail[~mask_no_bloqueado].copy()

# Forzar resultado correcto para NO BLOQUEADO
df_retail_no_bloq["Bloqueado"] = "No Bloqueado"
df_retail_no_bloq["bloqueos_raw"] = ""

print("\nFilas NO BLOQUEADO (excluidas del motor):", len(df_retail_no_bloq))
print("Filas a evaluar por el motor:", len(df_retail_calc))


# --------------------------------------------------------------
# 2. Preparar input técnico SOLO para filas con bloqueo
# --------------------------------------------------------------
df_retail_calc["bloqueos_raw"] = (
    df_retail_calc["Block MOPS"]
    .fillna("")
    .astype(str)
    .str.upper()
    .str.replace("SI -", "", regex=False)
    .str.replace("ALL CH", "ALL", regex=False)
    .str.replace(" ", ",", regex=False)
    .str.replace(",,", ",", regex=False)
    .str.strip(",")
)


# --------------------------------------------------------------
# 3. Cargar AUXILIAR – bloqueos_por_canal
# --------------------------------------------------------------
df_bloqueos_rules = pd.read_excel(
    "data/AUXILIAR_RULES.xlsx",
    sheet_name="bloqueos_por_canal",
    dtype=str
)

df_bloqueos_rules["canal"] = (
    df_bloqueos_rules["canal"].astype(str).str.strip().str.upper()
)
df_bloqueos_rules["bloqueo_code"] = (
    df_bloqueos_rules["bloqueo_code"].astype(str).str.strip().str.upper()
)
df_bloqueos_rules["bloqueado_result"] = (
    df_bloqueos_rules["bloqueado_result"].astype(str).str.strip()
)


# --------------------------------------------------------------
# 4. Aplicar motor SOLO a filas correspondientes
# --------------------------------------------------------------
print("\nAplicando motor de Bloqueado SOLO a filas con bloqueo declarado...")

df_retail_calc = apply_bloqueo_engine(
    df_base=df_retail_calc,
    df_bloqueos=df_bloqueos_rules
)


# --------------------------------------------------------------
# 5. Recombinar resultados
# --------------------------------------------------------------
df_retail = pd.concat(
    [df_retail_no_bloq, df_retail_calc],
    ignore_index=True
)


# ==============================================================
# VALIDACIONES Y AUDITORÍA
# ==============================================================

print("\n--- Distribución Bloqueado – Retail (FINAL) ---")
print(df_retail["Bloqueado"].value_counts())

print("\n--- Conteo de Bloqueados por Block MOPS y Canal ---")
print(
    df_retail[df_retail["Bloqueado"] == "Bloqueado"]
        .groupby(["Block MOPS", "Canal"])
        .size()
        .sort_values(ascending=False)
)

df_bloqueados_retail = (
    df_retail[df_retail["Bloqueado"] == "Bloqueado"]
    .sort_values(["Block MOPS", "Material"])
)

print("\nTotal filas BLOQUEADAS en Retail:", len(df_bloqueados_retail))

df_bloqueados_retail.to_excel(
    "data/RETAIL_BLOQUEADOS_DEBUG.xlsx",
    index=False
)

print("\n✅ Export generado: data/RETAIL_BLOQUEADOS_DEBUG.xlsx")