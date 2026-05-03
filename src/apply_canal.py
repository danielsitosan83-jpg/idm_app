import pandas as pd


def _match_value(rule_value, data_value):
    """
    Evalúa match simple con wildcard (*)
    """
    if rule_value == "*" or rule_value == "ANY":
        return True
    return rule_value == data_value


def _match_customer(rule_value, match_type, customer_name):
    """
    Evalúa match contra Customer Name según el tipo de regla
    """
    if rule_value == "*" or match_type == "ANY":
        return True

    if customer_name is None or pd.isna(customer_name):
        return False

    customer_name = str(customer_name).upper()

    if match_type == "EXACT":
        return customer_name == rule_value

    if match_type == "CONTAINS":
        return rule_value in customer_name

    if match_type == "NOT_CONTAINS":
        return rule_value not in customer_name

    if match_type == "EXACT_LIST":
        values = [v.strip() for v in rule_value.split("\\")]
        return customer_name in values

    return False


def apply_canal(df, canal_rules_df):
    """
    Aplica el motor de reglas de canal.
    Devuelve df con columna 'Canal'.
    """
    df = df.copy()

    # Normalización base
    canal_rules_df = canal_rules_df.copy()
    canal_rules_df.columns = [c.strip() for c in canal_rules_df.columns]

    # Ordenar por prioridad (menor gana)
    canal_rules_df["prioridad"] = pd.to_numeric(
        canal_rules_df["prioridad"], errors="coerce"
    )
    canal_rules_df = canal_rules_df.sort_values("prioridad")

    # Normalizar columnas usadas
    df["StockCategory"] = df["StockCategory"].astype(str).str.strip().str.upper()
    df["MRP Stat"] = df["MRP Stat"].astype(str).str.strip().str.upper()
    df["Order Type"] = df["Order Type"].astype(str).str.strip().str.upper()
    df["Cust. Name"] = df["Cust. Name"].astype(str).str.strip().str.upper()

    canales = []

    # Iterar fila por fila (motor de reglas)
    for _, row in df.iterrows():
        canal_final = None

        for _, rule in canal_rules_df.iterrows():
            if not _match_value(rule["stock_category"], row["StockCategory"]):
                continue

            if not _match_value(rule["mrp_stat"], row["MRP Stat"]):
                continue

            if not _match_value(rule["order_type"], row["Order Type"]):
                continue

            if not _match_customer(
                rule["customer_name_rule"],
                rule["customer_name_match_type"],
                row["Cust. Name"],
            ):
                continue

            # Primera regla que matchea gana
            canal_final = rule["canal_result"]
            break

        canales.append(canal_final)

    df["Canal"] = canales
    return df