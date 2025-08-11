
from __future__ import annotations
import re, unicodedata, itertools
from typing import Tuple, Dict, Any, List, Iterable
import pandas as pd
import numpy as np

# -----------------------------
# Normalización de Celdas (nueva revisión)
# -----------------------------

_ABBR = {
    "AV.": "AVENIDA", "AV ": "AVENIDA ",
    "CALZ.": "CALZADA", "CALZ ": "CALZADA ",
    "BLVD.": "BOULEVARD", "BLVD ": "BOULEVARD ",
    "7MA": "SÉPTIMA", "7A ": "SÉPTIMA ", "5TA": "QUINTA", "5A ": "QUINTA ",
    "Z.": "ZONA", "ZONA ": "ZONA ",
    "NO.": "", "Nº": "", "NUM.": "", "NUMERO": "",
    "EDF.": "EDIFICIO", "EDIF.": "EDIFICIO",
    "KM.": "KILOMETRO", "KM ": "KILOMETRO ",
    "B°": "BARRIO", "BRR.": "BARRIO",
    "COL.": "COLONIA",
    "ESQ.": "ESQUINA",
}
_STOPWORDS_NOMBRE = {
    "INSTITUTO","COLEGIO","ESCUELA","LICEO","CENTRO","EDUCATIVO","NACIONAL",
    "MIXTO","OFICIAL","URBANO","RURAL","PRIVADO","POR","COOPERATIVA"
}

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s

def normalize_text(s: Any) -> str:
    s = "" if s is np.nan or s is None else str(s)
    s = _strip_accents(s).upper()
    # espacios y puntuación
    s = re.sub(r"[\.\,\;\:\#\(\)\[\]\{\}/\\\-]+", " ", s)
    # remplazar abreviaturas comunes
    for k,v in _ABBR.items():
        s = s.replace(k, v)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_nombre(s: Any) -> str:
    s = normalize_text(s)
    tokens = [t for t in s.split() if t not in _STOPWORDS_NOMBRE]
    return " ".join(tokens)

def normalize_direccion(s: Any) -> str:
    return normalize_text(s)

def normalize_telefono(raw: Any) -> str:
    """Devuelve 8 dígitos o 'SIN_TELEFONO'. Si hay varios, devuelve el primero válido y
    agrega el resto en TELEFONOS_LIST al agrupar."""
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return "SIN_TELEFONO"
    s = str(raw)
    digitos = re.findall(r"(\d{4}[\s\-]?\d{4})", s)
    if not digitos:
        return "SIN_TELEFONO"
    tel = re.sub(r"\D","", digitos[0])
    return tel if len(tel)==8 else "SIN_TELEFONO"

def token_set(s: str) -> set:
    return set([t for t in s.split() if len(t)>1])

def jaccard(a: str, b: str) -> float:
    A, B = token_set(a), token_set(b)
    if not A or not B: return 0.0
    return len(A & B) / len(A | B)

# -----------------------------
# Unión de departamentos
# -----------------------------

def union_departamentos(filepaths: Iterable[str], departamento_from_filename: bool=True) -> pd.DataFrame:
    frames = []
    for fp in filepaths:
        df = pd.read_csv(fp)
        cols_upper = {c: c.upper() for c in df.columns}
        df.rename(columns=cols_upper, inplace=True)
        if "DEPARTAMENTO" not in df.columns and departamento_from_filename:
            dep = Path(fp).stem
            dep = re.sub(r"(?i)(establecimientos|diversificado|datos|csv)","", dep).replace("_"," ").strip().upper()
            df["DEPARTAMENTO"] = dep
        frames.append(df)
    if not frames:
        raise ValueError("No se leyeron CSVs. Verifique la ruta/patrón.")
    out = pd.concat(frames, ignore_index=True)
    for col in ["MUNICIPIO","DISTRITO","ESTABLECIMIENTO","DIRECCION","TELEFONO"]:
        if col not in out.columns:
            out[col] = np.nan
    return out

# -----------------------------
# Detección y resolución de duplicados
# -----------------------------

def preparar_campos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # normalizados
    df["ESTABLECIMIENTO_NORM"] = df["ESTABLECIMIENTO"].map(normalize_nombre) if "ESTABLECIMIENTO" in df else ""
    df["DIRECCION_NORM"] = df["DIRECCION"].map(normalize_direccion) if "DIRECCION" in df else ""
    df["TELEFONO_ESTD"] = df["TELEFONO"].map(normalize_telefono) if "TELEFONO" in df else "SIN_TELEFONO"
    # llave de bloqueo para comparar solo candidatos similares
    mun = df["MUNICIPIO"].fillna("").map(normalize_text) if "MUNICIPIO" in df else ""
    df["CLAVE_BLOQUEO"] = (
        mun.astype(str).str[:20] + "|" +
        df["ESTABLECIMIENTO_NORM"].str[:16] + "|" +
        df["DIRECCION_NORM"].str[:12]
    )
    return df

def _agg_telefonos(series: pd.Series) -> Tuple[str, List[str]]:
    tels = []
    for v in series.astype(str).tolist():
        if v and v != "SIN_TELEFONO":
            v = re.sub(r"\D","", v)
            if len(v)==8:
                tels.append(v)
    unique = list(dict.fromkeys(tels))
    principal = unique[0] if unique else "SIN_TELEFONO"
    return principal, unique

def deduplicar(df: pd.DataFrame, similarity_threshold: float=0.90) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df.copy(), df.copy()

    base = preparar_campos(df)

    horarios_cols = [c for c in base.columns if c.upper() in {"JORNADA","HORARIO"}]
    def _agg_group(g: pd.DataFrame) -> pd.Series:
        tel_pri, tel_list = _agg_telefonos(g["TELEFONO_ESTD"]) if "TELEFONO_ESTD" in g else ("SIN_TELEFONO", [])
        row = g.iloc[0].copy()
        row["TELEFONO_ESTD"] = tel_pri
        row["TELEFONOS_LIST"] = ",".join(tel_list) if tel_list else ""
        for hcol in horarios_cols:
            row[hcol] = "|".join(sorted(set(g[hcol].dropna().astype(str))))
        return row

    # Primera revisión, duplicados exactos
    exact_key = ["DEPARTAMENTO","MUNICIPIO","ESTABLECIMIENTO_NORM","DIRECCION_NORM","TELEFONO_ESTD"]
    for k in exact_key:
        if k not in base.columns:
            base[k] = ""

    agrupado = base.groupby(exact_key, dropna=False, as_index=False).apply(_agg_group).reset_index(drop=True)

    # Segunda revisión, duplicados probables
    try:
        from rapidfuzz import fuzz
        def _sim(a,b): return max(fuzz.token_set_ratio(a,b), fuzz.QRatio(a,b))/100.0
        def _cond(rowA, rowB):
            same_phone = rowA["TELEFONO_ESTD"] != "SIN_TELEFONO" and rowA["TELEFONO_ESTD"] == rowB["TELEFONO_ESTD"]
            sim_nom = _sim(rowA["ESTABLECIMIENTO_NORM"], rowB["ESTABLECIMIENTO_NORM"])
            sim_dir = _sim(rowA["DIRECCION_NORM"], rowB["DIRECCION_NORM"])
            return same_phone or (sim_nom >= similarity_threshold and sim_dir >= 0.85)
    except Exception:
        def _cond(rowA, rowB):
            same_phone = rowA["TELEFONO_ESTD"] != "SIN_TELEFONO" and rowA["TELEFONO_ESTD"] == rowB["TELEFONO_ESTD"]
            sim_nom = jaccard(rowA["ESTABLECIMIENTO_NORM"], rowB["ESTABLECIMIENTO_NORM"])
            sim_dir = jaccard(rowA["DIRECCION_NORM"], rowB["DIRECCION_NORM"])
            return same_phone or (sim_nom >= similarity_threshold and sim_dir >= 0.85)

    posibles_rows = []
    cleaned_rows = []
    for _, g in agrupado.groupby("CLAVE_BLOQUEO", dropna=False):
        n = len(g)
        if n == 1:
            cleaned_rows.append(g.iloc[0])
            continue
        used = set()
        for i in range(n):
            if i in used: continue
            base_row = g.iloc[i].copy()
            cluster_idx = [i]
            for j in range(i+1, n):
                if j in used: continue
                if _cond(base_row, g.iloc[j]):
                    cluster_idx.append(j)
                    used.add(j)
            if len(cluster_idx) == 1:
                cleaned_rows.append(base_row)
            else:
                cluster = g.iloc[cluster_idx]
                merged = _agg_group(cluster)
                cleaned_rows.append(merged)
        # guardar pares dudosos con similitud intermedia para revisión
        # (aquí incluimos cualquier par del bloque que NO se fusionó automáticamente pero podría ser similar)
        for i in range(n):
            for j in range(i+1, n):
                a, b = g.iloc[i], g.iloc[j]
                if not _cond(a,b):
                    sim_n = jaccard(a["ESTABLECIMIENTO_NORM"], b["ESTABLECIMIENTO_NORM"])
                    sim_d = jaccard(a["DIRECCION_NORM"], b["DIRECCION_NORM"])
                    if max(sim_n, sim_d) >= 0.70:
                        posibles_rows.append({
                            "DEPARTAMENTO": a.get("DEPARTAMENTO",""),
                            "MUNICIPIO": a.get("MUNICIPIO",""),
                            "ESTAB_A": a["ESTABLECIMIENTO"],
                            "ESTAB_B": b["ESTABLECIMIENTO"],
                            "DIR_A": a["DIRECCION"],
                            "DIR_B": b["DIRECCION"],
                            "TEL_A": a["TELEFONO_ESTD"],
                            "TEL_B": b["TELEFONO_ESTD"],
                            "SIM_NOMBRE": round(sim_n,3),
                            "SIM_DIRECCION": round(sim_d,3)
                        })

    df_clean = pd.DataFrame(cleaned_rows).reset_index(drop=True)
    df_posibles = pd.DataFrame(posibles_rows).reset_index(drop=True)
    prefer = ["DEPARTAMENTO","MUNICIPIO","DISTRITO","ESTABLECIMIENTO","ESTABLECIMIENTO_NORM","DIRECCION","DIRECCION_NORM",
              "TELEFONO","TELEFONO_ESTD","TELEFONOS_LIST"]
    cols = [c for c in prefer if c in df_clean.columns] + [c for c in df_clean.columns if c not in prefer]
    df_clean = df_clean[cols]
    return df_clean, df_posibles


def limpiar_y_unir(
    archivos_departamentos_glob: str|Iterable[str]=None,
    df_ya_unido: pd.DataFrame|None=None,
    similarity_threshold: float=0.90
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df_ya_unido is None and archivos_departamentos_glob is None:
        raise ValueError("Proporcione archivos_departamentos_glob o df_ya_unido.")
    if df_ya_unido is None:
        import glob
        files = sorted(glob.glob(archivos_departamentos_glob)) if isinstance(archivos_departamentos_glob, str) else list(archivos_departamentos_glob)
        df = union_departamentos(files)
    else:
        df = df_ya_unido.copy()
        cols_upper = {c: c.upper() for c in df.columns}
        df.rename(columns=cols_upper, inplace=True)
        for col in ["DEPARTAMENTO","MUNICIPIO","DISTRITO","ESTABLECIMIENTO","DIRECCION","TELEFONO"]:
            if col not in df.columns:
                df[col] = np.nan

    df_limpio, posibles = deduplicar(df, similarity_threshold=similarity_threshold)
    return df_limpio, posibles
