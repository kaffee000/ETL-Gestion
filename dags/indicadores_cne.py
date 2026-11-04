# Archivo: dags/indicadores_cne.py
"""
Relación entre indicadores CNE y CMF. Los KPIs se calculan en SQL (sql/mart_kpis.sql); aquí solo se
evalúa la significancia de las correlaciones, que requiere permutaciones.
"""
from itertools import permutations

import numpy as np
import pandas as pd


# ----------------------------------------------------------------- Relación CNE - CMF
def _rangos(a):
    return pd.Series(a).rank().to_numpy()


def _p_permutacion(x, y, semilla=7):
    """p bilateral de Pearson por permutaciones: válido con pocos años, sin supuesto de normalidad."""
    n = len(x)
    xz = x - x.mean()
    yz = y - y.mean()
    denom = np.sqrt((xz ** 2).sum() * (yz ** 2).sum())
    if denom == 0:
        return float("nan"), float("nan")
    r = float(xz @ yz / denom)
    if n <= 7:
        perms = np.array(list(permutations(range(n))))
    else:
        perms = np.array([np.random.default_rng(semilla + i).permutation(n) for i in range(5000)])
    distrib = (yz[perms] @ xz) / denom
    return r, float((np.abs(distrib) >= abs(r) - 1e-12).mean())


def _q_bh(pvalores: pd.Series) -> pd.Series:
    """Ajuste Benjamini-Hochberg: se prueban muchos pares y algunos saldrían altos por azar."""
    p = pvalores.dropna().sort_values()
    if p.empty:
        return pd.Series(np.nan, index=pvalores.index)
    q = (p * len(p) / np.arange(1, len(p) + 1))[::-1].cummin()[::-1].clip(upper=1)
    return q.reindex(pvalores.index)


def correlaciones(panel: pd.DataFrame, n_min=4) -> pd.DataFrame:
    """
    Correlaciones por banco, indicador CMF e indicador CNE, en dos formas:
      nivel:     valores anuales; sensible a tendencias comunes.
      variacion: cambios año a año; quita esas tendencias, pero con un punto menos.
    Son asociaciones exploratorias entre pocos años: no prueban causalidad.
    """
    columnas = ["institucion", "indicador_cmf", "indicador_cne", "tipo", "n", "anio_desde", "anio_hasta",
                "pearson", "spearman", "p_valor", "q_valor", "lectura"]
    panel = panel.assign(valor_cmf=panel["valor_cmf"].astype(float), valor_cne=panel["valor_cne"].astype(float))
    filas = []
    for llaves, g in panel.groupby(["institucion", "indicador_cmf", "indicador_cne"]):
        g = g.dropna(subset=["valor_cmf", "valor_cne"]).sort_values("anio")
        consecutivos = g["anio"].diff() == 1
        variacion = pd.DataFrame({"x": g["valor_cmf"].diff(), "y": g["valor_cne"].diff(),
                                  "anio": g["anio"]})[consecutivos]
        nivel = pd.DataFrame({"x": g["valor_cmf"], "y": g["valor_cne"], "anio": g["anio"]})
        for tipo, datos in (("nivel", nivel), ("variacion", variacion)):
            x, y = datos["x"].to_numpy(float), datos["y"].to_numpy(float)
            if len(x) < n_min or x.std() == 0 or y.std() == 0:
                continue
            r, p = _p_permutacion(x, y)
            rho = float(np.corrcoef(_rangos(x), _rangos(y))[0, 1])
            filas.append({"institucion": llaves[0], "indicador_cmf": llaves[1], "indicador_cne": llaves[2],
                          "tipo": tipo, "n": len(x), "anio_desde": int(datos["anio"].min()),
                          "anio_hasta": int(datos["anio"].max()), "pearson": r, "spearman": rho, "p_valor": p})
    if not filas:
        return pd.DataFrame(columns=columnas)
    df = pd.DataFrame(filas)
    df["q_valor"] = _q_bh(df["p_valor"])

    def lectura(f):
        signo = "positiva" if f["pearson"] > 0 else "negativa"
        if f["q_valor"] < 0.10:
            return f"asociación {signo} (exploratoria)"
        if abs(f["pearson"]) >= 0.7:
            return f"indicio {signo}, no concluyente"
        return "sin relación clara"

    df["lectura"] = df.apply(lectura, axis=1)
    df[["pearson", "spearman", "p_valor", "q_valor"]] = df[["pearson", "spearman", "p_valor", "q_valor"]].round(4)
    return df[columnas].sort_values(["q_valor", "p_valor"]).reset_index(drop=True)
