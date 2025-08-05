import pandas as pd
from bs4 import BeautifulSoup
import glob
import os
import numpy as np

def extract_all_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # 1) Localizar la tabla de resultados por su id parcial "dgResultado"
    table = soup.find('table', id=lambda x: x and 'dgResultado' in x)
    if table is None:
        return []

    rows = table.find_all('tr')

    # 2) Encontrar el índice de la fila de encabezados (la que contiene "CODIGO")
    header_idx = None
    for i, row in enumerate(rows):
        texts = [c.get_text(strip=True).upper() for c in row.find_all(['td','th'])]
        # buscamos la fila cuyo segundo elemento sea "CODIGO"
        if len(texts) > 1 and texts[1] in ('CODIGO','CÓDIGO'):
            header_idx = i
            break
    if header_idx is None:
        return []

    # 3) Extraer nombres de columnas (omitimos la primera celda vacía)
    header_cells = rows[header_idx].find_all(['td','th'])
    headers = [c.get_text(strip=True) for c in header_cells][1:]

    # 4) Recorrer cada fila de datos tras el encabezado
    records = []
    for row in rows[header_idx+1:]:
        cells = row.find_all('td')
        # descartamos líneas cortas (por ejemplo, paginador u otras)
        if len(cells) < len(headers) + 1:
            continue

        # tomamos justo las siguientes len(headers) celdas
        vals = [c.get_text(strip=True) for c in cells[1:1+len(headers)]]
        # reemplazamos '' o '--' por NaN
        vals = [np.nan if v in ('','--') else v for v in vals]

        record = dict(zip(headers, vals))
        records.append(record)

    return records

def process_html_dir(input_dir, output_csv):
    html_files = glob.glob(os.path.join(input_dir, '*.htm*'))
    all_data = []
    for fn in html_files:
        with open(fn, 'r', encoding='utf-8', errors='ignore') as f:
            html = f.read()
        all_data.extend(extract_all_from_html(html))

    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"Guardados {len(all_data)} registros en {output_csv}")
    else:
        print("No se extrajo ningún registro.")

if __name__ == "__main__":
    carpeta_html = r"C:\Users\irvin\UVG\Octavo_Semestre\Data_Science\DS_PROY1\htmls"
    destino_csv  = "establecimientos.csv"
    process_html_dir(carpeta_html, destino_csv)
