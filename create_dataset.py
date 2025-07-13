import re
import pandas as pd
from bs4 import BeautifulSoup
import sys
import os

def extract_data_from_file(file_path):
    """Extrae los datos clave de un archivo XLS con estructura HTML"""
    encodings = ['utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"No se pudo decodificar el archivo {file_path}")

    soup = BeautifulSoup(content, 'html.parser')
    text = soup.get_text()
    
    # Diccionario para almacenar los datos extraídos
    data = {
        'Código': None,
        'Departamento': None,
        'Municipio': None,
        'Establecimiento': None,
        'Dirección': None,
        'Teléfono': None,
        'Distrito Supervisión': None,
        'Supervisor': None,
        'Director': None,
        'Departamental': None,
        'Nivel': None,
        'Sector': None,
        'Area': None,
        'Jornada': None,
        'Plan': None,
        'Modalidad': None,
        'Estado Actual': None
    }
    
    # Patrones mejorados para extracción precisa
    patterns = {
        'Código': r'Código\s+(\d{2}-\d{2}-\d{4}-\d{2})',
        'Departamento': r'Departamento\s+([^\n]+?)\s*Municipio',
        'Municipio': r'Municipio\s+([^\n]+?)\s*Establecimiento',
        'Establecimiento': r'Establecimiento\s+([^\n]+?)\s*Dirección',
        'Dirección': r'Dirección\s+([^\n]+?)\s*Teléfono',
        'Teléfono': r'Teléfono\s+([^\n]+?)\s*Distrito Supervisión',
        'Distrito Supervisión': r'Distrito Supervisión\s+([^\n]+?)\s*Supervisor',
        'Supervisor': r'Supervisor\s+([^\n]+?)\s*Director',
        'Director': r'Director\s+([^\n]+?)\s*Departamental',
        'Departamental': r'Departamental\s+([^\n]+?)\s*Nivel:',
        'Nivel': r'Nivel:\s+([^\n]+?)\s*Sector:',
        'Sector': r'Sector:\s+([^\n]+?)\s*Area:',
        'Area': r'Area:\s+([^\n]+?)\s*Jornada:',
        'Jornada': r'Jornada:\s+([^\n]+?)\s*Plan:',
        'Plan': r'Plan:\s+([^\n]+?)\s*Modalidad:',
        'Modalidad': r'Modalidad:\s+([^\n]+?)\s*Estado Actual:',
        'Estado Actual': r'Estado Actual:\s+([^\n]+)'
    }
    
    for field, pattern in patterns.items():
        match = re.search(pattern, text, re.DOTALL)
        if match:
            data[field] = match.group(1).strip()
    
    return data

def process_files(file_paths, output_csv):
    """Procesa múltiples archivos y guarda los resultados en un CSV"""
    all_data = []
    
    for file_path in file_paths:
        try:
            print(f"Procesando archivo: {file_path}")
            data = extract_data_from_file(file_path)
            all_data.append(data)
        except Exception as e:
            print(f"Error al procesar {file_path}: {str(e)}")
    
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"\nDatos guardados en {output_csv}")
        print(f"Total de registros procesados: {len(all_data)}")
    else:
        print("No se encontraron datos para exportar.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python create_dataset.py directorio output.csv")
    else:
        input_path = sys.argv[1]
        output_file = sys.argv[2] if sys.argv[2].endswith('.csv') else sys.argv[2] + '.csv'
        
        if os.path.isdir(input_path):
            input_files = [os.path.join(input_path, f) for f in os.listdir(input_path) 
                         if f.lower().endswith('.xls')]
        else:
            input_files = [input_path]
        
        process_files(input_files, output_file)