from bs4 import BeautifulSoup
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from PIL import Image
from io import BytesIO

# URL del archivo XML

def transformXml(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"#,
        #"Referer": "https://example.com"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        xml_content = response.content
    else:
        raise Exception(f"Error al descargar el archivo: {response.status_code}")

    # Parsear el contenido XML
    root = ET.fromstring(xml_content)

    # Extraer datos de cada <item>
    data = []
    for item in root.findall('.//item'):
        row = {
            "titulo": item.find('title').text if item.find('title') is not None else None,
            "canonica": item.find('link').text if item.find('link') is not None else None,
            #"guid": item.find('guid').text if item.find('guid') is not None else None,
            "fecha_publicacion": item.find('pubDate').text.strip() if item.find('pubDate') is not None else None,
            "copete": item.find('description').text if item.find('description') is not None else None,
            "cuerpo": item.find('{http://purl.org/rss/1.0/modules/content/}encoded').text.strip() 
                            if item.find('{http://purl.org/rss/1.0/modules/content/}encoded') is not None else None,
            "imagen_url": item.find('enclosure').attrib.get('url') if item.find('enclosure') is not None else None,
        }
        data.append(row)

    # Convertir a DataFrame
    df = pd.DataFrame(data)
    return df

# Ruta del archivo con las URLs
archivo_urls = "urls.txt"

# Leer el archivo de texto que contiene las URLs
# El archivo debe tener una URL por línea
with open(archivo_urls, "r") as file:
    urls = [line.strip() for line in file.readlines()]

print(urls)
# Crear una lista para almacenar los DataFrames
dataframes = []

# Procesar cada URL y acumular los resultados
for url in urls:
    df = transformXml(url)
    dataframes.append(df)

# Concatenar todos los DataFrames en uno solo
df = pd.concat(dataframes, ignore_index=True)

# Ver los primeros registros
print(df.head())

# Limpiar etiquetas HTML de una columna
def clean_html(text):
    if text:
        soup = BeautifulSoup(text, "html.parser")
        return soup.get_text()
    return None

# Aplicar la función a las columnas content_encoded y title
df['cuerpo'] = df['cuerpo'].apply(clean_html)
df['titulo'] = df['titulo'].apply(clean_html)

# Elimina 'https://www.' de todas las filas de la columna 'link'
df['canonica'] = df['canonica'].str.replace('https://www.', '', regex=False)

# Función para obtener las características de una imagen desde una URL
def get_image_properties(url):
    try:
        # Descargar la imagen
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, stream=True)
        
        if response.status_code == 200:
            # Calcular el peso en KB
            size_kb = len(response.content) / 1024
            
            # Leer la imagen con PIL
            image = Image.open(BytesIO(response.content))
            width, height = image.size
            mode = image.mode  # Profundidad de color (e.g., RGB, RGBA, etc.)
            
            return {"width": width, "height": height, "mode": mode, "size_kb": size_kb}
        else:
            return {"width": None, "height": None, "mode": None, "size_kb": None}
    except Exception as e:
        print(f"Error al procesar la imagen: {e}")
        return {"width": None, "height": None, "mode": None, "size_kb": None}

# Aplicar la función a la columna enclosure_url
image_properties = df['imagen_url'].apply(get_image_properties)

# Convertir los resultados a columnas separadas
df['imagen_ancho'] = image_properties.apply(lambda x: x['width'])
df['imagen_alto'] = image_properties.apply(lambda x: x['height'])
df['imagen_modo'] = image_properties.apply(lambda x: x['mode'])
df['imagen_tamano_kb'] = image_properties.apply(lambda x: x['size_kb'])

df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'])
df['imagen_ancho'] = pd.to_numeric(df['imagen_ancho'])
df['imagen_alto'] = pd.to_numeric(df['imagen_alto'])
df['imagen_tamano_kb'] = pd.to_numeric(df['imagen_tamano_kb'])

### Conexión a SQL
# Leo el archivo de configuración
#config = configparser.ConfigParser()
#config.read('config.ini')

#print(config)

# Construyo la cadena de conexión
#connection_string = (
#    f"DRIVER={{{config['DATABASE']['DRIVER']}}};"
#    f"SERVER={config['DATABASE']['SERVER']};"
#    f"DATABASE={config['DATABASE']['DATABASE']};"
#    f"UID={config['DATABASE']['UID']};"
#    f"PWD={config['DATABASE']['PWD']}"
#)

#connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
#engine = sqlalchemy.create_engine(connection_url,fast_executemany=True)
#engine.connect()
#print("hasta sql")

#df_sql = pd.read_sql(f"SELECT * FROM body_articulos_vincolo_dim", con=engine.connect())
df_sql = pd.read_csv('c:\\Users\\ctello\\OneDrive - El Cronista\\Documentos\\Python\\Scripts Cronista\\Análisis\\241122 - Descarga de archivos xml\\prueba.csv')
df['order'] = "segunda"
df_sql['order'] = "primera"

df_final = pd.concat([df,df_sql]).reset_index(drop=True)

df_final = df_final.sort_values('order')
df_final = df_final.drop_duplicates(subset=['canonica'], keep="first")

cols = ['fecha_publicacion','canonica','titulo','copete','cuerpo','imagen_url','imagen_ancho','imagen_alto','imagen_modo','imagen_tamano_kb']
df_final = df_final[cols]

type_dict = {'fecha_publicacion' : 'datetime64[ns]',
             'canonica' : 'object',
             'titulo' : 'object',
             'copete' : 'object',
             'cuerpo' : 'object',
             'imagen_url' : 'object',
             'imagen_ancho' : 'int',
             'imagen_alto' : 'int',
             'imagen_modo' : 'object',
             'imagen_tamano_kb' : 'int'
}
df_final = df_final.astype(type_dict)

#df.to_sql('body_articulos_vincolo_dim',con=engine.connect(), if_exists='append', index= False)
df_final.to_csv('c:\\Users\\ctello\\OneDrive - El Cronista\\Documentos\\Python\\Scripts Cronista\\Análisis\\241122 - Descarga de archivos xml\\prueba2.csv', index=False, encoding='utf-8')
