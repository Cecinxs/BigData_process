#CRIAÇÃO DE UM MANIFESTO PARA BIG DATA FILE SHARE(BDFS) ARCGIS GEOANALYTICS TOOLS
#Esse script lê todos os arquivos .parquet de uma pasta e transforma em um "manifesto".json já estruturado. Esse 
#manifesto irá ser lido no GeoAnalytics server do Arcgis para rodar um processo em BDFS.

#O dado em .parquet é em ponto, contém 89milhões de registro e possui estrutura simples de latlong e duas colunas numéricas.

import json
import os

# Caminho da pasta onde estão os arquivos Parquet
caminho_arquivos = r"C:\caminho\para\seus\arquivos"

# Lista de arquivos parquet
arquivos = [f for f in os.listdir(caminho_arquivos) if f.endswith(".parquet")]

# Nome da camada que será lida pelo BDFS
nome_layer = "meus_pontos"

# Campos dos dados
latitude_field = "latitude"
longitude_field = "longitude"

# Caminhos relativos dos arquivos
paths = [os.path.join(".", f).replace("\\", "/") for f in arquivos]

# Estrutura do manifesto
manifesto = {
    "type": "parquet",
    "name": nome_layer,
    "geometry": {
        "geometryType": "esriGeometryPoint",
        "spatialReference": {"wkid": 4326},
        "fields": [
            {
                "name": longitude_field,
                "formats": ["x"]
            },
            {
                "name": latitude_field,
                "formats": ["y"]
            }
        ]
    },
    "fields": {
        "Col1": "esriFieldTypeBigInteger",
        "Col2": "esriFieldTypeBigInteger"
        # Adicione outros campos se necessário
    },
    "datasets": [{"path": path} for path in paths]
}

# Salva o arquivo manifest.json
caminho_manifest = os.path.join(caminho_arquivos, "manifest.json")
with open(caminho_manifest, "w") as f:
    json.dump(manifesto, f, indent=4)

print("Manifesto criado com sucesso!")