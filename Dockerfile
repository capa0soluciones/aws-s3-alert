# imagen de docker para python
FROM python:3.11

# setea el directorio de trabajo
WORKDIR /usr/src/app
# copia el archivo de requerimientos
COPY requirements.txt ./
# instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt
# copia el resto de los archivos
COPY . .
# ejecuta la aplicacion
CMD ["python", "app.py"]