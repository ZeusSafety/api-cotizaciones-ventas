# Usar la imagen oficial de Python
FROM python:3.13-slim

# Establecer el directorio de trabajo
WORKDIR /workspace

# Copiar el archivo de dependencias
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la función
COPY main.py .

# Exponer el puerto que Google Cloud Functions usa
EXPOSE 8080

# Variable de entorno para el puerto
ENV PORT=8080

# Ejecutar la función usando functions-framework
# La función en main.py se llama 'ventasCotiza'
CMD exec functions-framework --target=ventasCotiza --port=$PORT --host=0.0.0.0