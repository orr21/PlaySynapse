FROM mageai/mageai:latest

# 3. Instalar dependencias de Python
COPY requirements.txt /home/src/requirements.txt
RUN pip install --no-cache-dir -r /home/src/requirements.txt

WORKDIR /home/src
EXPOSE 6789