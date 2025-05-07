FROM apache/airflow:2.8.2-python3.10

COPY requirements.txt /requirements.txt

# Utiliser le mécanisme recommandé par l'image officielle pour installer des paquets
USER airflow

RUN pip install --no-cache-dir --user -r /requirements.txt

# Ajouter ~/.local/bin au PATH (car pip --user y installe airflow & autres binaires)
ENV PATH="/home/airflow/.local/bin:$PATH"
