# PlaySynapse: Sistema de Analítica Deportiva

**Video Demo:** [https://youtu.be/yp8N_ug8NPM](https://youtu.be/yp8N_ug8NPM)

> **Trabajo Fin de Máster - Máster en Big Data & Data Engineering**  
> **Autor:** Óscar Rico Rodríguez  
> **Tutores:** Jorge Centeno y Alberto González  
> **Fecha:** Febrero 2026

## 📄 Resumen

**PlaySynapse** es una plataforma de ingeniería de datos diseñada para unificar la analítica deportiva histórica con la inmediatez de la inteligencia artificial. El proyecto responde a la necesidad de integrar fuentes heterogéneas —desde registros estadísticos históricos hasta flujos de datos de visión por computador— en una arquitectura común que democratice el acceso a insights avanzados.

La solución implementada se basa en una arquitectura **Lakehouse contenerizada**. Para la gestión de datos históricos, se han desarrollado pipelines Batch orquestados con **Mage.ai** y procesados con **Polars**. Para el componente de tiempo real, se ha desplegado un sistema de Streaming basado en **Redpanda** que ingesta y procesa eventos de juego simulados, emulando la salida de modelos de visión artificial. Finalmente, se ha integrado un módulo de **IA Generativa** (Llama 3 vía Groq) que transforma estos datos estructurados en narrativas tácticas ("Andrés Montes") visualizadas en tiempo real.

## 🎯 Objetivos del Proyecto

1.  **Pipeline de Datos Unificado**: Diseñar un flujo capaz de procesar tanto información histórica como eventos en tiempo real generados por modelos de visión artificial.
2.  **Arquitectura Híbrida**: Implementar un sistema Batch + Streaming eficiente con almacenamiento Lakehouse (Medallion Architecture).
3.  **Insights con GenAI**: Aplicar técnicas de inteligencia artificial para generar narrativas analíticas y comentarios en lenguaje natural a partir de métricas deportivas.
4.  **Escalabilidad Multideporte**: Diseñar la plataforma con una visión agnóstica al deporte, permitiendo su adaptación a otros dominios de video-analítica.

## 🚀 Características Principales

- **Simulación en Tiempo Real**: Capacidad de reproducir partidos históricos sincronizados evento a evento.
- **Streaming de Baja Latencia**: Arquitectura basada en eventos utilizando Redpanda (Kafka compatible).
- **Data Lakehouse**: Almacenamiento escalable en MinIO (S3 compatible) con formato Delta Lake.
- **Narración Automática**: Comentarista IA en vivo utilizando Groq (Llama 3).
- **Orquestación Moderna**: Pipelines de datos gestionados con Mage.ai.
- **Visualización Interactiva**: Dashboard analítico (Streamlit) y App de narración en vivo (Gradio).

## 🛠️ Arquitectura del Sistema

El flujo de datos sigue una arquitectura moderna de streaming y batch:

```mermaid
graph TD
    A[Simulator / NBA API] -->|Raw Events| B(Redpanda: nba_live)
    B -->|Consumer| C{Mage: Streaming Pipeline}
    C -->|Processed Events + AI Prompt| D(Redpanda: nba_gold_events)
    D -->|Consumer| E[NBA Live App (Gradio)]
    E -->|Narrative Generation| F[Groq LLM]

    G[Populate Historical Data] -->|Batch Pipeline| H{Mage: Batch ETL}
    H -->|Write Delta Tables| I[(MinIO: Bronze/Silver/Gold)]
    I -->|Query| J[NBA Dashboard (Streamlit)]
```

## 🏗️ Estructura del Proyecto

- **`data_platform/`**: Orquestador **Mage**. Contiene los pipelines de datos (ingesta, transformación, streaming).
- **`nba_dashboard/`**: Aplicación **Streamlit** para visualización analítica histórica.
- **`nba_live_app/`**: Aplicación **Gradio** para la narración en vivo con IA.
- **`realtime_simulator.py`**: Script de simulación que inyecta datos de partidos al sistema en tiempo real.
- **`docker-compose.yml`**: Definición de infraestructura (contenedores).

## 💻 Requisitos Previos

- **Docker** y **Docker Compose**.
- **Python 3.9+** (opcional, para scripts locales).
- API Key de **Groq** (para la narración con IA).

## 🚀 Inicio Rápido

### 1. Configuración

Clona el repositorio y configura las variables de entorno:

```bash
cp .env.example .env
# Edita .env y añade tu GROQ_API_KEY
```

### 2. Despliegue

```bash
docker-compose up -d
```

Servicios disponibles:

- **Mage (Orquestador):** `http://localhost:6789`
- **Redpanda Console:** `http://localhost:8080`
- **MinIO Console:** `http://localhost:9001`
- **NBA Live App:** `http://localhost:7860`
- **NBA Dashboard:** `http://localhost:8501`

### 3. Ejecución

1.  En **Mage**, activa el pipeline `nba_pbp_realtime`.
2.  Ejecuta el simulador localmente:
    ```bash
    pip install -r requirements.txt
    python realtime_simulator.py
    ```
3.  Abre la **NBA Live App** para ver la narración y el **NBA Dashboard** para análisis.

## 📊 Tecnologías Utilizadas

| Componente          | Tecnología            | Propósito                     |
| :------------------ | :-------------------- | :---------------------------- |
| **Lenguaje**        | Python 🐍             | Lógica principal              |
| **Orquestación**    | Mage.ai 🧙            | Pipelines Batch & Streaming   |
| **Streaming**       | Redpanda 🐼           | Broker de eventos (Kafka API) |
| **Storage**         | MinIO 🪣              | Data Lake (S3 API)            |
| **Formato**         | Delta Lake 🔺         | Tablas ACID                   |
| **Frontend**        | Streamlit & Gradio 🎨 | UI Analítica y Live           |
| **GenAI**           | Groq (Llama 3) ⚡     | Generación de texto           |
| **Infraestructura** | Docker 🐳             | Contenedores                  |
