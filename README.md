#  Analyse des données Netflix avec Streamlit & DuckDB

##  Présentation du projet

Ce projet a été réalisé dans le cadre d'un module pour ce semestre.  Nous avons choisi de travailler sur le sujet de Netflix.
L'objectif est de créer une application web interactive, développée avec Streamlit, qui permet de :

- Téléverser un fichier CSV contenant des données du catalogue **Netflix**.
- Stocker et interroger ces données à l'aide de **DuckDB**.
- Visualiser **quatre indicateurs clés de performance (KPI)** à travers des visualisations interactives.
- Filtrer dynamiquement les résultats par **année de sortie**, **type de contenu**, **pays** ou **genre**.

---

##  Installation et exécution

### 1. Prérequis

- Python 3.9 ou plus récent
- pip
- Git

### 2. Installation du projet

```bash
git clone https://github.com/nom-du-groupe/nom-du-projet.git
cd nom-du-projet
pip install -r requirements.txt

### 3. Lancement de l'application

streamlit run app.py



## Description de l'application

L'application développée permet d'analyser le catalogue de Netflix de manière interactive grâce à l’interface **Streamlit** et à l’intégration de **DuckDB**.  
Elle nous permet de :

- Charger un fichier CSV contenant les données de contenus Netflix.
- Visualiser quatre indicateurs clés (KPI) à l’aide de graphiques interactifs.
- Filtrer dynamiquement les résultats selon :
  - la **date de sortie**
  - le **pays**
  - le **type de contenu** (film ou série)
  - le **genre**


## Répartition des tâches

 Membre de l’équipe | Rôle et responsabilités 

 **Louis**           | Création de l'interface Streamlit, mise en place des composants interactifs (upload, filtres) |
 **Youssef**         | Intégration de DuckDB, écriture des requêtes SQL pour extraire les données filtrées |
 **Davin**           | Réalisation des visualisations des KPI (graphiques avec Plotly/Matplotlib), design des graphiques |
 **Shamini**         | Organisation du dépôt GitHub, gestion des branches, documentation technique et README |






