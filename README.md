# GE Stack — Great Expectations + PySpark + Docker

Stack complète de **Data Quality** avec interface web professionnelle.

## Architecture

```
Great_expectation_stack/

├── docker-compose.yml        # Orchestration Docker
├── nginx.conf                # Reverse proxy vers l'API
├── frontend                  # Interface web (HTML/CSS/JS vanilla)
│   ├── app.js
│   ├── index.html
│   └── style.css
├── data
│   |── sample.csv             # Dataset exemple (15 employés)          
├── ge_runner        
│   ├── app.py                 # API Flask (REST)
│   ├── Dockerfile             # Python 3.10 + Java + PySpark + GE
│   ├── ge_config
│   │   └── nginx.conf
│   ├── requirements.txt       # Dépendances Python
│   └── suites
├── README.md
├── spark
│   └── Dockerfile

```

## Services Docker

| Service     | Port | Description |
|-------------|------|-------------|
| `ge-api`    | 5000 | Flask API + PySpark + Great Expectations |
| `frontend`  | 8080 | Interface web via Nginx |

## Démarrage rapide

```bash
# 1. Se placer dans le dossier
cd ge-stack/

# 2. Build et démarrage (première fois ~5-10 min pour télécharger Spark)
docker compose up --build -d

# 3. Suivre les logs de l'API (Spark prend ~60s à démarrer)
docker compose logs -f ge-api

# 4. Ouvrir l'interface web
open http://localhost:8080

# 5. Ou accéder à l'API directement
curl http://localhost:5000/api/data/preview | python -m json.tool
```

## Utilisation de l'interface

### 1. Onglet "Données"
- Aperçu des 10 premières lignes du CSV
- Profil de chaque colonne (type, nulls, min/max/mean)
- Statistiques globales

### 2. Onglet "Suites"
Créez des suites d'expectations :
1. Cliquez **+ Nouvelle suite** → donnez un nom (ex: `employee_quality`)
2. Sélectionnez une suite dans la liste
3. Choisissez un type d'expectation dans le catalogue
4. Renseignez les paramètres et cliquez **Ajouter**

### 3. Onglet "Validation"
1. Sélectionnez une suite
2. Cliquez **Valider avec PySpark**
3. Consultez le résultat détaillé (pass/fail par expectation)

### 4. Onglet "Résultats"
Historique de toutes les validations (50 dernières).

## API REST

```bash
# Aperçu des données
GET /api/data/preview

# Profil des colonnes (stats)
GET /api/data/stats

# Catalogue des expectations disponibles
GET /api/expectations/catalogue

# Gestion des suites
GET    /api/suites
POST   /api/suites          {"name": "ma_suite"}
DELETE /api/suites/:name

# Gestion des expectations dans une suite
POST   /api/suites/:name/expectations
DELETE /api/suites/:name/expectations/:idx

# Validation
POST /api/validate/:suite_name

# Historique
GET    /api/results
DELETE /api/results
```

## Expectations disponibles

| Type | Paramètres |
|------|-----------|
| `expect_column_to_exist` | column |
| `expect_column_values_to_not_be_null` | column |
| `expect_column_values_to_be_unique` | column |
| `expect_column_values_to_be_between` | column, min_value, max_value |
| `expect_column_values_to_match_regex` | column, regex |
| `expect_column_values_to_be_in_set` | column, value_set |
| `expect_table_row_count_to_be_between` | min_value, max_value |
| `expect_column_mean_to_be_between` | column, min_value, max_value |

## Exemple de test rapide via curl

```bash
# Créer une suite
curl -X POST http://localhost:5000/api/suites \
  -H "Content-Type: application/json" \
  -d '{"name": "employee_quality"}'

# Ajouter une expectation : pas de null sur 'name'
curl -X POST http://localhost:5000/api/suites/employee_quality/expectations \
  -H "Content-Type: application/json" \
  -d '{"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "name"}}'

# Ajouter : salary doit être entre 0 et 200000
curl -X POST http://localhost:5000/api/suites/employee_quality/expectations \
  -H "Content-Type: application/json" \
  -d '{"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "salary", "min_value": 0, "max_value": 200000}}'

# Lancer la validation
curl -X POST http://localhost:5000/api/validate/employee_quality | python -m json.tool
```

## Ajouter vos propres données

Remplacez `data/sample.csv` par votre propre CSV. Le schéma est inféré automatiquement par PySpark.

## Arrêt et nettoyage

```bash
# Arrêter la stack
docker compose down

# Supprimer volumes (résultats, projets GE)
docker compose down -v

# Rebuild complet
docker compose up --build --force-recreate -d
```


## DEBUGGING
``` bash
# 1. Corriger les permissions sur l'hôte
chmod 644 ge-stack/frontend/index.html

# 2. Redémarrer le container (le volume sera remonté avec les bonnes permissions)
docker compose -f ge-stack/docker-compose.yml restart frontend

# 3. Tester
curl http://localhost:8080/
```

``` bash
# Copier les fichiers mis à jour dans votre dossier ge-stack
# puis redémarrer uniquement le container API (pas besoin de rebuild)
docker compose restart ge-api

# Pour le frontend, copier index.html et recharger le browser
docker cp frontend/index.html ge-frontend:/usr/share/nginx/html/index.html
# (ou faire un docker compose restart frontend si les permissions sont OK)
```

###

``` bash
curl -s http://localhost:5000/api/data/preview | python3 -m json.tool
```
### Vérification crash java


``` bash
docker compose logs ge-api | grep -E "(ERROR|WARN|Exception|Killed|OOM|java)" | tail -30

docker compose logs -f ge-api
```


``` bash
# Quel fichier est actif côté serveur ?
curl -s http://localhost:5000/api/data/files | python3 -m json.tool

# Est-ce que le select fonctionne ?
curl -s -X POST http://localhost:5000/api/data/select \
  -H "Content-Type: application/json" \
  -d '{"name": "VUE_BULLETIN_ALL_202602201016.csv"}' | python3 -m json.tool

# Preview après sélection
curl -s http://localhost:5000/api/data/preview | python3 -m json.tool
```