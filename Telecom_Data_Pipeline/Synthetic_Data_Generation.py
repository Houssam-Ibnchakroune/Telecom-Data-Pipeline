import random
from faker import Faker
from datetime import datetime, timedelta
fake = Faker()
import string
import uuid
# Liste des types d'enregistrement
import psycopg2

# --- Variables globales pour le timestamp et le compteur ---
# On démarre au 1er jour du mois en cours à 00:00:00
now = datetime.now()
current_ts = datetime(now.year, now.month, 1, 0, 0, 0)

# Compteur d'enregistrements générés
cdr_counter = 0
# Paramètres de connexion
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DBNAME   = "projet_spark"
PG_USER     = "postgres"
PG_PASSWORD = "0000"

# Se connecter à PostgreSQL
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DBNAME,
    user=PG_USER,
    password=PG_PASSWORD
)
cur = conn.cursor()
cur.execute("""Select customer_id from customers """)
customer_ids = [row[0] for row in cur.fetchall()]
# 4) Commit & nettoyage
conn.commit()
cur.close()
conn.close()

def generate_cdr():
    global current_ts, cdr_counter

    # Incrément du compteur
    cdr_counter += 1
    def maroc_msisdn():
        if random.random() > 0.05:  # 95% chance to return a number
            return random.choice(customer_ids)
        else:
            if random.random() > 0.5 :
                return None  # 5% chance of missing value
            else :
                chars = string.ascii_letters + string.digits  # a-z, A-Z, 0-9
                return ''.join(random.choices(chars, k=12))  
    # Dictionnaire indicative des indicatifs et longueurs typiques
    FOREIGN_CC = {
        "1":   10,   # USA/Canada
        "33":   9,   # France
        "44":  10,   # UK
        "49":  11,   # Allemagne
        "34":   9,   # Espagne
        "39":  10,   # Italie
        "7":   10,   # Russie
        "81":  10,   # Japon
        "971":  9,   # Émirats arabes unis
        "966":  9,   # Arabie saoudite
    }

    def generate_foreign_msisdn() -> str:
        """
        Génère un MSISDN étranger (sans le '+'), en choisissant aléatoirement
        un indicatif pays dans FOREIGN_CC et le nombre de chiffres approprié.
        """
        if random.random() > 0.05:
            cc = random.choice(list(FOREIGN_CC.keys()))
            length = FOREIGN_CC[cc]
            # génère la partie abonnée de la bonne longueur
            subscriber = "".join(random.choices("0123456789", k=length))
            return cc + subscriber
        else:
            if random.random() > 0.5 :
                return None  # 5% chance of missing value
            else :
                chars = string.ascii_letters + string.digits  # a-z, A-Z, 0-9
                return ''.join(random.choices(chars, k=12))  
             
    record_types = ['voice', 'sms', 'data']
    weights = [0.6, 0.1, 0.3]  
    cell_ids = [
    "RABAT_01", "RABAT_02", "RABAT_03",
    "CASABLANCA_01", "CASABLANCA_02", "CASABLANCA_03", "CASABLANCA_04",
    "MARRAKECH_01", "MARRAKECH_02",
    "TAZA_01",
    "FES_01", "FES_02",
    "TANGER_01", "TANGER_02", "TANGER_03",
    "AGADIR_01", "AGADIR_02",
    "OUJDA_01",
    "MEKNES_01", "MEKNES_02",
    "KENITRA_01",
    "NADOR_01",
    "ALHOCEIMA_01", "ALHOCEIMA_02",
    "SAFI_01",
    "ELJADIDA_01",
    "KHOURIBGA_01",
    "TETOUAN_01",
    "LAAYOUNE_01",
    "DAKHLA_01"
]
    data_products=["DATA_BASIC","DATA_PREMIUM"]
    # Choisir un seul élément
    record_type = random.choices(record_types, weights=weights, k=1)[0]
    ts = current_ts.isoformat()
    base = {
        "record_ID" : str(uuid.uuid4()),
        "record_type": record_type,
        "timestamp": ts if random.random() > 0.1 else fake.date_time_between(start_date=datetime(2010, 1, 1), end_date="now").isoformat() if random.random() > 0.5 else None,  # 2% de chance d'être null
        "cell_id":  random.choice(cell_ids) if random.random() > 0.3 else None,  # 30% de chance d'être null
        "technology": random.choices(["2G", "3G", "4G", "5G","LTE","NR","UMTS"],weights=[0.2,0.3,0.6,0.3,0.1,0.08,0.02],k=1)[0] if random.random() > 0.1 else None,  # 10% de chance d'être null
    }

    if record_type == "voice":
        # Pour les appels vocaux, ajouter des champs spécifiques avec possibilité de nulls
        base.update({
            "caller_id": maroc_msisdn(),
            "callee_id": random.choices([maroc_msisdn(),generate_foreign_msisdn()],weights=[0.8,0.2],k=1)[0],
            "duration_sec": random.randint(10, 600) if random.random() > 0.2 else random.randint(-600, -10)  if random.random() > 0.5 else None   # 20% de chance d'être null
        })
    elif record_type == "sms":
        # Pour les SMS, ajouter des champs spécifiques avec possibilité de nulls
        base.update({
            "sender_id": maroc_msisdn(),  
            "receiver_id": random.choices([maroc_msisdn(),generate_foreign_msisdn()],weights=[0.8,0.2],k=1)[0],  
        })
    elif record_type == "data":
        # Pour les données, ajouter des champs spécifiques avec possibilité de nulls
        base.update({
            "user_id": maroc_msisdn(),
            "data_volume_mb": round(random.uniform(0.1, 2000), 2) if random.random() > 0.1 else None,  # 10% de chance d'être null
            "session_duration_sec": random.randint(60, 600) if random.random() > 0.2 else random.randint(-600, -10)  if random.random() > 0.5 else None , # 20% de chance d'être null
            "product_code":random.choices(data_products,weights=[0.8,0.2],k=1)[0],
        })
    # --- Mise à jour du timestamp global pour l'appel suivant ---
    # On avance de 5 à 120 secondes aléatoirement
    step = random.randint(5, 120)
    current_ts += timedelta(seconds=step)
    return base