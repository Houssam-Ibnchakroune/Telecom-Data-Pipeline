import os
import random
from datetime import date
from typing import List

import psycopg2
from psycopg2.extensions import connection as _connection
from faker import Faker

# --- Données statiques modifiées et réordonnées ---
FIRST_NAMES_MALE: List[str] = [
    "Yassine", "Rayan", "Issam", "Mounir", "Anwar", "Brahim", "Ilyas", "Soufiane",
    "Taha", "Yanis", "Yahya", "Abdelaziz", "Aymen", "Noureddine", "Mourad", "Younes",
    "Redouane", "Khalil", "Selim", "Samy", "Abdelkarim", "Zouhair", "Ehab", "Nacer",
    "Azeddine", "Emad", "Farouk", "Rabah", "Fouad", "Jalal"
]

FIRST_NAMES_FEMALE: List[str] = [
    "Ines", "Sara", "Lamiae", "Wissal", "Nisrine", "Marwa", "Houda", "Salima",
    "Hind", "Naoual", "Ilham", "Ikram", "Mouna", "Rania", "Basma", "Dounia",
    "Amal", "Nezha", "Nada", "Ghita", "Yousra", "Mina", "Lyâa", "Soumaya",
    "Chaymae", "Selma", "Wafae", "Zakia", "Oumaima", "Fouzia"
]

LAST_NAMES: List[str] = [
    "El Idrissi", "Benomar", "Bennis", "El Malki", "Bouhjar", "Boukili", "Azmi", "El Amrani",
    "Ouchrif", "Belkadi", "Zitoune", "Kettani", "Bouazza", "El Othmani", "Benjida", "Rhazi",
    "Chouki", "Ouahbi", "Benmoussa", "El Hilali", "Belhaj", "Zair", "Benabdellah", "Laaroussi",
    "Msaddi", "Guerfi", "Skalli", "Bouziane", "Rifi", "El Ghazi"
]

RATE_PLANS: List[str] = ["PLAN_A", "PLAN_B", "PLAN_C"]

# --- Régions administratives du Maroc ---
REGIONS: List[str] = [
    "Tanger-Tétouan-Al Hoceima",
    "L'Oriental",
    "Fès-Meknès",
    "Rabat-Salé-Kénitra",
    "Béni Mellal-Khénifra",
    "Casablanca-Settat",
    "Marrakech-Safi",
    "Drâa-Tafilalet",
    "Souss-Massa",
    "Guelmim-Oued Noun",
    "Laâyoune-Sakia El Hamra",
    "Dakhla-Oued Ed Dahab"
]


# --- Générateurs de données ---
def generate_moroccan_name() -> str:
    """Retourne un prénom marocain (H/F) suivi d’un nom de famille."""
    gender = random.choice(['male', 'female'])
    first_list = FIRST_NAMES_MALE if gender == 'male' else FIRST_NAMES_FEMALE
    return f"{random.choice(first_list)} {random.choice(LAST_NAMES)}"


def generate_maroc_msisdn() -> str:
    """Retourne un MSISDN marocain au format 2126xxxxxxxx ou 2127xxxxxxxx."""
    prefix = random.choice(["2126", "2127"])
    suffix = ''.join(str(random.randint(0, 9)) for _ in range(8))
    return prefix + suffix


# --- Base de données ---
def get_db_connection() -> _connection:
    """Lit les variables d’environnement et renvoie une connexion psycopg2."""
    return psycopg2.connect(
        host     = os.getenv("PG_HOST", "localhost"),
        port     = int(os.getenv("PG_PORT", 5432)),
        dbname   = os.getenv("PG_DBNAME", "projet_spark"),
        user     = os.getenv("PG_USER", "postgres"),
        password = os.getenv("PG_PASSWORD", "0000")
    )


def create_customers_table(conn: _connection) -> None:
    """Crée la table customers si elle n’existe pas déjà."""
    ddl = """
    CREATE TABLE IF NOT EXISTS customers (
      customer_id        VARCHAR(20) PRIMARY KEY,
      customer_name      VARCHAR(100) NOT NULL,
      subscription_type  VARCHAR(10)  NOT NULL,
      rate_plan_id       VARCHAR(20)  NOT NULL,
      activation_date    DATE         NOT NULL,
      status             VARCHAR(10)  NOT NULL,
      region             VARCHAR(50)  NOT NULL
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def insert_customers(conn: _connection, n: int = 1000) -> None:
    """
    Génère et insère n clients fictifs dans la table `customers`.
    Les doublons sur customer_id sont ignorés.
    """
    fake = Faker()
    insert_sql = """
    INSERT INTO customers
      (customer_id, customer_name, subscription_type, rate_plan_id,
       activation_date, status, region)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        for _ in range(n):
            customer_id       = generate_maroc_msisdn()
            customer_name     = generate_moroccan_name()
            subscription_type = "postpaid"
            rate_plan_id      = random.choice(RATE_PLANS)
            activation_date   = fake.date_between(start_date="-2y", end_date=date.today())
            status            = "active"
            region            = random.choice(REGIONS)

            cur.execute(insert_sql, (
                customer_id,
                customer_name,
                subscription_type,
                rate_plan_id,
                activation_date,
                status,
                region
            ))
    conn.commit()


# --- Point d’entrée ---
def main():
    N = 1000  # Nombre de clients à générer

    conn = get_db_connection()
    try:
        create_customers_table(conn)
        insert_customers(conn, n=N)
        print(f"[✓] Insertion de {N} clients terminée.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
