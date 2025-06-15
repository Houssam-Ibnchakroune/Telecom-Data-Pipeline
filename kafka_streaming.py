from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
from Synthetic_Data_Generation import generate_cdr
fake = Faker()
conf = {
    'bootstrap.servers': 'localhost:9092'  
}

producer = Producer(**conf)





# Fonction callback pour les erreurs
def delivery_report(err, msg):
    if err is not None:
        print('Erreur d\'envoi: {}'.format(err))
    else:
        print(f"Message envoyé à {msg.topic()} [{msg.partition()}]")

# Boucle d'envoi
topic = 'cdr_topic'

try:
    while True:
        # Envoyer 5 messages à chaque itération
        for _ in range(5):
            cdr = generate_cdr()
            producer.produce(
                topic,
                key=cdr["record_type"],
                value=json.dumps(cdr),
                callback=delivery_report
            )
            producer.poll(0)  # Nécessaire pour traiter les callbacks
        time.sleep(2.57)  # Attente de 5 secondes après avoir envoyé les 5 messages
except KeyboardInterrupt:
    print("Arrêt du producteur")
finally:
    # Vider la file d'attente de messages avant de quitter
    producer.flush()