#!/usr/bin/env python3
import os
import subprocess
import sys
import time
import shutil
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("smoke_test")

def main():
    logger.info("🔍 Lancement du Smoke Test pour verity-bigquery...")

    # 1. Vérification des variables d'environnement
    env_vars = ["GOOGLE_CLOUD_PROJECT", "VERITY_DATASET", "GOOGLE_APPLICATION_CREDENTIALS"]
    missing = [var for var in env_vars if not os.environ.get(var)]

    if missing:
        logger.warning(f"Il manque des variables d'environnement cruciales : {', '.join(missing)}")
        logger.warning("Assure-toi de les définir avant de lancer le test. Exemple :")
        logger.warning("export GOOGLE_CLOUD_PROJECT='ton-projet-gcp'")
        logger.warning("export VERITY_DATASET='ton_dataset'")
        logger.warning("export GOOGLE_APPLICATION_CREDENTIALS='/chemin/vers/ta/cle.json'")
        logger.info("On tente quand même de lancer le binaire pour voir les logs d'erreur...")

    # 2. Détermination du chemin du binaire
    binary_path = shutil.which("verity-bigquery")
    if not binary_path:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cargo_target = os.path.join(script_dir, "..", "target", "debug", "verity-bigquery")
        if os.path.exists(cargo_target):
            binary_path = cargo_target

    if not binary_path:
        logger.error("Impossible de trouver le binaire 'verity-bigquery'.")
        logger.error("Assure-toi de lancer la compilation d'abord avec : cd verity-bigquery && cargo build")
        sys.exit(1)

    logger.info(f"🚀 Lancement de {binary_path}...")

    try:
        process = subprocess.Popen(
            [binary_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        
        time.sleep(2)
        process.stdin.close()
        
        out, err = process.communicate(timeout=3)
        
        logger.info("\n--- 📜 LOGS DU CONNECTEUR (STDERR) ---\n" + err + "\n--------------------------------------")
        
        if "BigQuery client initialized successfully" in err:
            logger.info("✅ SUCCÈS ! Le binaire démarre et s'authentifie bien avec BigQuery.")
            logger.info("=> Si ça plante dans ton autre repo, c'est que la requête SQL ou le payload JSON-RPC envoyé par le moteur Core de Verity est mal formaté.")
        elif "No auth configured" in err or "GCP Auth Error" in err or "missing" in err:
            logger.error("❌ ERREUR D'AUTHENTIFICATION/ENVIRONNEMENT !")
            logger.error("=> Le problème vient bien du fait que le connecteur ne reçoit pas les bons credentials au démarrage.")
            logger.error("=> Vérifie que ton pipeline GitHub Actions ou Cloud Build injecte bien le bon Service Account et les bonnes variables.")
        else:
            logger.warning("❓ Statut indéterminé, lis attentivement les logs ci-dessus.")

    except Exception as e:
        logger.exception(f"Erreur inattendue : {e}")

if __name__ == "__main__":
    main()
