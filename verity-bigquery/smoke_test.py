#!/usr/bin/env python3
import os
import subprocess
import sys
import shutil
import logging
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("smoke_test")

def main():
    logger.info("🔍 Lancement du Smoke Test pour verity-bigquery...")

    # 1. Vérification Env
    env_vars = ["GOOGLE_CLOUD_PROJECT", "VERITY_DATASET"]
    missing = [var for var in env_vars if not os.environ.get(var)]
    if missing:
        logger.error(f"❌ Variables manquantes : {', '.join(missing)}")
        sys.exit(1)

    # 2. Localisation du binaire
    binary_path = shutil.which("verity-bigquery") or os.path.abspath(os.path.join(__file__, "../../target/debug/verity-bigquery"))
    
    if not os.path.exists(binary_path):
        logger.error("❌ Binaire introuvable. Fais 'cargo build' d'abord.")
        sys.exit(1)

    # 3. Le payload pour forcer le connecteur à travailler (Exemple de format Verity JSON-RPC)
    rpc_payload = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "execute",
        "params": {
            "query": "SELECT 1 as test"
        }
    }) + "\n"

    logger.info(f"🚀 Lancement de {binary_path}...")

    try:
        process = subprocess.Popen(
            [binary_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        
        # Envoie la commande au binaire et attend la réponse (plus de time.sleep sauvage)
        out, err = process.communicate(input=rpc_payload, timeout=10)
        
        logger.info("\n--- 📜 LOGS DU CONNECTEUR (STDERR) ---\n" + err + "--------------------------------------")
        
        # Vérification 1 : L'initialisation
        if "BigQuery client initialized successfully" in err:
            logger.info("✅ Auth GCP OK.")
        else:
            logger.error("❌ Echec de l'authentification GCP.")
            return

        # Vérification 2 : L'exécution
        if "✅ Query executed successfully" in err or "terminé" in err:
            logger.info("✅ SUCCÈS TOTAL ! Le connecteur a exécuté la requête SQL.")
        elif "BIGQUERY CONNECTOR CRASHED" in err:
            logger.error("❌ Le connecteur a planté pendant l'exécution de la requête.")
        
        # Ce que Verity (Core) recevra vraiment :
        logger.info(f"\n--- 📦 REPONSE POUR VERITY (STDOUT) ---\n{out}")

    except subprocess.TimeoutExpired:
        process.kill()
        logger.error("❌ Timeout ! Le connecteur est resté bloqué.")
    except Exception as e:
        logger.exception(f"Erreur inattendue : {e}")

if __name__ == "__main__":
    main()