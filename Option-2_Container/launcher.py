import os
import subprocess
from dotenv import load_dotenv

# Pfad zur .env-Datei
ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), ".env")

def collect_user_input():
    """Fragt den Benutzer nach Eingaben und speichert sie in der .env-Datei."""
    print("Bitte geben Sie die folgenden Details ein:")
    source_bucket = input("SOURCE_BUCKET: ")
    destination_bucket = input("DESTINATION_BUCKET: ")
    iam_api_key = input("IAM_API_KEY: ")
    # account_id = input("ACCOUNT_ID: ")
    region = input("REGION: ")

    # Speichert die Eingaben in der .env-Datei
    with open(ENV_FILE_PATH, "w") as env_file:
        env_file.write(f"SOURCE_BUCKET={source_bucket}\n")
        env_file.write(f"DESTINATION_BUCKET={destination_bucket}\n")
        env_file.write(f"IAM_API_KEY={iam_api_key}\n")
        # env_file.write(f"ACCOUNT_ID={account_id}\n")
        env_file.write(f"REGION={region}\n")

    print("\nKonfiguration wurde in der .env-Datei gespeichert.")

def execute_file_copy():
    # Führt die file_copy.py aus
    script_path = os.path.join(os.path.dirname(__file__), "file_copy.py")
    subprocess.run(["python3", script_path])

def main():
    # Prüfen, ob die .env-Datei existiert
    if os.path.exists(ENV_FILE_PATH):
        print("Eine .env-Datei wurde gefunden.")
        choice = input("Möchten Sie neue Eingaben machen? (y/N): ").strip().lower()
        if choice == "y":
            collect_user_input()
        else:
            print("Vorhandene Werte aus der .env-Datei werden verwendet.")
    else:
        print("Keine .env-Datei gefunden. Neue Eingaben erforderlich.")
        collect_user_input()

    # Lädt die .env-Datei
    load_dotenv(ENV_FILE_PATH)

    # Führt file_copy.py aus
    execute_file_copy()

if __name__ == "__main__":
    main()