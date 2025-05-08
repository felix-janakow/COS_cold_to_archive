# Tutorial - Python Skript Cold Vault -> Archive Tier

Dieses Skript dient dazu, Daten aus einem COS-Bucket im Cold Vault-Tier in das Archiv-Tier zu verschieben. Dies geschieht durch eine REPLACE-Operation auf die Metadaten der Objekte im Bucket. Diese Operation löst eine Änderung aus, die von COS erkannt wird und den Archivierungsprozess startet.

> [!NOTE] 
> Die REPLACE-Operation betrifft ausschließlich die Metadaten – der Inhalt der Datei selbst bleibt unverändert.
>
> Je nach gewähltem Archivtyp kann die Wiederherstellung archivierter Daten bis zu 2 Stunden (bei Instant Retrieval) oder bis zu 12 Stunden (bei Cold Archive) dauern.       

## Vorbereitung

Bei Ausführung des Skripts wird nach folgenden Eingabedaten gefragt: 

    - SOURCE_BUCKET
    - DESTINATION_BUCKET
    - IAM_API_KEY
    - ACCOUNT_ID
    - REGION


