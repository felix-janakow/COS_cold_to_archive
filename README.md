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

Diese werden in einer .env Datei gespeichert und lokal "neben" das Skript abgelegt

### Finden der geforderten Eingabedaten

Da es nicht unbedingt direkt klar ist, wo die Daten aufzufinden sind, wird im folgenden Beschrieben wie man an die einzelnen Variablen ran kommt: 

### SOURCE_BUCKET und DESTINATION_BUCKET

-> Die beiden Eingaben werden zusammen betrachtet, da es sich bei uns um einen "INPLACE COPY" handelt. Quelle und Ziel sind also derselbe Bucket. 

![Image](https://github.com/user-attachments/assets/08db8f19-edbe-4cec-98a7-ae8a93676fb1)