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

Da es nicht unbedingt direkt klar ist, wo die Daten aufzufinden sind, wird im folgenden beschrieben wie man an die einzelnen Variablen ran kommt: 


### - SOURCE_BUCKET und - DESTINATION_BUCKET

-> Die beiden Eingaben werden zusammen betrachtet, da es sich bei uns um einen "INPLACE COPY" handelt. Quelle und Ziel sind also derselbe Bucket. 

- Klicken Sie auf ``Ressourcenliste``am linken Rand und suchen Sie nach Ihrer COS Instanz, welche die Buckets enthält, die Sie archivieren möchten   

![Image](https://github.com/user-attachments/assets/67925d35-80cf-4681-8993-859d11f41618)

- Kopieren Sie den Namen des Buckets und bewahren Sie diesen in irgendeiner Form abrufbar als Notiz ab 

![Image](https://github.com/user-attachments/assets/3f4af865-8bfe-4034-8412-e90b5e805e1e)


### - IAM_API_KEY

Erstellen Sie einen IAM Schlüssel indem Sie:
-  Über die obere Navigationsleiste auf ``Manage`` klicken und ``Access(IAM)`` auswählen
-  ``API-keys`` auf der linken Seiteleiste auswählen
-  ``Create +`` klicken  
-  Vergeben Sie irgendeinen Namen
- Leaked Action können Sie ignorieren
- Für Session Creation wählen Sie ``Yes``
- Speichern Sie sich den Key in irgendeiner Form abrufbar als Notiz ab

![Image](https://github.com/user-attachments/assets/f15b760d-fc69-46b8-80ab-9d486eddb301)

### - Region

Die Region ist in unserem Falle der Wert ``eu-de``