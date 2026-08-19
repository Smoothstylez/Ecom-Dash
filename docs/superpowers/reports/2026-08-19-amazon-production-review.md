# Amazon Integration Production-Review (2026-08-19)

Review-Scope: Commits `4ed5bc6` (Amazon FBA Basis), `dd2d341` (Kaufland Support-API),
`2a111ce` (Amazon Auto-Refresh). Ziel: sicheres Upgrade auf dem Home-Server (nur per
VPN erreichbar) bewerten.

Jeder Befund wurde nach der ersten Review-Runde noch einmal direkt am Code verifiziert.
Zwei ursprünglich gemeldete Punkte waren beim genaueren Hinsehen falsch/übertrieben und
sind unten als "korrigiert" markiert.

## Entscheidung des Betreibers (bereits geklärt)

- Secrets im Docker-Image (Punkt 1+2 der Erstfassung): **kein Fix nötig**. Home-Server,
  nur per VPN erreichbar, kein öffentliches Production-Deployment im klassischen Sinn.
- Nur ein Amazon-Marketplace (DE) im Einsatz: Punkt 7 (Modern-Finance nur primärer
  Marketplace) ist damit **aktuell irrelevant**. Erst relevant, falls ein zweiter
  Amazon-Marketplace hinzukommt.

## Muss gefixt werden

### A. Amazon-Datenbank fehlt in Backup/Restore
- **Wo:** `app/services/exports.py:208-214` (Backup-Erstellung), `:677-683`
  (Restore-Mapping), `:831-838` (Pre-Restore-Safety-Backup)
- **Befund:** Alle drei Stellen listen `combined`, `shopify`, `kaufland`,
  `bookkeeping`, `ebay` – aber nicht `AMAZON_FBA_DB_PATH`.
- **Konsequenz:** Ein "Vollbackup" sichert keine Amazon-Bestellungen, Finance-Daten,
  Rechnungen, FIFO-Lots. Bei einem Restore bleiben diese Daten komplett verloren.
- **Fix:** Amazon-DB an allen drei Stellen ergänzen.

### B. Inbound-Cost-Endpoint stürzt bei jedem Aufruf ab
- **Wo:** `app/routers/amazon.py:102-108,191-197`; `app/services/amazon_fba.py:368-378`
- **Befund:** `InboundCostRequest` enthält das Feld `notes`, aber
  `add_inbound_cost()` akzeptiert dieses Argument nicht.
- **Verifiziert:** Direkter Funktionsaufruf reproduziert
  `TypeError: add_inbound_cost() got an unexpected keyword argument 'notes'`.
- **Konsequenz:** `POST /api/amazon/inbound/shipments/{shipment_id}/costs` schlägt
  **immer** mit HTTP 500 fehl.
- **Fix:** `notes` entweder aus dem Request-Modell entfernen oder in
  `add_inbound_cost()`/der DB-Spalte ergänzen.

### C. Teilweise fehlgeschlagene Amazon-Syncs werden als "erfolgreich" gespeichert
- **Wo:** `app/services/importers/amazon_sp_api.py:2237-2244` (Status `partial` bei
  Fehlern ohne 429/503); `app/services/amazon_auto_refresh.py:189-202,245-259`
  (`run_amazon_task` wirft nur bei Throttle oder vollständigem `error`)
- **Befund:** Schlägt z. B. ein Inventory-Request mit einem gewöhnlichen HTTP-500
  fehl (kein 429/503), bleibt der Gesamtstatus `partial`. Der Scheduler behandelt
  das trotzdem als vollen Erfolg: `last_status` wird `success`, `last_error` wird
  gelöscht, Backoff wird zurückgesetzt.
- **Konsequenz:** Ein echter, wiederkehrender Fehler in einem Teilbereich bleibt im
  Status-Endpoint unsichtbar und wird nicht mit Backoff behandelt.
- **Empfohlenes Vorgehen:** Bei `status == "partial"` den Task **nicht** als
  `success` markieren, sondern als eigenen Status `partial` mit erhaltenem
  `last_error` speichern. Kein Backoff-Level hochzählen (kein Quota-Problem), aber
  auch keinen Erfolg vortäuschen. Der nächste Versuch läuft trotzdem im normalen
  Intervall (5/15/30 Min.), nur sichtbar mit Fehlerhinweis im Status.

## Sollte gefixt werden (Robustheit, kein akuter Vorfall)

### D. Scheduler-Lock kann bei DB-Fehler dauerhaft hängen bleiben
- **Wo:** `app/services/amazon_auto_refresh.py:219-225`
- **Befund:** `acquire_database_lease(owner_id)` wird aufgerufen, *bevor* der
  `try/finally`-Block beginnt. Wirft dieser Aufruf eine Ausnahme (z. B. SQLite
  `database is locked` durch einen parallelen Schreibzugriff), bleibt
  `_CYCLE_LOCK` für immer belegt.
- **Konsequenz:** Alle folgenden automatischen und manuellen Syncs würden bis zum
  nächsten Prozess-Neustart nur noch `already_running` zurückgeben – der
  Auto-Refresh würde also lautlos stehen bleiben.
- **Bisher nicht beobachtet**, aber realistisch bei gleichzeitigem Zugriff mehrerer
  Worker auf dieselbe SQLite-Datei.
- **Fix:** `acquire_database_lease()`-Aufruf ebenfalls in try/except absichern und
  bei Fehler `_CYCLE_LOCK` wieder freigeben.

## Kein Fix nötig (falsch gemeldet / im ersten Review übertrieben)

### E. "Settlement-Gebühren ohne Order werden jeder Order zugerechnet" — **korrigiert, kein Bug**
- **Ursprünglicher Befund:** Der SQL-Prädikat-Zweig
  `(e.amazon_order_id IS NULL AND e.event_type = 'SettlementReportLine')` in
  `_canonical_financial_event_predicate()` (`amazon_fba.py:22-26`) würde angeblich
  in jede Order-Summe einfließen.
- **Bei genauer Prüfung:** Dieser Zweig wird in `load_amazon_order_summaries()` und
  `get_amazon_order_detail()` immer zusammen mit
  `e.amazon_order_id = o.amazon_order_id` verwendet – und `amazon_order_id` ist
  `PRIMARY KEY` in `amazon_orders`, also nie `NULL`. In SQL ist
  `NULL = 'irgendein-wert'` nie wahr. Der "Null-Order"-Zweig kann in diesem
  Kontext also nie greifen – toter Code, kein tatsächlicher Bug.
- Der Zweig wird nur in `get_amazon_finance_overview()` (Gesamt-Finance-Übersicht,
  nicht pro Order) ohne Order-Korrelation verwendet – dort ist die Einbeziehung
  nicht zugeordneter Settlement-Gebühren beabsichtigt (Gesamtsumme aller
  Kontobewegungen).
- **Ergebnis:** Kein Fix notwendig.

### F. "Manueller Trigger kann Amazon-Quota beliebig oft umgehen" — **abgeschwächt**
- **Ursprünglicher Befund (hoch):** Jeder Trigger erzwingt Orders/Finance/Inventory
  unabhängig vom normalen Intervall.
- **Bei genauer Prüfung:**
  - `select_due_tasks(..., force=True)` prüft **zuerst**, ob ein Task aktuell im
    Backoff steckt (`last_status == "backoff" and next_eligible > now`) – ein
    Task, der gerade wegen 429 im Backoff ist, wird auch bei `force=True`
    **übersprungen**. Das haben wir live beobachtet: Trigger lief für
    Orders/Finance, Inventory blieb im Backoff.
  - `_CYCLE_LOCK` sorgt dafür, dass parallele Trigger niemals gleichzeitig laufen
    – ein zweiter Klick während eines laufenden Zyklus bringt sofort
    `already_running` zurück, ohne neue API-Calls.
- **Praktische Antwort auf die Frage "1–2× am Tag drücken, ist das schlimm?":**
  **Nein, unproblematisch.** Das entspricht im schlimmsten Fall einem vorgezogenen
  regulären Delta-Sync (kleines Zeitfenster, kein 730-Tage-Reimport). Amazons
  SP-API-Ratenlimits für die hier verwendeten Endpunkte (Orders, Finance,
  Inventory) sind kurzfristige Token-Buckets (Sekunden-/Minutenbereich), keine
  harten Tages-Kontingente – sie erholen sich unabhängig vom Wochentag
  kontinuierlich, sobald keine Anfragen mehr laufen.
  Kritisch würde es nur bei sehr schnellem, wiederholtem automatisiertem
  Antriggern (z. B. mehrfach pro Minute durch ein Skript) – das ist beim
  manuellen Klicken im Dashboard nicht der Fall.
- **Ergebnis:** Kein akuter Fix nötig für normale Nutzung. Optional als
  Komfort/Absicherung: ein kurzer serverseitiger Mindestabstand
  zwischen zwei manuellen Triggern (z. B. 60 Sekunden), rein zur Absicherung gegen
  versehentliches Doppelklicken – keine Pflicht.

## Bekannt, aber unkritisch – nur als Backlog (auf Wunsch später)

### G. Bestellliste kann bei unveränderter Order-Anzahl veraltete Felder zeigen
- **Wo:** `app/services/amazon_auto_refresh.py` (aktualisiert nur `changestamp`,
  ruft nie `populate_combined_orders`/`refresh_combined_order_row` auf);
  `app/services/orders.py:555-585` (`_combined_orders_ready` vergleicht nur
  Zeilenanzahl)
- **Wichtige Einschränkung, die im Erstreview fehlte:** Betroffen ist **nur die
  Bestellliste** (`GET /api/orders`, Spalten Status/Fees/Financial-Status). Die
  **Order-Detailseite liest immer live aus der Amazon-Quelldatenbank**
  (`get_amazon_order_detail()` in `app/services/orders.py:1104-1115`) und ist
  **nie veraltet**.
- **Konkretes Szenario:** Eine bestehende Order wechselt z. B. von `Pending` zu
  `Shipped`, oder eine Finance-Buchung trifft nachträglich ein – ohne dass sich
  die Gesamtzahl der Amazon-Orders ändert. Die Listenansicht zeigt dann weiterhin
  den alten Status/alte Zahlen, bis irgendwann eine neue Order hinzukommt (dann
  wird die komplette Amazon-Menge neu aufgebaut) oder ein manueller Vollsync
  läuft.
- **Fix-Richtung (falls gewünscht):** Nach jedem erfolgreichen Auto-Refresh-Task
  gezielt `refresh_combined_order_row()` für die geänderten Amazon-Order-IDs
  aufrufen (analog zum bestehenden Shopify/Kaufland-Live-Sync-Muster in
  `app/services/live_sync.py:233-236`).
- **Priorität:** Mittel – kein Datenverlust, nur eine sichtbare Verzögerung in
  der Listenansicht.

### H. Inventory-Snapshot kann bei großem Sync ältere Zeilen "verlieren"
- **Wo:** `app/services/importers/amazon_sp_api.py:1380-1399` (jede Zeile bekommt
  ihren eigenen `_utc_now()`-Zeitstempel), `app/services/amazon_fba.py:225,240`
  (Anzeige wählt nur `MAX(captured_at)`)
- **Szenario:** Läuft ein Inventory-Sync über mehrere Amazon-Seiten länger als eine
  Sekunde, können frühere Items einen älteren Sekunden-Zeitstempel bekommen und
  fallen aus der "aktuellsten" Anzeige heraus (die Zeilen bleiben aber in der
  Datenbank erhalten, nur die aktuelle Übersicht zeigt sie nicht).
- **Priorität:** Niedrig – nur relevant bei größeren/mehrseitigen Inventory-Syncs.

### I. Mehrere gleichartige FBA-Inbound-Kosten können sich gegenseitig überschreiben
- **Wo:** Schema `amazon_sp_api.py:581-592` (`UNIQUE(source_event_id, cost_type)`),
  Persistenz `:1645-1690`
- **Szenario:** Enthält eine Amazon-Transaktion zwei Positionen mit demselben
  Kosten-Typ (z. B. zwei `FBAInboundTransportationFee`-Komponenten), überschreibt
  die zweite die erste beim Speichern.
- **Priorität:** Niedrig – Randfall, betrifft nur mehrfach gleichartige
  Kostenkomponenten in einer einzigen Transaktion.

### J. Geplanter Orders-Task lädt nie Katalogbilder nach
- **Wo:** `app/services/amazon_auto_refresh.py` `TASKS["orders"]` und
  `TASKS["reconcile"]` setzen `include_catalog_images: False`
- **Szenario:** Neue Bestellpositionen ohne gespeichertes Bild bekommen ihr Bild
  nur nachgeladen, wenn ein **manueller** Sync mit Standard-Flags läuft.
- **Priorität:** Niedrig – rein kosmetisch (fehlendes Produktbild), kein
  Datenfehler.

### K. Direkter Aufruf von `/amazon` im Browser liefert vermutlich 404
- **Wo:** `app/main.py:260-269` – Liste der Dashboard-Alias-Routen enthält
  `/amazon` nicht.
- **Szenario:** Navigation über die Seitenleiste funktioniert (Client-Side
  Routing), aber ein Lesezeichen/Browser-Reload auf `/amazon` direkt schlägt
  vermutlich fehl.
- **Priorität:** Niedrig – reine Komfortfrage, einfache Ein-Zeilen-Ergänzung.

## Zusammenfassung: was ist jetzt wirklich zu tun?

| # | Thema | Einschätzung | Empfehlung |
|---|-------|--------------|------------|
| A | Amazon-DB fehlt in Backup/Restore | Bestätigt, wichtig | **Fixen** |
| B | Inbound-Cost-Endpoint crasht immer | Bestätigt, reproduziert | **Fixen** |
| C | Partial-Sync als Erfolg gespeichert | Bestätigt | **Fixen** |
| D | Scheduler-Lock kann hängen bleiben | Strukturell bestätigt, noch nicht aufgetreten | Fixen (Robustheit) |
| E | Settlement-Gebühren in jeder Order | **Kein Bug** (verifiziert) | Kein Fix |
| F | Trigger umgeht Quota | Abgeschwächt, mit Backoff-Check bereits sicher | Kein Fix nötig, optional Cooldown |
| G | Bestellliste zeigt alte Felder | Bestätigt, aber nur Listenansicht betroffen | Backlog / auf Wunsch |
| H | Inventory-Snapshot verliert Zeilen | Bestätigt, Randfall | Backlog |
| I | FBA-Kosten überschreiben sich | Bestätigt, Randfall | Backlog |
| J | Keine Bilder bei Scheduler-Orders | Bestätigt, kosmetisch | Backlog |
| K | `/amazon` Direktaufruf 404 | Bestätigt, kosmetisch | Backlog |
