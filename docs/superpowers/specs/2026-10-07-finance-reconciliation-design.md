# Reportbasierter Steuerreport mit unabhängigem Finanzabgleich

Original-Steuerreports und bestätigte Rechnungen/Gutschriften bestimmen die
Steuerberechnung. Finance-Abweichungen, Quellenausfälle und fehlende Finance-
Abdeckung sind Hinweise, auch wenn die optionale Vorsteuer-Vollständigkeits-
Sperre aktiviert ist. Unauflösbare Konflikte in Steuerquellen bleiben Blocker.

Ein eigener read-only Service gleicht Amazon-Gebühren nach nativer Währung,
Kategorie und ursprünglichem Transaktionsmonat ab. ModernTransaction-
Lifecycles werden einmal gezählt; Originaldatum und Freigabedatum getrennt
ausgegeben. Moderne Daten und Settlement-Zeilen werden nicht addiert.
Fehlende Original-Lifecycle-Daten und unbekannte Kategorien machen den
Abgleich unvollständig, nicht die Steuerberechnung unbrauchbar.

Beträge aus Item-Breakdowns verwenden Elternbeträge einmal; Base/Tax/Promo
sind Aufschlüsselungen, keine weiteren Gebühren. Kundenrabatte und Ads werden
separat ausgewiesen; GBP wird nativ verglichen, keine geschätzte EUR-Konversion.
Bestellbezogene positive Rechnungspositionen werden zusätzlich je Bestellung
und Kategorie verglichen; Gegenabweichungen dürfen nicht verschwinden.
Rechnungs-Gesamtsummen bleiben maßgeblich, Positionsrundungen werden offengelegt.

API-Resultat `sections.finance_reconciliation`: Status, Gebührenabgleich je
Währung/Kategorie, Datumsüberleitung, Detailabweichungen, ausgeschlossene Ads,
unbekannte Gebühren und Quellprobleme. Zustände `matched`, `explained`,
`differences`, `incomplete`. Frontend zeigt getrennt Steuerberechnung und
Finanzabgleich; Unterschiede ändern keine Steuerbeträge/Freigaben/Buchungen.

Verifikation: reale isolierte SQLite-Tests für Lifecycle, Doppelquellen,
gleichbetragige echte Transaktionen, unterschiedliche Gebührenarten/Währungen,
Teilrefunds, unbekannte Typen, fehlende Quellen, Gegenabweichungen und
Report-Abgabe trotz Finance-Warnung. Read-only Laufzeitvergleich Juli–September
mit null ungeklärtem Rest; Steuerbeträge und Buchungen müssen unverändert sein.
