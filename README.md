# FHI Importer

Reads government XML data and inserts it into a `mysql` database.

## Set-up

1. Copy `Funds XX-XXX-XXXX.xml` into directory. Rename to `funds.xml`.
2. Copy `Hospital XX-XXX-XXXX.xml` into directory. Rename to `hospital.xml`.
3. Copy `GeneralHealth XX-XXX-XXXX.xml` into directory. Rename to `extras.xml`.
4. Copy `Combined Open XX-XXX-XXXX.xml` into directory. Rename to `combined.xml`.
5. Create a database `scratch` in your local `mysql` server.