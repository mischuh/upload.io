# Basic Idea

* Resource
    ** Wizard -> ```Catalog``` (which data do get from our customer)
* Mapping (source -> target)
    ** TODO: Describe Source itself (Connection, URL, Transformation (Skip Header, Lines...))
    ** Receive new Data (```Source```), validate against Catalog
    ** Mapping Catalog to internal DataSchema(s)
    ** speichern
* DataSchema
    ** Avro Schema
    ** Mapping to DataTypes (if possible, otherwise String as default)
* Connector
    ** How to handle ```Source``` of our customers
    ** CSV, XLS, SAP, REST...
    ** Upload, FTP, Pull, Push
* Target
    ** Where to store ```Source``` to
    ** DB
    ** Stream
    ** File

// "encoding": "cp1252",

# Example

    python3 sandbox.py