provider "google" {
    project = "dw-lab1-dsa"
    region = "us-west1"
}

# Dataset
resource "google_bigquery_dataset" "crimes_la" {
    dataset_id = "crimes_la"
    friendly_name = "crimes_la"
    description = "Crimes_la dataset"
    location = "US"
}

# BigQuery Tables
resource "google_bigquery_table" "crimes_la" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.dsa_dataset.dataset_id
    table_id = "Crimes_La"

    schema = jsonencode([
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "Time_OCC",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "Rpt_Dist_No",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "Part_1_2",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "Weapon_used_id",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Status",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "Date_OCC",
            "type": "DATE",
            "mode": "REQUIRED"
        },
        {
            "name": "Date_Rpt",
            "type": "DATE",
            "mode": "REQUIRED"
        },
    ])
}