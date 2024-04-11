provider "google" {
    project = "dw-lab1-dsa"
    region = "us-west1"
}

# Uploads transformed files to bucket
resource "google_storage_bucket_object" "data_folder" {
  for_each = toset(split("\n", file("../data/files.txt")))
  name   = "${trim(each.value, "./")}" 
  source  = ".${each.value}"            
  bucket = "bucket-dw-modeling-pedro"
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
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Crimes_La"

    schema = jsonencode([
        {
            "name": "id",
            "type": "INTEGER",
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
    ])
}

resource "google_bigquery_table" "Status" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Status"

    schema = jsonencode([
        {
            "name": "id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "description",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ])
}

resource "google_bigquery_table" "Crime_Date" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Crimes_Date"

    schema = jsonencode([
        {
            "name": "crime_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "date_rpt",
            "type": "DATE",
            "mode": "REQUIRED"
        },
        {
            "name": "datetime_occ",
            "type": "TIMESTAMP",
            "mode": "REQUIRED"
        }
    ])
}


resource "google_bigquery_table" "Victims" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Victims"

    schema = jsonencode([
        {
            "name": "crime_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "age",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "sex",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "descent_id",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ])
}

resource "google_bigquery_table" "Victim_Descent" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Victim_Descent"

    schema = jsonencode([
        {
            "name": "id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "description",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ])
}

resource "google_bigquery_table" "Mocodes_Crimes" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Mocodes_Crimes"

    schema = jsonencode([
        {
            "name": "crime_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "Mocode1",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode2",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode3",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode4",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode5",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode6",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode7",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode8",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode9",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Mocode10",
            "type": "INTEGER",
            "mode": "NULLABLE"
        }
    ])
}

resource "google_bigquery_table" "Mocodes" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Mocodes"

    schema = jsonencode([
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "description",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ])
}

resource "google_bigquery_table" "Locations" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Location"

    schema = jsonencode([
        {
            "name": "crime_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "area_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "location",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "cross_street",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "premis_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "lat",
            "type": "FLOAT64",
            "mode": "REQUIRED"
        },
        {
            "name": "lon",
            "type": "FLOAT64",
            "mode": "REQUIRED"
        },
    ])
}

resource "google_bigquery_table" "Areas" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Areas"

    schema = jsonencode([
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "description",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ])
}

resource "google_bigquery_table" "Premisses" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Premisses"

    schema = jsonencode([
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "description",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ])
}

resource "google_bigquery_table" "Crimes_List" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Crimes_List"

    schema = jsonencode([
        {
            "name": "crime_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "Crm_Cd",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "Crm_Cd_2",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Crm_Cd_3",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Crm_Cd_4",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
    ])
}

resource "google_bigquery_table" "Crimes" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Crimes"

    schema = jsonencode([
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "description",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ])
}

resource "google_bigquery_table" "Weapons" {
    deletion_protection = false
    dataset_id = google_bigquery_dataset.crimes_la.dataset_id
    table_id = "Weapons"

    schema = jsonencode([
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "description",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ])
}

locals {
    table_id = tomap({
        "crimes_la" = google_bigquery_table.crimes_la.table_id,
        "status" = google_bigquery_table.Status.table_id,
        "crimes_date" = google_bigquery_table.Crime_Date.table_id,
        "victims" = google_bigquery_table.Victims.table_id,
        "descent" = google_bigquery_table.Victim_Descent.table_id,
        "mocodes_crimes" = google_bigquery_table.Mocodes_Crimes.table_id,
        "mocodes_desc" = google_bigquery_table.Mocodes.table_id,
        "location" = google_bigquery_table.Locations.table_id,
        "areas" = google_bigquery_table.Areas.table_id,
        "premis" = google_bigquery_table.Premisses.table_id,
        "crimes_list" = google_bigquery_table.Crimes_List.table_id,
        "crimes" = google_bigquery_table.Crimes.table_id,
        "weapons" = google_bigquery_table.Weapons.table_id
    })
}

resource "random_string" "random_id" {
    length = 8
    special = false
    upper = false
}

# Loads data from bucket to BigQuery table
resource "google_bigquery_job" "job_sql_1" {
    for_each = toset(split("\n", file("../data/files.txt")))
    job_id = "load_job_${random_string.random_id.result}_${split("/", each.value)[3]}"
    # Waits the upload to finish
    depends_on = [google_storage_bucket_object.data_folder]

    labels = {
        "job" = "job_load"
    }

    load {
        source_uris = ["gs://bucket-dw-modeling-pedro/${trim(each.value, "./")}",]

        destination_table {
            project_id = "dw-lab1-dsa"
            dataset_id = google_bigquery_dataset.crimes_la.dataset_id
            table_id = lookup(local.table_id, split("/", each.value)[3])
        }

        skip_leading_rows = 1
        #schema_update_options = ["ALLOW_FIELD_RELAXATION", "ALLOW_FIELD_ADDITION"]
        write_disposition = "WRITE_APPEND"
    }
}