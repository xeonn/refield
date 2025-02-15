use reqwest::{Client, StatusCode};
use serde_json::{from_str, Value};

/// Check if the table is partitioned by querying its metadata
async fn is_partitioned(client: &Client, db_url: &str, table_name: &str) -> Result<bool, String> {
    let url = format!("{}/{}", db_url, table_name);
    let response = client.get(&url).send().await.map_err(|e| e.to_string())?;

    if response.status() != StatusCode::OK {
        return Err(format!(
            "Failed to fetch table metadata: Status code {}",
            response.status()
        ));
    }

    let body = response.text().await.map_err(|e| e.to_string())?;
    let json: Value = from_str(&body).map_err(|e| e.to_string())?;

    // Check if the "partitioned" field exists and is true
    Ok(json["partitioned"].as_bool().unwrap_or(false))
}

/// Update a document in CouchDB
async fn update_document(
    client: &Client,
    db_url: &str,
    table_name: &str,
    doc: &Value,
) -> Result<(), String> {
    let id = doc["_id"].as_str().ok_or("Document missing '_id' field")?;
    let rev = doc["_rev"]
        .as_str()
        .ok_or("Document missing '_rev' field")?;
    let url = format!("{}/{}/{}", db_url, table_name, id);

    let response = client
        .put(&url)
        .json(doc)
        .header("If-Match", rev) // Ensure we're updating the correct revision
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
        return Err(format!(
            "Failed to update document {}: Status code {}",
            id,
            response.status()
        ));
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    // Parse command-line arguments using `clap`
    let args = match refield::args::parse_args() {
        Ok(args) => args,
        Err(err) => {
            eprintln!("Error: {}", err);
            return;
        }
    };

    // Initialize HTTP client
    let client = Client::new();

    println!(
        "Starting field rename operation: '{}' -> '{}' in table '{}'",
        args.old_field, args.new_field, args.table_name
    );

    if args.dry_run {
        println!("Dry-run mode enabled. No changes will be made to the database.");
    } else {
        println!("Dry-run mode disabled. Changes will be applied to the database.");
    }

    // Check if the table is partitioned
    let is_partitioned = match is_partitioned(&client, &args.db_url, &args.table_name).await {
        Ok(partitioned) => partitioned,
        Err(err) => {
            eprintln!("Error checking if table is partitioned: {}", err);
            return;
        }
    };

    if is_partitioned {
        println!("Table '{}' is partitioned.", args.table_name);
    } else {
        println!("Table '{}' is not partitioned.", args.table_name);
    }

    // Split the old field path into components (e.g., "a.b.c" -> ["a", "b", "c"])
    let old_field_path: Vec<&str> = args.old_field.split('.').collect();

    // Fetch all documents from the specified table
    let documents =
        match refield::fetch::fetch_documents(&client, &args.db_url, &args.table_name, is_partitioned).await {
            Ok(docs) => docs,
            Err(err) => {
                eprintln!("Error fetching documents: {}", err);
                return;
            }
        };

    println!("Processing {} documents...", documents.len());

    // Process each document
    for mut doc in documents {
        let id = doc["_id"].as_str().unwrap_or("<unknown>");
        let idclone = id.to_string();
        let renamed = refield::rename::rename_nested_field(&mut doc, &old_field_path, &args.new_field);

        if renamed {
            println!("Field renamed in document ID: {}", idclone);

            if !args.dry_run {
                // Update the document in CouchDB
                if let Err(err) =
                    update_document(&client, &args.db_url, &args.table_name, &doc).await
                {
                    eprintln!("Error updating document {}: {}", idclone, err);
                } else {
                    println!("Updated document ID: {}", idclone);
                }
            } else {
                println!("Dry-run: Document ID {} would have been updated.", idclone);
            }
        } else {
            println!(
                "Field '{}' not found in document ID: {}",
                args.old_field, idclone
            );
        }
    }

    println!("Operation completed.");
}
