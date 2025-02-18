use std::time::Duration;

use refield::fetch::FetchDocument;
use reqwest::{Client, StatusCode};
use serde_json::Value;
use tokio::time::sleep;

/// Update a document in CouchDB
async fn update_document(
    client: &Client,
    db_host: &str,
    table_name: &str,
    doc: &Value,
) -> Result<(), String> {
    let id = doc["_id"].as_str().ok_or("Document missing '_id' field")?;
    let rev = doc["_rev"]
        .as_str()
        .ok_or("Document missing '_rev' field")?;

    let idencoded = urlencoding::encode(id);
    let url = format!("{}/{}/{}", db_host, table_name, idencoded);

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
    let db_host = args.db_url.clone();
    let table_name = args.table_name.clone();
    let old_field = args.old_field.clone();
    let new_field = args.new_field.clone();
    let dry_run = args.dry_run;
    let limit = args.limit;

    // Initialize HTTP client
    let client = Client::new();

    println!(
        "Starting field rename operation: '{}' -> '{}' in table '{}'",
        old_field, new_field, table_name
    );

    if dry_run {
        println!("Dry-run mode enabled. No changes will be made to the database.");
    } else {
        println!("Dry-run mode disabled. Changes will be applied to the database.");
    }

    // Split the old field path into components (e.g., "a.b.c" -> ["a", "b", "c"])
    let old_field_path: Vec<String> = old_field.split('.').map(|s| s.to_string()).collect();

    let fd = FetchDocument::new(client.clone(), db_host.clone(), table_name.clone(), limit);

    fd.with_callback(Box::new(move |mut doc: Value| {
        let client = client.clone();
        let db_url = db_host.clone();
        let table_name = args.table_name.clone();
        let old_field_path = old_field_path.clone();
        let new_field = args.new_field.clone();
        let dry_run = args.dry_run;
        let old_field = old_field.clone();

        tokio::spawn(async move {
            let id = doc["_id"].as_str().unwrap_or("<unknown>");
            let idclone = id.to_string();

            let old_field_path: &[&str] = &old_field_path
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>();

            let renamed =
                refield::rename::rename_nested_field(&mut doc, old_field_path, &new_field);

            if renamed {
                if !dry_run {
                    // Update the document in CouchDB
                    if let Err(err) = update_document(&client, &db_url, &table_name, &doc).await {
                        eprintln!("\tError updating document {}: {}", idclone, err);
                    } else {
                        println!("\tupdated document ID: {}", idclone);
                    }
                    sleep(Duration::from_millis(200)).await;
                } else {
                    println!(
                        "\tDry-run: Document ID {} would have been updated.",
                        idclone
                    );
                }
            } else {
                println!(
                    "\tfield '{}' not found in document ID: {}",
                    old_field, idclone
                );
            }
        });
    }))
    .execute()
    .await;

    println!("Operation completed.");
}
