use refield::fetch::FetchDocument;
use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;


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

    // Extract arguments for convenience
    let db_host = args.db_url.clone();
    let table_name = args.table_name.clone();
    let old_field = args.old_field.clone();
    let new_field = args.new_field.clone();
    let dry_run = args.dry_run;
    let limit = args.limit;

    // Initialize an HTTP client for making requests
    let client = Client::new();

    // Print the operation details
    println!(
        "Starting field rename operation: '{}' -> '{}' in table '{}'",
        old_field, new_field, table_name
    );

    // Inform the user about the dry-run mode
    if dry_run {
        println!("Dry-run mode enabled. No changes will be made to the database.");
    } else {
        println!("Dry-run mode disabled. Changes will be applied to the database.");
    }

    // Split the old field path into components (e.g., "a.b.c" -> ["a", "b", "c"])
    let old_field_path: Vec<String> = old_field.split('.').map(|s| s.to_string()).collect();

    // Create a FetchDocument instance to fetch documents from the database
    let fd = FetchDocument::new(client.clone(), db_host.clone(), table_name.clone(), limit);

    // Define a callback to process each fetched document
    fd.with_callback(Box::new(move |doc: Value| {
        // Clone necessary variables to ensure they live long enough in the closure
        let client = client.clone();
        let db_url = db_host.clone();
        let table_name = table_name.clone();
        let old_field_path = old_field_path.clone();
        let new_field = new_field.clone();
        let dry_run = dry_run;

        // Spawn a new asynchronous task to process the document
        tokio::spawn(async move {
            process_document(
                client,
                db_url,
                table_name,
                old_field_path,
                new_field,
                dry_run,
                doc,
            )
            .await;
        });
    }))
    .execute()
    .await;

    // Indicate that the operation is complete
    println!("Operation completed.");
}

/// Used as a callback to process a single document fetched from the database.
async fn process_document(
    client: Client,
    db_url: String,
    table_name: String,
    old_field_path: Vec<String>,
    new_field: String,
    dry_run: bool,
    mut doc: Value,
) {
    let id = doc["_id"].as_str().unwrap_or("<unknown>");
    let idclone = id.to_string();

    // Convert the old field path into a slice of string slices for processing
    let old_field_path: &[&str] = &old_field_path
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();

    // Attempt to rename the nested field in the document
    let renamed = refield::rename::rename_nested_field(&mut doc, old_field_path, &new_field);

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
            // Dry-run mode: Log what would have been updated
            println!(
                "\tDry-run: Document ID {} would have been updated.",
                idclone
            );
        }
    } else {
        // Field not found in the document
        println!(
            "\tfield '{}' not found in document ID: {}",
            old_field_path.join("."),
            idclone
        );
    }
}

/// Persists changes to a document in CouchDB when the dry-run mode is disabled.
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
        .header("If-Match", rev)
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
