use clap::{Arg, Command};
use reqwest::{Client, StatusCode};
use serde_json::{from_str, Value};

/// Struct to represent command-line arguments
struct Args {
    db_url: String,       // URL of the CouchDB database
    table_name: String,   // Name of the table (or document type)
    old_field: String,    // Old field name to be renamed (supports dot notation for nested fields)
    new_field: String,    // New field name to replace the old one
    dry_run: bool,        // Whether to perform a dry run (preview changes without modifying the database)
}

/// Parse command-line arguments using `clap`
fn parse_args() -> Result<Args, String> {
    let matches = Command::new("CouchDB Field Renamer")
        .version("1.0")
        .author("Onn Khairuddin Ismail")
        .about("Renames a field in documents within a CouchDB database")
        .arg(
            Arg::new("db_url")
                .short('u')
                .long("url")
                .value_name("URL")
                .help("URL of the CouchDB database")
                .required(true),
        )
        .arg(
            Arg::new("table_name")
                .short('t')
                .long("table")
                .value_name("TABLE")
                .help("Name of the table (or document type)")
                .required(true),
        )
        .arg(
            Arg::new("old_field")
                .short('o')
                .long("old")
                .value_name("OLD_FIELD")
                .help("Old field name to be renamed (supports dot notation for nested fields)")
                .required(true),
        )
        .arg(
            Arg::new("new_field")
                .short('n')
                .long("new")
                .value_name("NEW_FIELD")
                .help("New field name to replace the old one")
                .required(true),
        )
        .arg(
            Arg::new("dry_run")
                .long("dry-run") // Use --dry-run to enable dry-run mode
                .help("Enable dry-run mode (preview changes without modifying the database)")
                .action(clap::ArgAction::SetTrue) // Defaults to false unless --dry-run is provided
                .default_value("false"), // Default value is false (not dry-run)
        )
        .get_matches();

    // Extract arguments from matches
    let db_url = matches.get_one::<String>("db_url").unwrap().clone();
    let table_name = matches.get_one::<String>("table_name").unwrap().clone();
    let old_field = matches.get_one::<String>("old_field").unwrap().clone();
    let new_field = matches.get_one::<String>("new_field").unwrap().clone();
    let dry_run = *matches.get_one::<bool>("dry_run").unwrap_or(&false);

    // Validate that the paths (excluding the last key) are identical
    let old_path: Vec<&str> = old_field.split('.').collect();
    let new_path: Vec<&str> = new_field.split('.').collect();

    if old_path.len() != new_path.len() {
        return Err(format!(
            "Error: The paths for 'old_field' and 'new_field' must have the same depth. \
             Found 'old_field' with {} levels and 'new_field' with {} levels.",
            old_path.len(),
            new_path.len()
        ));
    }

    if old_path[..old_path.len() - 1] != new_path[..new_path.len() - 1] {
        return Err(format!(
            "Error: The paths for 'old_field' and 'new_field' must be identical up to the last key. \
             Found 'old_field' path: {:?} and 'new_field' path: {:?}.",
            &old_path[..old_path.len() - 1],
            &new_path[..new_path.len() - 1]
        ));
    }

    Ok(Args {
        db_url,
        table_name,
        old_field,
        new_field,
        dry_run,
    })
}

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

/// Fetch all documents from the specified CouchDB table
async fn fetch_documents(
    client: &Client,
    db_url: &str,
    table_name: &str,
    is_partitioned: bool,
) -> Result<Vec<Value>, String> {
    let mut url = format!("{}/{}/_all_docs?include_docs=true", db_url, table_name);

    // If the table is partitioned, fetch documents from all partitions
    if is_partitioned {
        url.push_str("&startkey=\"_design/\"");
    }

    let response = client.get(&url).send().await.map_err(|e| e.to_string())?;

    if response.status() != StatusCode::OK {
        return Err(format!(
            "Failed to fetch documents: Status code {}",
            response.status()
        ));
    }

    let body = response.text().await.map_err(|e| e.to_string())?;
    let json: Value = from_str(&body).map_err(|e| e.to_string())?;

    // Extract the "rows" array and map it to the "doc" field
    let rows = json["rows"]
        .as_array()
        .ok_or("No 'rows' field in response")?;
    let docs: Vec<Value> = rows
        .iter()
        .filter_map(|row| row["doc"].as_object().cloned())
        .map(Value::Object)
        .collect();

    Ok(docs)
}

/// Recursively rename a field in a JSON document, including nested object arrays
fn rename_nested_field(doc: &mut Value, old_field_path: &[&str], new_field: &str) -> bool {
    if old_field_path.is_empty() {
        return false; // Invalid path
    }

    let (current_key, remaining_path) = old_field_path.split_first().unwrap();

    match doc {
        Value::Object(obj) => {
            if let Some(value) = obj.get_mut(*current_key) {
                if remaining_path.is_empty() {
                    // Base case: Rename the field
                    if let Some(value) = obj.remove(*current_key) {
                        // split the new_field into components
                        let new_field_path: Vec<&str> = new_field.split('.').collect();
                        // use last element as the new field name
                        let new_field = new_field_path.last().unwrap();

                        obj.insert(new_field.to_string(), value);
                        return true;
                    }
                } else {
                    // Recursive case: Traverse deeper
                    return rename_nested_field(value, remaining_path, new_field);
                }
            }
        }
        Value::Array(arr) => {
            // Process each element in the array recursively
            let mut renamed = false;
            for item in arr {
                renamed |= rename_nested_field(item, old_field_path, new_field);
            }
            return renamed;
        }
        _ => {}
    }

    false
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
    let args = match parse_args() {
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
        match fetch_documents(&client, &args.db_url, &args.table_name, is_partitioned).await {
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
        let renamed = rename_nested_field(&mut doc, &old_field_path, &args.new_field);

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
