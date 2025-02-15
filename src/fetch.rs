use reqwest::{Client, StatusCode};
use serde_json::{from_str, Value};

/// Fetch all documents from the specified CouchDB table
pub async fn fetch_documents(
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
