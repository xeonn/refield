use reqwest::{Client, StatusCode};
use serde_json::{from_str, Value};

/// A struct to fetch documents from a CouchDB database.
/// It supports pagination, partitioned tables, and applying a callback to each document.
pub struct FetchDocument<'a> {
    client: Client,                          // HTTP client for making requests
    db_host: String,                         // Base URL of the CouchDB instance
    table_name: String,                      // Name of the database or table
    is_partitioned: bool,                    // Indicates if the table is partitioned
    callback: Box<dyn Fn(Value) -> () + 'a>, // Callback function to process each document
    bookmark: Option<String>,                // Bookmark for pagination
    limit: usize,                            // Maximum number of documents to fetch per request
    doc_count: usize,                        // Total number of documents in the table
}

impl<'a> FetchDocument<'a> {
    /// Constructs a new `FetchDocument` instance with default values.
    pub fn new(client: Client, db_host: String, table_name: String, limit: usize) -> Self {
        Self {
            client,
            db_host,
            table_name,
            is_partitioned: false,      // Default to not partitioned
            callback: Box::new(|_| ()), // Default callback does nothing
            bookmark: None,             // No initial bookmark
            limit,
            doc_count: 0, // Document count starts at 0
        }
    }

    /// Sets the callback function to be applied to each fetched document.
    pub fn with_callback(mut self, callback: Box<dyn Fn(Value) -> () + 'a>) -> Self {
        self.callback = callback; // Assign the provided callback
        self
    }

    /// Executes the document fetching process.
    /// - Fetches metadata about the table.
    /// - Fetches documents in batches and applies the callback to each document.
    pub async fn execute(mut self) {
        // Fetch metadata about the table (e.g., partitioned status, document count)
        self.get_metadata().await.unwrap();

        let mut count = 1; // Counter for tracking the number of iterations
        let mut total_record = 0; // Total number of records fetched so far

        loop {
            // Fetch a batch of documents and apply the callback
            let num_of_record = self.fetch_and_apply().await.unwrap();
            total_record += num_of_record;

            // Log progress
            println!(
                "Fetched {}/{} transactions. Iteration: {}",
                total_record, self.doc_count, count
            );

            // Break the loop if fewer records than the limit are returned (end of data)
            if num_of_record < self.limit {
                break;
            }

            count += 1; // Increment the iteration counter
        }
    }

    /// Fetches metadata about the table, including whether it is partitioned and the total document count.
    async fn get_metadata(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Construct the URL for fetching table metadata
        let url = format!("{}/{}", self.db_host, self.table_name);

        // Send a GET request to fetch metadata
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        // Check if the response status is successful (HTTP 200)
        if response.status() != StatusCode::OK {
            return Err(format!(
                "Failed to fetch table metadata: Status code {}",
                response.status()
            )
            .into());
        }

        // Parse the response body as JSON
        let body = response.text().await.map_err(|e| e.to_string())?;
        let json: Value = from_str(&body).map_err(|e| e.to_string())?;

        // Extract the "partitioned" field to determine if the table is partitioned
        self.is_partitioned = json["props"]["partitioned"].as_bool().unwrap_or(false);

        // Extract the total document count
        self.doc_count = json["doc_count"].as_u64().unwrap_or(0) as usize;

        // Log whether the table is partitioned
        if self.is_partitioned {
            println!("Table '{}' is partitioned.", self.table_name);
        } else {
            println!("Table '{}' is not partitioned.", self.table_name);
        }

        Ok(())
    }

    /// Fetches a batch of documents and applies the callback to each document.
    async fn fetch_and_apply(&mut self) -> Result<usize, String> {
        // Construct the URL for fetching documents
        let url = format!(
            "{}/{}/_find?include_docs=true",
            self.db_host, self.table_name
        );

        // Create the query selector JSON
        let selector = serde_json::to_string(&SelectorContent {
            selector: serde_json::json!({
                "_id": {
                    "$gt": null // Fetch all documents with _id greater than null
                }
            }),
            limit: self.limit as i32, // Limit the number of documents per request
            bookmark: self.bookmark.clone(), // Use the bookmark for pagination
        })
        .map_err(|e| e.to_string())?;

        // Send the POST request to fetch documents
        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(selector)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        // Check if the response status is successful (HTTP 200)
        if response.status() != StatusCode::OK {
            return Err(format!(
                "Failed to fetch documents: Status code {}",
                response.status()
            ));
        }

        // Parse the response body as JSON
        let body = response.text().await.map_err(|e| e.to_string())?;
        let json: Value = from_str(&body).map_err(|e| e.to_string())?;

        // Extract the bookmark for pagination
        self.bookmark = json["bookmark"].as_str().map(String::from);

        // Extract the "docs" array from the response
        let rows = json["docs"]
            .as_array()
            .ok_or("No 'docs' field in response")?;

        // Apply the callback to each document
        let count = rows
            .iter()
            .map(|doc| (self.callback)(doc.clone())) // Call the callback for each document
            .count(); // Count the number of documents processed

        Ok(count) // Return the number of documents processed
    }
}

/// Represents the structure of the query selector used for fetching documents.
#[derive(Debug, serde::Serialize)]
struct SelectorContent {
    selector: serde_json::Value, // JSON object representing the query conditions
    limit: i32,                  // Maximum number of records to fetch
    #[serde(skip_serializing_if = "Option::is_none")]
    bookmark: Option<String>, // Optional bookmark for pagination
}
