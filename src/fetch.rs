use reqwest::{Client, StatusCode};
use serde_json::{from_str, Value};

pub struct FetchDocument<'a> {
    client: Client,
    db_host: String,
    table_name: String,
    is_partitioned: bool,

    callback: Box<dyn Fn(Value) -> () + 'a>,

    bookmark: Option<String>,
    limit: usize,
    doc_count: usize,
}

impl<'a> FetchDocument<'a> {
    pub fn new(client: Client, db_host: String, table_name: String, limit: usize) -> Self {
        Self {
            client,
            db_host,
            table_name,
            is_partitioned: false,
            callback: Box::new(|_| ()),
            bookmark: None,
            limit,
            doc_count: 0,
        }
    }

    pub fn with_callback(mut self, callback: Box<dyn Fn(Value) -> () + 'a>) -> Self {
        self.callback = callback;
        self
    }

    pub async fn execute(mut self) {
        // get metadata
        self.get_metadata().await.unwrap();

        let mut count = 1; // Counter for tracking the number of iterations
        let mut total_record = 0;
        loop {
            // Fetch a batch of transactions
            let num_of_record = self.fetch_and_apply().await.unwrap();
            total_record += num_of_record;
            println!(
                "Fetched {}/{} transactions. Iteration: {}",
                total_record, self.doc_count, count
            );

            // Break the loop if fewer records than the limit are returned (end of data)
            if num_of_record < self.limit {
                break;
            }
            count += 1;
        }
    }

    /// Check if the table is partitioned by querying its metadata
    async fn get_metadata(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/{}", self.db_host, self.table_name);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if response.status() != StatusCode::OK {
            return Err(format!(
                "Failed to fetch table metadata: Status code {}",
                response.status()
            )
            .into());
        }

        let body = response.text().await.map_err(|e| e.to_string())?;
        let json: Value = from_str(&body).map_err(|e| e.to_string())?;

        // Check if the "partitioned" field exists and is true
        self.is_partitioned = json["props"]["partitioned"].as_bool().unwrap_or(false);
        self.doc_count = json["doc_count"].as_u64().unwrap_or(0) as usize;

        if self.is_partitioned {
            println!("Table '{}' is partitioned.", self.table_name);
        } else {
            println!("Table '{}' is not partitioned.", self.table_name);
        }

        Ok(())
    }

    async fn fetch_and_apply(&mut self) -> Result<usize, String> {
        let url = format!(
            "{}/{}/_find?include_docs=true",
            self.db_host, self.table_name
        );

        let selector = serde_json::to_string(&SelectorContent {
            selector: serde_json::json!({
                "_id": {
                    "$gt": null
                }
            }),
            limit: self.limit as i32,
            bookmark: self.bookmark.clone(),
        })
        .map_err(|e| e.to_string())?;

        // println!("selector: {}", selector);

        let client = reqwest::Client::new();

        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(selector)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if response.status() != StatusCode::OK {
            return Err(format!(
                "Failed to fetch documents: Status code {}",
                response.status()
            ));
        }

        let body = response.text().await.map_err(|e| e.to_string())?;
        // println!("body: {}", body);
        let json: Value = from_str(&body).map_err(|e| e.to_string())?;

        // Extract bookmark for pagination
        self.bookmark = json["bookmark"].as_str().map(String::from);

        // println!("bookmark: {:?}", self.bookmark);

        // Extract the "rows" array and map it to the "doc" field
        let rows = json["docs"]
            .as_array()
            .ok_or("No 'docs' field in response")?;

        // Apply the callback to each document
        let count = rows
            .iter()
            // .filter_map(|row| row["doc"].as_object().cloned())
            // .map(Value::Object)
            .map(|doc| (self.callback)(doc.clone()))
            .count();

        Ok(count)
    }
}

/// Represents the structure of the query selector used for fetching transactions.
#[derive(Debug, serde::Serialize)]
struct SelectorContent {
    selector: serde_json::Value, // JSON object representing the query conditions
    limit: i32,                  // Maximum number of records to fetch
    #[serde(skip_serializing_if = "Option::is_none")]
    bookmark: Option<String>, // Optional bookmark for pagination
}
