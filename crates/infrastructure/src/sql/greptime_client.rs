// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! GreptimeDB Client Wrapper
//!
//! Provides a high-level interface for interacting with GreptimeDB via HTTP API.
//! GreptimeDB supports both gRPC and HTTP APIs - we use HTTP for simplicity and to avoid
//! dependency conflicts. Handles connection management, write operations, and read operations.

#[cfg(feature = "greptime")]
use std::collections::HashMap;
#[cfg(feature = "greptime")]
use std::time::Duration;

#[cfg(feature = "greptime")]
use anyhow::{Context, Result};
#[cfg(feature = "greptime")]
use reqwest::Client as HttpClient;
#[cfg(feature = "greptime")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "greptime")]
use serde_json::{json, Value};

/// GreptimeDB client wrapper for time-series data operations
#[cfg(feature = "greptime")]
#[derive(Debug, Clone)]
pub struct GreptimeClient {
    http_client: HttpClient,
    base_url: String,
    database: String,
    username: Option<String>,
    password: Option<String>,
}

#[cfg(feature = "greptime")]
impl GreptimeClient {
    /// Create a new GreptimeDB client
    pub async fn new(
        host: &str,
        port: u16,
        database: &str,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        let base_url = format!("http://{}:{}", host, port);
        
        // Create HTTP client with timeouts
        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .context("Failed to create HTTP client")?;
        
        Ok(Self {
            http_client,
            base_url,
            database: database.to_string(),
            username,
            password,
        })
    }

    /// Insert a single row into a table
    pub async fn insert(&self, table_name: &str, row: GreptimeRow) -> Result<()> {
        let rows = vec![row];
        self.insert_batch(table_name, rows).await
    }

    /// Insert multiple rows in a batch using InfluxDB Line Protocol
    /// This enables automatic schema generation in GreptimeDB
    pub async fn insert_batch(&self, table_name: &str, rows: Vec<GreptimeRow>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        
        // Get columns from first row (all rows should have same structure)
        let columns = if let Some(first_row) = rows.first() {
            first_row.columns()
        } else {
            return Ok(());
        };
        
        // Build InfluxDB line protocol format
        // Format: measurement,tag1=value1 field1=value1,field2=value2 timestamp
        // Tags: instrument_id (for filtering/indexing)
        // Fields: all numeric/string values
        // Timestamp: in nanoseconds
        let mut lines = Vec::new();
        for row in &rows {
            let mut tags = Vec::new();
            let mut fields = Vec::new();
            let mut timestamp: Option<i64> = None;
            
            for col in &columns {
                let value = row.data.get(col)
                    .ok_or_else(|| anyhow::anyhow!("Missing column: {}", col))?;
                
                // Timestamp is special - it goes at the end
                if col == "timestamp" {
                    if let GreptimeValue::Timestamp(ts) = value {
                        timestamp = Some(*ts);
                    }
                    continue;
                }
                
                // Tags (for indexing/filtering): instrument_id, indicator_name, indicator_type
                if col == "instrument_id" || col == "indicator_name" || col == "indicator_type" {
                    if let GreptimeValue::String(s) = value {
                        tags.push(format!("{}={}", col, self.escape_influx_tag(s)));
                    }
                    // Skip NULL tag values
                    continue;
                }
                
                // All other columns are fields
                // Skip NULL values (they're omitted in InfluxDB line protocol)
                let field_str = self.value_to_influx_field(col, value)?;
                if !field_str.is_empty() {
                    fields.push(field_str);
                }
            }
            
            // Build the line: measurement,tags fields timestamp
            let mut line = table_name.to_string();
            if !tags.is_empty() {
                line.push(',');
                line.push_str(&tags.join(","));
            }
            if !fields.is_empty() {
                line.push(' ');
                line.push_str(&fields.join(","));
            }
            if let Some(ts) = timestamp {
                line.push(' ');
                line.push_str(&ts.to_string());
            }
            lines.push(line);
        }
        
        let line_protocol = lines.join("\n");
        
        // Use InfluxDB write API endpoint for automatic schema generation
        let url = format!("{}/v1/influxdb/write", self.base_url);
        let mut request = self.http_client
            .post(&url)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(line_protocol.clone());
        
        // Add authentication if provided
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }
        
        let response = request
            .send()
            .await
            .context("Failed to send insert request to GreptimeDB")?;
        
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            anyhow::bail!("GreptimeDB insert failed with status {}: {}", status, error_text);
        }
        
        Ok(())
    }
    
    /// Escape InfluxDB tag/field names and values
    fn escape_influx_tag(&self, s: &str) -> String {
        // Escape commas, spaces, and equals signs in tag values
        s.replace(",", "\\,")
         .replace(" ", "\\ ")
         .replace("=", "\\=")
    }
    
    /// Convert GreptimeValue to InfluxDB field format
    fn value_to_influx_field(&self, name: &str, value: &GreptimeValue) -> Result<String> {
        // Escape field name (commas, spaces, equals)
        let escaped_name = name.replace(",", "\\,")
                               .replace(" ", "\\ ")
                               .replace("=", "\\=");
        
        match value {
            GreptimeValue::Int64(v) => Ok(format!("{}={}i", escaped_name, v)),
            GreptimeValue::Float64(v) => Ok(format!("{}={}", escaped_name, v)),
            GreptimeValue::String(v) => {
                // Escape string field value
                let escaped = v.replace("\\", "\\\\")
                              .replace("\"", "\\\"")
                              .replace(",", "\\,")
                              .replace(" ", "\\ ");
                Ok(format!("{}=\"{}\"", escaped_name, escaped))
            }
            GreptimeValue::Boolean(v) => Ok(format!("{}={}", escaped_name, if *v { "true" } else { "false" })),
            GreptimeValue::Json(v) => {
                // JSON values need to be properly escaped and quoted
                let escaped = v.replace("\\", "\\\\")
                              .replace("\"", "\\\"")
                              .replace(",", "\\,")
                              .replace(" ", "\\ ");
                Ok(format!("{}=\"{}\"", escaped_name, escaped))
            }
            GreptimeValue::Timestamp(_) => {
                // Timestamp is handled separately, shouldn't be here
                anyhow::bail!("Timestamp should not be converted to field");
            }
            GreptimeValue::Null => {
                // NULL values are omitted from InfluxDB line protocol
                // Return empty string so caller can skip this field
                Ok(String::new())
            }
        }
    }

    /// Execute a SQL query and return results
    pub async fn query(&self, sql: &str) -> Result<Vec<GreptimeRow>> {
        // Build query request with form-encoded data
        // Note: GreptimeDB uses schemas, not databases. Tables are in the default "public" schema.
        let mut form_data = std::collections::HashMap::new();
        form_data.insert("sql".to_string(), sql.to_string());
        
        // Make HTTP POST request to GreptimeDB query endpoint
        let url = format!("{}/v1/sql", self.base_url);
        let mut request = self.http_client.post(&url).form(&form_data);
        
        // Add authentication if provided
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }
        
        let response = request
            .send()
            .await
            .context("Failed to send query request to GreptimeDB")?;
        
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            anyhow::bail!("GreptimeDB query failed with status {}: {}", status, error_text);
        }
        
        // Parse response
        let result: Value = response
            .json()
            .await
            .context("Failed to parse GreptimeDB query response")?;
        
        // Convert GreptimeDB response to GreptimeRow format
        self.parse_query_result(result)
    }

    /// Ensure a table exists with the given schema
    /// Creates the table if it doesn't exist
    pub async fn ensure_table(&self, table_name: &str, schema: &str) -> Result<()> {
        let create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, schema);
        self.query(&create_sql).await?;
        Ok(())
    }
    
    /// Convert GreptimeValue to JSON value for HTTP API
    fn value_to_json(&self, value: &GreptimeValue) -> Result<Value> {
        match value {
            GreptimeValue::Int64(v) => Ok(json!(v)),
            GreptimeValue::Float64(v) => Ok(json!(v)),
            GreptimeValue::String(v) => Ok(json!(v)),
            GreptimeValue::Boolean(v) => Ok(json!(v)),
            GreptimeValue::Timestamp(ns) => {
                // GreptimeDB expects timestamps in nanoseconds
                Ok(json!(ns))
            }
            GreptimeValue::Json(v) => {
                // Parse JSON string to Value
                serde_json::from_str(v).map_err(|e| anyhow::anyhow!("Invalid JSON in GreptimeValue: {}", e))
            }
            GreptimeValue::Null => Ok(Value::Null),
        }
    }
    
    /// Parse GreptimeDB query result into GreptimeRow format
    fn parse_query_result(&self, result: Value) -> Result<Vec<GreptimeRow>> {
        // GreptimeDB query response format:
        // { "output": [{ "records": { "schema": {...}, "rows": [...] } }] }
        let mut rows = Vec::new();
        
        if let Some(output) = result.get("output").and_then(|o| o.as_array()) {
            for item in output {
                if let Some(records) = item.get("records") {
                    if let Some(rows_data) = records.get("rows").and_then(|r| r.as_array()) {
                        if let Some(schema) = records.get("schema").and_then(|s| s.get("column_schemas")) {
                            let columns: Vec<String> = schema
                                .as_array()
                                .unwrap_or(&vec![])
                                .iter()
                                .filter_map(|col| col.get("column_name").and_then(|n| n.as_str()).map(|s| s.to_string()))
                                .collect();
                            
                            for row_data in rows_data {
                                if let Some(row_array) = row_data.as_array() {
                                    let mut greptime_row = GreptimeRow::new();
                                    for (i, value) in row_array.iter().enumerate() {
                                        if i < columns.len() {
                                            let col_name = &columns[i];
                                            let greptime_value = self.json_to_value(value)?;
                                            greptime_row.data.insert(col_name.clone(), greptime_value);
                                        }
                                    }
                                    rows.push(greptime_row);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(rows)
    }
    
    /// Convert JSON value to GreptimeValue
    fn json_to_value(&self, value: &Value) -> Result<GreptimeValue> {
        match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(GreptimeValue::Int64(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(GreptimeValue::Float64(f))
                } else {
                    anyhow::bail!("Unable to convert number to GreptimeValue")
                }
            }
            Value::String(s) => Ok(GreptimeValue::String(s.clone())),
            Value::Bool(b) => Ok(GreptimeValue::Boolean(*b)),
            Value::Null => Ok(GreptimeValue::Null),
            _ => anyhow::bail!("Unsupported JSON value type for GreptimeDB")
        }
    }
}

/// Row data structure for GreptimeDB operations
#[cfg(feature = "greptime")]
#[derive(Debug, Clone)]
pub struct GreptimeRow {
    pub(crate) data: HashMap<String, GreptimeValue>,
}

#[cfg(feature = "greptime")]
impl GreptimeRow {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn with_column(mut self, name: &str, value: GreptimeValue) -> Self {
        self.data.insert(name.to_string(), value);
        self
    }

    pub fn columns(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    pub fn values(&self) -> Vec<GreptimeValue> {
        self.data.values().cloned().collect()
    }
}

#[cfg(feature = "greptime")]
impl Default for GreptimeRow {
    fn default() -> Self {
        Self::new()
    }
}

/// Value types supported by GreptimeDB
#[cfg(feature = "greptime")]
#[derive(Debug, Clone)]
pub enum GreptimeValue {
    Int64(i64),
    Float64(f64),
    String(String),
    Boolean(bool),
    Timestamp(i64), // Unix timestamp in nanoseconds
    Json(String), // JSON string value for complex data structures
    Null, // NULL value for optional columns
}

// Stub implementation when greptime feature is not enabled
#[cfg(not(feature = "greptime"))]
pub struct GreptimeClient;

#[cfg(not(feature = "greptime"))]
impl GreptimeClient {
    pub async fn new(
        _host: &str,
        _port: u16,
        _database: &str,
        _username: Option<String>,
        _password: Option<String>,
    ) -> anyhow::Result<Self> {
        anyhow::bail!("GreptimeDB feature is not enabled. Enable the 'greptime' feature to use GreptimeDB.")
    }

    pub async fn insert(&self, _table_name: &str, _row: GreptimeRow) -> anyhow::Result<()> {
        anyhow::bail!("GreptimeDB feature is not enabled")
    }

    pub async fn insert_batch(&self, _table_name: &str, _rows: Vec<GreptimeRow>) -> anyhow::Result<()> {
        anyhow::bail!("GreptimeDB feature is not enabled")
    }

    pub async fn query(&self, _sql: &str) -> anyhow::Result<Vec<GreptimeRow>> {
        anyhow::bail!("GreptimeDB feature is not enabled")
    }

    pub async fn ensure_table(&self, _table_name: &str, _schema: &str) -> anyhow::Result<()> {
        anyhow::bail!("GreptimeDB feature is not enabled")
    }
}

#[cfg(not(feature = "greptime"))]
pub struct GreptimeRow;

#[cfg(not(feature = "greptime"))]
#[derive(Debug, Clone)]
pub enum GreptimeValue {
    Int64(i64),
    Float64(f64),
    String(String),
    Boolean(bool),
    Timestamp(i64),
}
