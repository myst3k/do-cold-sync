use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Object {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: String,
}

#[derive(Debug, Clone)]
pub struct ListObjectsV2Response {
    pub objects: Vec<S3Object>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

pub struct GetObjectResponse {
    pub body: reqwest::Response,
    pub content_length: Option<u64>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PutObjectResponse {
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct HeadObjectResponse {
    pub content_length: u64,
    pub etag: Option<String>,
    pub exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateMultipartUploadResponse {
    pub upload_id: String,
}

#[derive(Debug, Clone)]
pub struct UploadPartResponse {
    pub etag: String,
    pub part_number: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedPart {
    pub part_number: i32,
    pub etag: String,
}
