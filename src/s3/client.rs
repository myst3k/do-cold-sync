// TODO: Test multipart uploads with large objects (100+ MiB)
// TODO: Test multipart downloads with byte-range parallel fetching

use std::fmt::Write;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use aws_sigv4::http_request::SignableBody;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use quick_xml::events::Event;
use quick_xml::Reader;
use tracing::debug;

use super::signing;
use super::types::{ListObjectsV2Response, GetObjectResponse, PutObjectResponse, HeadObjectResponse, CreateMultipartUploadResponse, UploadPartResponse, CompletedPart, S3Object};

#[derive(Clone)]
pub struct S3Client {
    http: reqwest::Client,
    endpoint: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    user_agent: String,
}

impl S3Client {
    pub fn new(
        endpoint: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
        user_agent: String,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            http,
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            user_agent,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Build the URL for a bucket operation.
    /// Virtual-host style: <https://bucket.endpoint>/
    fn bucket_url(&self, bucket: &str) -> String {
        let base = self.endpoint.trim_end_matches('/');
        if let Some(scheme_end) = base.find("://") {
            let scheme = &base[..scheme_end + 3];
            let host = &base[scheme_end + 3..];
            format!("{scheme}{bucket}.{host}")
        } else {
            format!("{base}/{bucket}")
        }
    }

    fn object_url(&self, bucket: &str, key: &str) -> String {
        let encoded_key = Self::encode_key(key);
        format!("{}/{}", self.bucket_url(bucket), encoded_key)
    }

    fn encode_key(key: &str) -> String {
        // RFC 3986: encode each path segment, preserve slashes
        key.split('/')
            .map(|segment| urlencoding::encode(segment).into_owned())
            .collect::<Vec<_>>()
            .join("/")
    }

    async fn execute_signed(
        &self,
        method: &str,
        url: &str,
        extra_headers: Vec<(String, String)>,
        body: Option<bytes::Bytes>,
        signable_body: SignableBody<'_>,
    ) -> Result<reqwest::Response> {
        // x-amz-content-sha256 is required by Wasabi and some S3 providers
        let content_sha = match &signable_body {
            SignableBody::Bytes(b) => {
                use sha2::{Sha256, Digest};
                let hash = Sha256::digest(b);
                hex::encode(hash)
            }
            _ => "UNSIGNED-PAYLOAD".to_string(),
        };

        let mut headers = vec![
            ("user-agent".to_string(), self.user_agent.clone()),
            ("host".to_string(), extract_host(url)?),
            ("x-amz-content-sha256".to_string(), content_sha),
        ];
        headers.extend(extra_headers);

        let sig_headers = signing::sign_request(
            method,
            url,
            &headers,
            signable_body,
            &self.region,
            &self.access_key_id,
            &self.secret_access_key,
        )?;

        let mut request = self
            .http
            .request(method.parse().unwrap(), url);

        for (k, v) in &headers {
            request = request.header(k.as_str(), v.as_str());
        }
        for (k, v) in &sig_headers {
            request = request.header(k.as_str(), v.as_str());
        }

        if let Some(body) = body {
            request = request.body(body);
        }

        let resp = request.send().await.context("HTTP request failed")?;

        Ok(resp)
    }

    async fn execute_signed_streaming(
        &self,
        method: &str,
        url: &str,
        extra_headers: Vec<(String, String)>,
        body: reqwest::Body,
    ) -> Result<reqwest::Response> {
        let mut headers = vec![
            ("user-agent".to_string(), self.user_agent.clone()),
            ("host".to_string(), extract_host(url)?),
            (
                "x-amz-content-sha256".to_string(),
                "UNSIGNED-PAYLOAD".to_string(),
            ),
        ];
        headers.extend(extra_headers);

        let sig_headers = signing::sign_request(
            method,
            url,
            &headers,
            SignableBody::UnsignedPayload,
            &self.region,
            &self.access_key_id,
            &self.secret_access_key,
        )?;

        let mut request = self
            .http
            .request(method.parse().unwrap(), url);

        for (k, v) in &headers {
            request = request.header(k.as_str(), v.as_str());
        }
        for (k, v) in &sig_headers {
            request = request.header(k.as_str(), v.as_str());
        }

        request = request.body(body);

        let resp = request.send().await.context("HTTP request failed")?;

        Ok(resp)
    }

    async fn check_error(&self, resp: reqwest::Response) -> Result<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }

        // Capture redirect location for debugging
        let location = resp.headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(std::string::ToString::to_string);

        let body = resp
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read body>".to_string());

        let message = parse_s3_error(&body).unwrap_or(body);
        if let Some(loc) = location {
            bail!("S3 error (HTTP {status}): {message} (redirect to: {loc})");
        }
        bail!("S3 error (HTTP {status}): {message}");
    }

    pub async fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: Option<i32>,
        start_after: Option<&str>,
    ) -> Result<ListObjectsV2Response> {
        self.list_objects_v2_with_delimiter(bucket, prefix, continuation_token, max_keys, start_after, None).await
    }

    pub async fn list_objects_v2_with_delimiter(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: Option<i32>,
        start_after: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<ListObjectsV2Response> {
        let mut url = format!("{}?list-type=2", self.bucket_url(bucket));

        if let Some(p) = prefix {
            let _ = write!(url, "&prefix={}", utf8_percent_encode(p, NON_ALPHANUMERIC));
        }
        if let Some(d) = delimiter {
            let _ = write!(url, "&delimiter={}", utf8_percent_encode(d, NON_ALPHANUMERIC));
        }
        if let Some(mk) = max_keys {
            let _ = write!(url, "&max-keys={mk}");
        }
        if let Some(ct) = continuation_token {
            let _ = write!(url, "&continuation-token={}", utf8_percent_encode(ct, NON_ALPHANUMERIC));
        }
        if let Some(sa) = start_after {
            let _ = write!(url, "&start-after={}", utf8_percent_encode(sa, NON_ALPHANUMERIC));
        }

        debug!("list_objects_v2: {}", url);

        let resp = self
            .execute_signed("GET", &url, vec![], None, SignableBody::Bytes(&[]))
            .await?;
        let resp = self.check_error(resp).await?;
        let body = resp.text().await.context("failed to read response body")?;

        parse_list_objects_v2(&body)
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<GetObjectResponse> {
        let url = self.object_url(bucket, key);

        let mut extra_headers = vec![];
        if let Some((start, end)) = range {
            extra_headers.push(("range".to_string(), format!("bytes={start}-{end}")));
        }

        let resp = self
            .execute_signed("GET", &url, extra_headers, None, SignableBody::UnsignedPayload)
            .await?;
        let resp = self.check_error(resp).await?;

        let content_length = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(std::string::ToString::to_string);

        Ok(GetObjectResponse {
            body: resp,
            content_length,
            etag,
        })
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: reqwest::Body,
        content_length: u64,
    ) -> Result<PutObjectResponse> {
        let url = self.object_url(bucket, key);

        let extra_headers = vec![
            ("content-length".to_string(), content_length.to_string()),
        ];

        let resp = self
            .execute_signed_streaming("PUT", &url, extra_headers, body)
            .await?;
        let resp = self.check_error(resp).await?;

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        Ok(PutObjectResponse { etag })
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<HeadObjectResponse> {
        let url = self.object_url(bucket, key);

        let resp = self
            .execute_signed("HEAD", &url, vec![], None, SignableBody::Bytes(&[]))
            .await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(HeadObjectResponse {
                content_length: 0,
                etag: None,
                exists: false,
            });
        }

        let resp = self.check_error(resp).await?;

        let content_length = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(std::string::ToString::to_string);

        Ok(HeadObjectResponse {
            content_length,
            etag,
            exists: true,
        })
    }

    /// Make a signed GET request to an arbitrary URL
    pub async fn signed_get(&self, url: &str) -> Result<reqwest::Response> {
        self.execute_signed("GET", url, vec![], None, SignableBody::Bytes(&[]))
            .await?
            .error_for_status()
            .map_err(|e| anyhow::anyhow!("signed GET failed: {e}"))
    }

    /// HEAD the endpoint root and return bucket metadata.
    /// Returns (`is_cold`, `object_count`, `bytes_used`).
    pub async fn head_bucket_info(&self) -> Result<(bool, u64, u64)> {
        let url = format!("{}/", self.endpoint.trim_end_matches('/'));
        let resp = self
            .execute_signed("HEAD", &url, vec![], None, SignableBody::Bytes(&[]))
            .await?;

        let is_cold = resp.headers().contains_key("x-rgw-bytes-billed");
        let object_count = resp.headers()
            .get("x-rgw-object-count")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let bytes_used = resp.headers()
            .get("x-rgw-bytes-used")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        Ok((is_cold, object_count, bytes_used))
    }

    pub async fn head_bucket(&self, bucket: &str) -> Result<bool> {
        let url = self.bucket_url(bucket);

        let resp = self
            .execute_signed("HEAD", &url, vec![], None, SignableBody::Bytes(&[]))
            .await?;

        Ok(resp.status().is_success())
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let url = self.bucket_url(bucket);

        let resp = self
            .execute_signed("PUT", &url, vec![], None, SignableBody::Bytes(&[]))
            .await?;
        self.check_error(resp).await?;

        Ok(())
    }

    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<CreateMultipartUploadResponse> {
        let url = format!("{}?uploads", self.object_url(bucket, key));

        let resp = self
            .execute_signed("POST", &url, vec![], None, SignableBody::Bytes(&[]))
            .await?;
        let resp = self.check_error(resp).await?;
        let body = resp.text().await.context("failed to read response body")?;

        let upload_id = parse_upload_id(&body)?;

        Ok(CreateMultipartUploadResponse { upload_id })
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: reqwest::Body,
        content_length: u64,
    ) -> Result<UploadPartResponse> {
        let url = format!(
            "{}?partNumber={}&uploadId={}",
            self.object_url(bucket, key),
            part_number,
            utf8_percent_encode(upload_id, NON_ALPHANUMERIC)
        );

        let extra_headers = vec![
            ("content-length".to_string(), content_length.to_string()),
        ];

        let resp = self
            .execute_signed_streaming("PUT", &url, extra_headers, body)
            .await?;
        let resp = self.check_error(resp).await?;

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        Ok(UploadPartResponse { etag, part_number })
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<PutObjectResponse> {
        let url = format!(
            "{}?uploadId={}",
            self.object_url(bucket, key),
            utf8_percent_encode(upload_id, NON_ALPHANUMERIC)
        );

        let xml_body = build_complete_multipart_xml(&parts);
        let body_bytes = bytes::Bytes::from(xml_body);

        let extra_headers = vec![
            ("content-type".to_string(), "application/xml".to_string()),
            ("content-length".to_string(), body_bytes.len().to_string()),
        ];

        let resp = self
            .execute_signed(
                "POST",
                &url,
                extra_headers,
                Some(body_bytes.clone()),
                SignableBody::Bytes(&body_bytes),
            )
            .await?;
        let resp = self.check_error(resp).await?;

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        Ok(PutObjectResponse { etag })
    }

    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<()> {
        let url = format!(
            "{}?uploadId={}",
            self.object_url(bucket, key),
            utf8_percent_encode(upload_id, NON_ALPHANUMERIC)
        );

        let resp = self
            .execute_signed("DELETE", &url, vec![], None, SignableBody::Bytes(&[]))
            .await?;
        self.check_error(resp).await?;

        Ok(())
    }
}

fn extract_host(url: &str) -> Result<String> {
    let parsed =
        reqwest::Url::parse(url).context("failed to parse URL")?;
    let host = parsed.host_str().context("URL has no host")?;
    match parsed.port() {
        Some(port) => Ok(format!("{host}:{port}")),
        None => Ok(host.to_string()),
    }
}

fn parse_s3_error(body: &str) -> Option<String> {
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);
    let mut in_code = false;
    let mut in_message = false;
    let mut code = String::new();
    let mut message = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "Code" {
                    in_code = true;
                } else if name == "Message" {
                    in_message = true;
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_code {
                    code = String::from_utf8_lossy(e.as_ref()).to_string();
                    in_code = false;
                } else if in_message {
                    message = String::from_utf8_lossy(e.as_ref()).to_string();
                    in_message = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }

    if code.is_empty() && message.is_empty() {
        None
    } else {
        Some(format!("{code}: {message}"))
    }
}

fn parse_list_objects_v2(body: &str) -> Result<ListObjectsV2Response> {
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut is_truncated = false;
    let mut next_continuation_token: Option<String> = None;

    let mut in_contents = false;
    let mut in_common_prefixes = false;
    let mut current_tag = String::new();
    let mut current_key = String::new();
    let mut current_size: u64 = 0;
    let mut cur_etag = String::new();
    let mut current_last_modified = String::new();
    let mut current_prefix = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "Contents" {
                    in_contents = true;
                    current_key.clear();
                    current_size = 0;
                    cur_etag.clear();
                    current_last_modified.clear();
                } else if name == "CommonPrefixes" {
                    in_common_prefixes = true;
                    current_prefix.clear();
                }
                current_tag = name;
            }
            Ok(Event::Text(ref e)) => {
                let text = String::from_utf8_lossy(e.as_ref()).to_string();
                if in_contents {
                    match current_tag.as_str() {
                        "Key" => current_key = text,
                        "Size" => current_size = text.parse().unwrap_or(0),
                        "ETag" => cur_etag = text,
                        "LastModified" => current_last_modified = text,
                        _ => {}
                    }
                } else if in_common_prefixes {
                    if current_tag == "Prefix" {
                        current_prefix = text;
                    }
                } else {
                    match current_tag.as_str() {
                        "IsTruncated" => is_truncated = text == "true",
                        "NextContinuationToken" => {
                            next_continuation_token = Some(text);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "Contents" {
                    objects.push(S3Object {
                        key: current_key.clone(),
                        size: current_size,
                        etag: cur_etag.clone(),
                        last_modified: current_last_modified.clone(),
                    });
                    in_contents = false;
                } else if name == "CommonPrefixes" {
                    if !current_prefix.is_empty() {
                        common_prefixes.push(current_prefix.clone());
                    }
                    in_common_prefixes = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => bail!("failed to parse ListObjectsV2 XML: {e}"),
            _ => {}
        }
        buf.clear();
    }

    Ok(ListObjectsV2Response {
        objects,
        common_prefixes,
        is_truncated,
        next_continuation_token,
    })
}

fn parse_upload_id(body: &str) -> Result<String> {
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut in_upload_id = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "UploadId" {
                    in_upload_id = true;
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_upload_id {
                    return Ok(String::from_utf8_lossy(e.as_ref()).to_string());
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => bail!("failed to parse multipart upload XML: {e}"),
            _ => {}
        }
        buf.clear();
    }

    bail!("UploadId not found in response XML")
}

fn build_complete_multipart_xml(parts: &[CompletedPart]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for part in parts {
        let _ = write!(
            xml,
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            part.part_number, part.etag
        );
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_builds() {
        let client = S3Client::new(
            "http://localhost:9000".to_string(),
            "us-east-1".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            "do-cold-sync/1.0".to_string(),
        );
        assert!(client.is_ok());
    }

    #[test]
    fn types_construct() {
        let obj = S3Object {
            key: "test/key.bin".to_string(),
            size: 1024,
            etag: "\"abc123\"".to_string(),
            last_modified: "2024-01-01T00:00:00Z".to_string(),
        };
        assert_eq!(obj.key, "test/key.bin");
        assert_eq!(obj.size, 1024);

        let part = CompletedPart {
            part_number: 1,
            etag: "\"def456\"".to_string(),
        };
        assert_eq!(part.part_number, 1);

        let head = HeadObjectResponse {
            content_length: 0,
            etag: None,
            exists: false,
        };
        assert!(!head.exists);

        let put = PutObjectResponse {
            etag: "\"abc\"".to_string(),
        };
        assert_eq!(put.etag, "\"abc\"");

        let upload = CreateMultipartUploadResponse {
            upload_id: "upload-123".to_string(),
        };
        assert_eq!(upload.upload_id, "upload-123");

        let upload_part = UploadPartResponse {
            etag: "\"part-etag\"".to_string(),
            part_number: 3,
        };
        assert_eq!(upload_part.part_number, 3);
    }

    #[test]
    fn parse_list_objects_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
    <IsTruncated>true</IsTruncated>
    <NextContinuationToken>token123</NextContinuationToken>
    <Contents>
        <Key>file1.txt</Key>
        <Size>100</Size>
        <ETag>"etag1"</ETag>
        <LastModified>2024-01-01T00:00:00Z</LastModified>
    </Contents>
    <Contents>
        <Key>file2.txt</Key>
        <Size>200</Size>
        <ETag>"etag2"</ETag>
        <LastModified>2024-01-02T00:00:00Z</LastModified>
    </Contents>
</ListBucketResult>"#;

        let result = parse_list_objects_v2(xml).unwrap();
        assert_eq!(result.objects.len(), 2);
        assert!(result.is_truncated);
        assert_eq!(
            result.next_continuation_token,
            Some("token123".to_string())
        );
        assert_eq!(result.objects[0].key, "file1.txt");
        assert_eq!(result.objects[0].size, 100);
        assert_eq!(result.objects[1].key, "file2.txt");
        assert_eq!(result.objects[1].size, 200);
    }

    #[test]
    fn parse_upload_id_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
    <Bucket>my-bucket</Bucket>
    <Key>my-key</Key>
    <UploadId>upload-id-12345</UploadId>
</InitiateMultipartUploadResult>"#;

        let id = parse_upload_id(xml).unwrap();
        assert_eq!(id, "upload-id-12345");
    }

    #[test]
    fn build_complete_xml() {
        let parts = vec![
            CompletedPart {
                part_number: 1,
                etag: "\"etag1\"".to_string(),
            },
            CompletedPart {
                part_number: 2,
                etag: "\"etag2\"".to_string(),
            },
        ];
        let xml = build_complete_multipart_xml(&parts);
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("<ETag>\"etag1\"</ETag>"));
        assert!(xml.contains("<PartNumber>2</PartNumber>"));
        assert!(xml.starts_with("<CompleteMultipartUpload>"));
        assert!(xml.ends_with("</CompleteMultipartUpload>"));
    }

    #[test]
    fn parse_s3_error_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchKey</Code>
    <Message>The specified key does not exist.</Message>
</Error>"#;

        let err = parse_s3_error(xml).unwrap();
        assert!(err.contains("NoSuchKey"));
        assert!(err.contains("The specified key does not exist."));
    }

    #[test]
    fn encode_key_preserves_slashes() {
        let encoded = S3Client::encode_key("path/to/my file.txt");
        assert_eq!(encoded, "path/to/my%20file%2Etxt");
    }

    #[test]
    fn extract_host_works() {
        assert_eq!(
            extract_host("http://localhost:9000/bucket").unwrap(),
            "localhost:9000"
        );
        assert_eq!(
            extract_host("https://s3.amazonaws.com/bucket").unwrap(),
            "s3.amazonaws.com"
        );
    }
}
