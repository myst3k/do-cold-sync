use anyhow::{Context, Result};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
};
use aws_sigv4::sign::v4;
use std::time::SystemTime;

pub fn sign_request(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: SignableBody,
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<Vec<(String, String)>> {
    let credentials = Credentials::new(access_key, secret_key, None, None, "do-cold-sync");
    let identity = credentials.into();

    let mut settings = SigningSettings::default();
    settings.uri_path_normalization_mode =
        aws_sigv4::http_request::UriPathNormalizationMode::Disabled;
    settings.percent_encoding_mode = PercentEncodingMode::Single;

    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .context("failed to build signing params")?;

    let header_pairs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let signable_request =
        SignableRequest::new(method, url, header_pairs.into_iter(), body)
            .context("failed to create signable request")?;

    let output = sign(signable_request, &signing_params.into())
        .context("failed to sign request")?;

    let signature_headers: Vec<(String, String)> = output
        .output()
        .headers()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    Ok(signature_headers)
}
