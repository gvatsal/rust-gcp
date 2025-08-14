use axum::extract::{Form, FromRequest, FromRequestParts, Multipart, Query, Request};
use axum::http::{header, StatusCode};
use axum::Json;
use http::Method;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[allow(dead_code)]
#[derive(Debug)]
pub enum BodyKind {
    // Content-Type: application/json
    Json(JsonValue),
    // Content-Type: application/x-www-form-urlencoded
    UrlEncodedForm(HashMap<String, String>),
    // Content-Type: multipart/form-data
    Multipart(Multipart),
    // If no body (e.g., GET request, or unsupported/missing Content-Type)
    Empty,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct RequestContext {
    pub query: HashMap<String, String>,
    pub body: BodyKind,
    pub payload: HashMap<String, String>,
}

impl<S> FromRequest<S> for RequestContext
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let method = req.method();

        // For OPTIONS request, returning an empty context
        if method == Method::OPTIONS {
            tracing::debug!("Options request returned");
            return Ok(Self {
                query: HashMap::new(),
                body: BodyKind::Empty,
                payload: HashMap::new(),
            });
        }

        let (mut parts, body) = req.into_parts();

        let Query(query) = Query::<HashMap<String, String>>::from_request_parts(&mut parts, state)
            .await
            .unwrap_or(Query(HashMap::new()));

        let req = Request::from_parts(parts, body);

        let content_type = req.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let mut payload = query.clone();
        let mut body = BodyKind::Empty;

        if content_type.starts_with("application/json") {
            let Json(json) = match Json::<JsonValue>::from_request(req, state).await {
                Ok(json) => json,
                Err(err) => {
                    tracing::error!("Invalid JSON body - err: {}", err);
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("Invalid JSON body: {}", err),
                    ));
                }
            };

            if let JsonValue::Object(map) = &json {
                for (k, v) in map.iter() {
                    if let JsonValue::String(s) = v {
                        payload.insert(k.clone(), s.clone());
                    }
                    else if let JsonValue::Number(n) = v {
                        payload.insert(k.clone(), n.clone().to_string());
                    }
                    else if let JsonValue::Bool(b) = v {
                        payload.insert(k.clone(), b.clone().to_string());
                    }
                    else if let JsonValue::Array(a) = v {
                        payload.insert(k.clone(), serde_json::to_string(a).unwrap_or_default());
                    }
                    else if let JsonValue::Object(o) = v {
                        payload.insert(k.clone(), serde_json::to_string(o).unwrap_or_default());
                    }
                }
            }

            body = BodyKind::Json(json);
        }
        else if content_type.starts_with("application/x-www-form-urlencoded") {
            let Form(form) = match Form::<HashMap<String, String>>::from_request(req, state).await {
                Ok(form) => form,
                Err(err) => {
                    tracing::error!("Invalid Form body - err: {}", err);
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("Invalid Form body: {}", err),
                    ));
                }
            };

            for (k, v) in form.iter() {
                payload.insert(k.clone(), v.clone());
            }

            body = BodyKind::UrlEncodedForm(form);
        }
        else if content_type.starts_with("multipart/form-data") {
            let mut multipart = match Multipart::from_request(req, state).await {
                Ok(multipart) => multipart,
                Err(err) => {
                    tracing::error!("Invalid multipart body - err: {}", err);
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("Invalid multipart body: {}", err),
                    ));
                }
            };

            loop {
                match multipart.next_field().await {
                    Ok(Some(field)) => {
                        if let Some(field_name) = field.name().map(|s| s.to_string()) {
                            if let Some(file_name) = field.file_name() {
                                // Ignoring file upload case wrt payload, can be expanded later
                                tracing::warn!("Multipart body contains file upload, which is currently ignored - filename: {}", file_name);
                            }
                            else {
                                if let Ok(txt) = field.text().await {
                                    payload.insert(field_name.clone(), txt.clone());
                                }
                            }
                        }
                    }
                    Ok(None) => break, // no more parts
                    Err(err) => {
                        tracing::error!("Error parsing multipart body - err: {}", err);
                        return Err((
                            StatusCode::BAD_REQUEST,
                            format!("Error parsing multipart body: {}", err),
                        ));
                    }
                }
            }

            body = BodyKind::Multipart(multipart);
        }

        tracing::debug!("Request - query: {:?}, body: {:?}, payload: {:?}", query, body, payload);
        Ok(Self {
            query,
            body,
            payload,
        })
    }
}