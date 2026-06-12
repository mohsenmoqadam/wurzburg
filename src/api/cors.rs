use axum::{
    extract::Request,
    http::{header, Method},
    middleware::Next,
    response::Response,
};

pub async fn manual_cors_middleware(req: Request, next: Next) -> Response {
    if req.method() == Method::OPTIONS {
        let mut res = Response::new(axum::body::Body::empty());
        res.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
        res.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_METHODS, "*".parse().unwrap());
        res.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_HEADERS, "*".parse().unwrap());
        return res;
    }

    let mut res = next.run(req).await;
    res.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
    res
}
