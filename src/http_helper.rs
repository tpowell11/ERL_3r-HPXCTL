use std::{collections::HashMap, fmt::Debug, io::Empty};
use tiny_http::{Header, Response, StatusCode};
pub fn allow_cors_header () -> Header {
    Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap()
}
pub fn json_header() -> Vec<Header> {
    vec![
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        allow_cors_header()
    ]
}
pub fn png_header() -> Vec<Header> {
    vec![
        Header::from_bytes(&b"Content-Type"[..], &b"image/png"[..]).unwrap(),
        allow_cors_header()
    ]
}
pub fn text_header() -> Vec<Header> {
    vec![
        Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
        allow_cors_header()
    ]
}
pub fn resp_200(data: &[u8]) -> Response<&[u8]> {
    Response::new(
        StatusCode::from(200),
        json_header(), 
        data,
        Some(data.len()), 
        None
    )
}
pub fn resp_200_png(data: &[u8]) -> Response<&[u8]> {
    Response::new(
        StatusCode::from(200), 
        png_header(), 
        data, 
        Some(data.len()), 
        None)
}
pub fn resp_200_plain (data: &[u8]) -> Response<&[u8]> {
    Response::new(
        StatusCode(200), 
        text_header(), 
        data, 
        Some(data.len()), 
    None)
}
pub fn resp_204() -> Response<Empty> {
    Response::empty(204)
}
pub fn resp_400(data: &[u8]) -> Response<&[u8]> {
    Response::new(StatusCode::from(403),
    json_header(),
    data, 
    Some(data.len()), 
    None
    )
}
pub fn resp_404(data: &[u8]) -> Response<&[u8]> {
    Response::new(
        StatusCode::from(404), 
        json_header(), 
        data, 
        Some(data.len()), 
        None
    )
}
pub fn resp_405(data: &[u8]) -> Response<&[u8]> {
    Response::new(
        StatusCode(405), 
        json_header(), 
        data, 
        Some(data.len()), 
        None
    )
}
pub fn resp_409(data: &[u8]) -> Response<&[u8]> {
    Response::new(
        StatusCode(409), 
        json_header(), 
        data, 
        Some(data.len()), 
    None)
}
pub fn resp_500(data: &[u8]) -> Response<&[u8]> {
    Response::new(
        StatusCode(500),
        json_header(),
        data, 
        Some(data.len()),
        None
    )
}
pub fn config_ok() -> String {
    "{\"config\":\"ok\"}".to_string()
}
pub fn err <T1:ToString+Debug,T2:ToString+Debug>(code:T1, error:T2) -> String {
    format!("{{\"code\":\"{:?}\",\"error\":\"{:?}\"}}", code, error)
}
pub struct URL {
    base: String,
    params: HashMap<String, String>,
}
impl Default for URL{
    fn default() -> Self {
        Self { base: Default::default(), params: Default::default() }
    }
}
impl URL {
    pub fn ingest(raw: &str) -> Self {
        let mut u = URL::default();
        let s: Vec<String> = raw.split('?').map(|a|String::from(a)).collect(); // [base, params[..]]
        if s.len() == 1 {
            u.base = s.get(0).unwrap().clone();
            return u;
        }
        u.base = s[0].clone();
        let param_string: Vec<String> = s[1].split('&').map(|a|String::from(a)).collect();
        for param in param_string {
            let ps: Vec<String> = param.split('=').map(|a|String::from(a)).collect();
            let pname = ps[0].clone();
            let parg = ps[1].clone();
            u.params.insert(pname, parg);
        }
        return u;
    }
    pub fn get_param_value(&self, key: &str) -> Option<String>{
        match self.params.get(&key.to_owned()){
            Some(v) => return Some(v.clone()),
            None => return None
        }
    }
    pub fn base(&self) -> String {
        return self.base.clone();
    }
}