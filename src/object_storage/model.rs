#[derive(Debug, Clone)]
pub struct StoredObject {
    pub checksum_sha256: String,
    pub size_bytes: usize,
}
