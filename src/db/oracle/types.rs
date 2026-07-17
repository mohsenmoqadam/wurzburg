use uuid::Uuid;

use crate::db::error::{DbError, DbResult};

pub fn uuid_to_raw16(uuid: Uuid) -> [u8; 16] {
    *uuid.as_bytes()
}

pub fn raw16_to_uuid(raw: &[u8]) -> DbResult<Uuid> {
    let bytes: [u8; 16] = raw.try_into().map_err(|_| {
        DbError::Query(format!(
            "expected Oracle RAW(16) UUID value, got {} bytes",
            raw.len()
        ))
    })?;

    Ok(Uuid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_raw16_round_trip_preserves_identity() {
        let id = Uuid::new_v4();
        let raw = uuid_to_raw16(id);

        assert_eq!(raw16_to_uuid(&raw).unwrap(), id);
    }

    #[test]
    fn raw16_to_uuid_rejects_wrong_length() {
        let err = raw16_to_uuid(&[1, 2, 3]).unwrap_err();

        assert!(err.to_string().contains("RAW(16)"));
    }
}
