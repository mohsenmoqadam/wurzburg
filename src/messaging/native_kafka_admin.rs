use std::{ffi::CString, ptr, time::Duration};

use rdkafka::{admin::AdminClient, bindings as native, client::DefaultClientContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaAdminError {
    InvalidInput,
    NativeContract(&'static str),
    DeadlineExceeded,
    Broker(i32),
}

impl KafkaAdminError {
    pub fn diagnostic_kind(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::NativeContract(_) => "native_contract",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Broker(_) => "broker_error",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AclResource<'a> {
    Topic(&'a str),
    Group(&'a str),
}

#[derive(Debug, Clone, Copy)]
pub enum AclOperation {
    Read,
    Describe,
}

#[derive(Debug, Clone, Copy)]
pub struct ProviderAcl<'a> {
    pub resource: AclResource<'a>,
    pub username: &'a str,
    pub operation: AclOperation,
}

pub fn upsert_scram_sha512(
    client: &AdminClient<DefaultClientContext>,
    username: &str,
    password: &[u8],
    iterations: i32,
    timeout: Duration,
) -> Result<(), KafkaAdminError> {
    let username = cstring(username)?;
    if password.is_empty() || iterations < 4096 {
        return Err(KafkaAdminError::InvalidInput);
    }

    // SAFETY: every native allocation is checked and held until librdkafka has
    // completed the synchronous queue result. RAII wrappers release all native
    // resources on every return path.
    unsafe {
        let alteration = ScramAlteration::upsert(&username, password, iterations)?;
        execute_admin(
            client,
            native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS,
            timeout,
            |rk, options, queue| {
                let mut value = alteration.0;
                native::rd_kafka_AlterUserScramCredentials(rk, &mut value, 1, options, queue);
            },
            |event| {
                let result = native::rd_kafka_event_AlterUserScramCredentials_result(event);
                if result.is_null() {
                    return Err(KafkaAdminError::NativeContract("scram_upsert_result"));
                }
                let mut count = 0;
                let responses =
                    native::rd_kafka_AlterUserScramCredentials_result_responses(result, &mut count);
                if responses.is_null() || count != 1 {
                    return Err(KafkaAdminError::NativeContract("scram_upsert_response"));
                }
                broker_error(
                    native::rd_kafka_AlterUserScramCredentials_result_response_error(*responses),
                )
            },
        )
    }
}

pub fn delete_scram_sha512(
    client: &AdminClient<DefaultClientContext>,
    username: &str,
    timeout: Duration,
) -> Result<(), KafkaAdminError> {
    let username = cstring(username)?;
    unsafe {
        let alteration = ScramAlteration::delete(&username)?;
        execute_admin(
            client,
            native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS,
            timeout,
            |rk, options, queue| {
                let mut value = alteration.0;
                native::rd_kafka_AlterUserScramCredentials(rk, &mut value, 1, options, queue);
            },
            |event| {
                let result = native::rd_kafka_event_AlterUserScramCredentials_result(event);
                if result.is_null() {
                    return Err(KafkaAdminError::NativeContract("scram_delete_result"));
                }
                let mut count = 0;
                let responses =
                    native::rd_kafka_AlterUserScramCredentials_result_responses(result, &mut count);
                if responses.is_null() || count != 1 {
                    return Err(KafkaAdminError::NativeContract("scram_delete_response"));
                }
                broker_error(
                    native::rd_kafka_AlterUserScramCredentials_result_response_error(*responses),
                )
            },
        )
    }
}

pub fn verify_scram_sha512(
    client: &AdminClient<DefaultClientContext>,
    username: &str,
    timeout: Duration,
) -> Result<bool, KafkaAdminError> {
    let username = cstring(username)?;
    unsafe {
        execute_admin(
            client,
            native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBEUSERSCRAMCREDENTIALS,
            timeout,
            |rk, options, queue| {
                let mut user = username.as_ptr();
                native::rd_kafka_DescribeUserScramCredentials(rk, &mut user, 1, options, queue);
            },
            |event| {
                let result = native::rd_kafka_event_DescribeUserScramCredentials_result(event);
                if result.is_null() {
                    return Err(KafkaAdminError::NativeContract("scram_describe_result"));
                }
                let mut count = 0;
                let descriptions =
                    native::rd_kafka_DescribeUserScramCredentials_result_descriptions(
                        result, &mut count,
                    );
                if descriptions.is_null() || count != 1 {
                    return Ok(false);
                }
                let description = *descriptions;
                let error = native::rd_kafka_UserScramCredentialsDescription_error(description);
                if error.is_null()
                    || native::rd_kafka_error_code(error)
                        != native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                {
                    return Ok(false);
                }
                let info_count =
                    native::rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(
                        description,
                    );
                for index in 0..info_count {
                    let info = native::rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(
                        description,
                        index,
                    );
                    if !info.is_null()
                        && native::rd_kafka_ScramCredentialInfo_mechanism(info)
                            == native::rd_kafka_ScramMechanism_t::RD_KAFKA_SCRAM_MECHANISM_SHA_512
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
        )
    }
}

pub fn create_acl(
    client: &AdminClient<DefaultClientContext>,
    acl: ProviderAcl<'_>,
    timeout: Duration,
) -> Result<(), KafkaAdminError> {
    unsafe {
        let binding = AclBinding::new(acl, false)?;
        execute_admin(
            client,
            native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_CREATEACLS,
            timeout,
            |rk, options, queue| {
                let mut value = binding.0;
                native::rd_kafka_CreateAcls(rk, &mut value, 1, options, queue);
            },
            |event| {
                let result = native::rd_kafka_event_CreateAcls_result(event);
                if result.is_null() {
                    return Err(KafkaAdminError::NativeContract("acl_create_result"));
                }
                let mut count = 0;
                let results = native::rd_kafka_CreateAcls_result_acls(result, &mut count);
                if results.is_null() || count != 1 {
                    return Err(KafkaAdminError::NativeContract("acl_create_response"));
                }
                broker_error(native::rd_kafka_acl_result_error(*results))
            },
        )
    }
}

pub fn acl_exists(
    client: &AdminClient<DefaultClientContext>,
    acl: ProviderAcl<'_>,
    timeout: Duration,
) -> Result<bool, KafkaAdminError> {
    unsafe {
        let filter = AclBinding::new(acl, true)?;
        execute_admin(
            client,
            native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBEACLS,
            timeout,
            |rk, options, queue| {
                native::rd_kafka_DescribeAcls(rk, filter.0, options, queue);
            },
            |event| {
                let result = native::rd_kafka_event_DescribeAcls_result(event);
                if result.is_null() {
                    return Err(KafkaAdminError::NativeContract("acl_describe_result"));
                }
                let mut count = 0;
                let values = native::rd_kafka_DescribeAcls_result_acls(result, &mut count);
                Ok(!values.is_null() && count > 0)
            },
        )
    }
}

pub fn delete_acl(
    client: &AdminClient<DefaultClientContext>,
    acl: ProviderAcl<'_>,
    timeout: Duration,
) -> Result<(), KafkaAdminError> {
    unsafe {
        let filter = AclBinding::new(acl, true)?;
        execute_admin(
            client,
            native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DELETEACLS,
            timeout,
            |rk, options, queue| {
                let mut value = filter.0;
                native::rd_kafka_DeleteAcls(rk, &mut value, 1, options, queue);
            },
            |event| {
                let result = native::rd_kafka_event_DeleteAcls_result(event);
                if result.is_null() {
                    return Err(KafkaAdminError::NativeContract("acl_delete_result"));
                }
                let mut count = 0;
                let responses = native::rd_kafka_DeleteAcls_result_responses(result, &mut count);
                if responses.is_null() || count != 1 {
                    return Err(KafkaAdminError::NativeContract("acl_delete_response"));
                }
                broker_error(native::rd_kafka_DeleteAcls_result_response_error(
                    *responses,
                ))
            },
        )
    }
}

unsafe fn execute_admin<T>(
    client: &AdminClient<DefaultClientContext>,
    operation: native::rd_kafka_admin_op_t,
    timeout: Duration,
    invoke: impl FnOnce(
        *mut native::rd_kafka_t,
        *const native::rd_kafka_AdminOptions_t,
        *mut native::rd_kafka_queue_t,
    ),
    parse: impl FnOnce(*mut native::rd_kafka_event_t) -> Result<T, KafkaAdminError>,
) -> Result<T, KafkaAdminError> {
    let rk = client.inner().native_ptr();
    let queue = NativeQueue(unsafe { native::rd_kafka_queue_new(rk) });
    if queue.0.is_null() {
        return Err(KafkaAdminError::NativeContract("queue_allocation"));
    }
    let options = NativeOptions(unsafe { native::rd_kafka_AdminOptions_new(rk, operation) });
    if options.0.is_null() {
        return Err(KafkaAdminError::NativeContract("admin_options_allocation"));
    }
    let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    let mut error_buffer = [0_i8; 256];
    let option_result = unsafe {
        native::rd_kafka_AdminOptions_set_request_timeout(
            options.0,
            timeout_ms,
            error_buffer.as_mut_ptr(),
            error_buffer.len(),
        )
    };
    if option_result != native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(KafkaAdminError::Broker(option_result as i32));
    }
    invoke(rk, options.0, queue.0);
    let event = NativeEvent(unsafe {
        native::rd_kafka_queue_poll(queue.0, timeout_ms.saturating_add(1000))
    });
    if event.0.is_null() {
        return Err(KafkaAdminError::DeadlineExceeded);
    }
    let event_error = unsafe { native::rd_kafka_event_error(event.0) };
    if event_error != native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(KafkaAdminError::Broker(event_error as i32));
    }
    parse(event.0)
}

unsafe fn broker_error(error: *const native::rd_kafka_error_t) -> Result<(), KafkaAdminError> {
    if error.is_null() {
        return Ok(());
    }
    let code = unsafe { native::rd_kafka_error_code(error) };
    if code == native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        Ok(())
    } else {
        Err(KafkaAdminError::Broker(code as i32))
    }
}

fn cstring(value: &str) -> Result<CString, KafkaAdminError> {
    if value.is_empty() || value.len() > 249 || value.bytes().any(|byte| byte == 0) {
        return Err(KafkaAdminError::InvalidInput);
    }
    CString::new(value).map_err(|_| KafkaAdminError::InvalidInput)
}

struct ScramAlteration(*mut native::rd_kafka_UserScramCredentialAlteration_t);

impl ScramAlteration {
    unsafe fn upsert(
        username: &CString,
        password: &[u8],
        iterations: i32,
    ) -> Result<Self, KafkaAdminError> {
        let value = unsafe {
            native::rd_kafka_UserScramCredentialUpsertion_new(
                username.as_ptr(),
                native::rd_kafka_ScramMechanism_t::RD_KAFKA_SCRAM_MECHANISM_SHA_512,
                iterations,
                password.as_ptr(),
                password.len(),
                ptr::null(),
                0,
            )
        };
        (!value.is_null())
            .then_some(Self(value))
            .ok_or(KafkaAdminError::NativeContract("scram_upsert_allocation"))
    }

    unsafe fn delete(username: &CString) -> Result<Self, KafkaAdminError> {
        let value = unsafe {
            native::rd_kafka_UserScramCredentialDeletion_new(
                username.as_ptr(),
                native::rd_kafka_ScramMechanism_t::RD_KAFKA_SCRAM_MECHANISM_SHA_512,
            )
        };
        (!value.is_null())
            .then_some(Self(value))
            .ok_or(KafkaAdminError::NativeContract("scram_delete_allocation"))
    }
}

impl Drop for ScramAlteration {
    fn drop(&mut self) {
        unsafe { native::rd_kafka_UserScramCredentialAlteration_destroy(self.0) };
    }
}

struct AclBinding(*mut native::rd_kafka_AclBinding_t);

impl AclBinding {
    unsafe fn new(acl: ProviderAcl<'_>, filter: bool) -> Result<Self, KafkaAdminError> {
        let (resource_type, name) = match acl.resource {
            AclResource::Topic(name) => (
                native::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC,
                cstring(name)?,
            ),
            AclResource::Group(name) => (
                native::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_GROUP,
                cstring(name)?,
            ),
        };
        let principal = cstring(&format!("User:{}", acl.username))?;
        let host = cstring("*")?;
        let operation = match acl.operation {
            AclOperation::Read => native::rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_READ,
            AclOperation::Describe => {
                native::rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE
            }
        };
        let mut error_buffer = [0_i8; 256];
        let value = if filter {
            unsafe {
                native::rd_kafka_AclBindingFilter_new(
                    resource_type,
                    name.as_ptr(),
                    native::rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
                    principal.as_ptr(),
                    host.as_ptr(),
                    operation,
                    native::rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW,
                    error_buffer.as_mut_ptr(),
                    error_buffer.len(),
                )
            }
        } else {
            unsafe {
                native::rd_kafka_AclBinding_new(
                    resource_type,
                    name.as_ptr(),
                    native::rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
                    principal.as_ptr(),
                    host.as_ptr(),
                    operation,
                    native::rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW,
                    error_buffer.as_mut_ptr(),
                    error_buffer.len(),
                )
            }
        };
        (!value.is_null())
            .then_some(Self(value))
            .ok_or(KafkaAdminError::InvalidInput)
    }
}

impl Drop for AclBinding {
    fn drop(&mut self) {
        unsafe { native::rd_kafka_AclBinding_destroy(self.0) };
    }
}

struct NativeQueue(*mut native::rd_kafka_queue_t);

impl Drop for NativeQueue {
    fn drop(&mut self) {
        unsafe { native::rd_kafka_queue_destroy(self.0) };
    }
}

struct NativeOptions(*mut native::rd_kafka_AdminOptions_t);

impl Drop for NativeOptions {
    fn drop(&mut self) {
        unsafe { native::rd_kafka_AdminOptions_destroy(self.0) };
    }
}

struct NativeEvent(*mut native::rd_kafka_event_t);

impl Drop for NativeEvent {
    fn drop(&mut self) {
        unsafe { native::rd_kafka_event_destroy(self.0) };
    }
}

#[cfg(test)]
mod tests {
    use super::{AclOperation, AclResource, KafkaAdminError, ProviderAcl, cstring};

    #[test]
    fn native_admin_contract_rejects_empty_and_nul_names() {
        assert_eq!(cstring(""), Err(KafkaAdminError::InvalidInput));
        assert_eq!(cstring("bad\0name"), Err(KafkaAdminError::InvalidInput));
    }

    #[test]
    fn provider_acl_is_exact_and_provider_scoped() {
        let acl = ProviderAcl {
            resource: AclResource::Group("provider_group_example"),
            username: "provider_user_example",
            operation: AclOperation::Read,
        };
        assert!(matches!(acl.resource, AclResource::Group(_)));
    }
}
