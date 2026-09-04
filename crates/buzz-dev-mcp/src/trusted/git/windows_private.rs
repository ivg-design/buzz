//! Owner-only Windows primitives for temporary trusted Git state.

#![allow(unsafe_code)]

use std::ffi::c_void;
use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};

use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, HANDLE};
use windows_sys::Win32::Security::Authorization::{SetNamedSecurityInfoW, SE_FILE_OBJECT};
use windows_sys::Win32::Security::{
    AclSizeInformation, AddAccessAllowedAceEx, CreateWellKnownSid, EqualSid, GetAce,
    GetAclInformation, GetFileSecurityW, GetLengthSid, GetSecurityDescriptorControl,
    GetSecurityDescriptorDacl, GetSecurityDescriptorOwner, GetTokenInformation, InitializeAcl,
    InitializeSecurityDescriptor, SetSecurityDescriptorControl, SetSecurityDescriptorDacl,
    SetSecurityDescriptorOwner, TokenUser, WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACL,
    ACL_REVISION, ACL_SIZE_INFORMATION, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION,
    OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSID,
    SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR, SECURITY_MAX_SID_SIZE, SE_DACL_PROTECTED,
    TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::Storage::FileSystem::{FILE_ALL_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

const SECURITY_DESCRIPTOR_REVISION: u32 = 1;

struct OwnedHandle(HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}

struct OwnerAcl {
    user: Vec<usize>,
    system: Vec<usize>,
    acl: Vec<usize>,
}

impl OwnerAcl {
    fn new(directory: bool) -> io::Result<Self> {
        let user = current_user_sid()?;
        let system = local_system_sid()?;
        let user_ptr = sid_ptr(&user);
        let system_ptr = sid_ptr(&system);
        let acl = access_acl(user_ptr, system_ptr, directory)?;
        Ok(Self { user, system, acl })
    }

    fn user(&self) -> PSID {
        sid_ptr(&self.user)
    }

    fn system(&self) -> PSID {
        sid_ptr(&self.system)
    }

    fn acl(&mut self) -> *mut ACL {
        self.acl.as_mut_ptr().cast()
    }
}

/// Keeps every buffer referenced by a Win32 `SECURITY_ATTRIBUTES` value alive.
///
/// The pointer is valid only while this value remains alive and has not been
/// moved by the caller. `as_mut_ptr` refreshes the descriptor pointer after a
/// move and is intended for one synchronous named-pipe creation call.
pub(super) struct OwnerOnlySecurityAttributes {
    _owner_acl: OwnerAcl,
    descriptor: Box<SECURITY_DESCRIPTOR>,
    attributes: SECURITY_ATTRIBUTES,
}

impl OwnerOnlySecurityAttributes {
    pub(super) fn new() -> Result<Self, String> {
        Self::new_inner().map_err(|_| "failed to prepare private named-pipe security".to_owned())
    }

    fn new_inner() -> io::Result<Self> {
        let mut owner_acl = OwnerAcl::new(false)?;
        let mut descriptor = Box::new(SECURITY_DESCRIPTOR::default());
        let descriptor_ptr = (&mut *descriptor as *mut SECURITY_DESCRIPTOR).cast::<c_void>();
        unsafe {
            bool_result(InitializeSecurityDescriptor(
                descriptor_ptr,
                SECURITY_DESCRIPTOR_REVISION,
            ))?;
            bool_result(SetSecurityDescriptorOwner(
                descriptor_ptr,
                owner_acl.user(),
                0,
            ))?;
            bool_result(SetSecurityDescriptorDacl(
                descriptor_ptr,
                1,
                owner_acl.acl(),
                0,
            ))?;
            bool_result(SetSecurityDescriptorControl(
                descriptor_ptr,
                SE_DACL_PROTECTED,
                SE_DACL_PROTECTED,
            ))?;
        }
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor_ptr,
            bInheritHandle: 0,
        };
        Ok(Self {
            _owner_acl: owner_acl,
            descriptor,
            attributes,
        })
    }

    pub(super) fn as_mut_ptr(&mut self) -> *mut SECURITY_ATTRIBUTES {
        self.attributes.lpSecurityDescriptor =
            (&mut *self.descriptor as *mut SECURITY_DESCRIPTOR).cast::<c_void>();
        &mut self.attributes
    }
}

pub(super) fn private_tempdir(prefix: &str) -> Result<tempfile::TempDir, String> {
    let directory = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .map_err(|_| "failed to create private Git directory".to_owned())?;
    secure_private_directory(directory.path())?;
    Ok(directory)
}

pub(super) fn secure_private_directory(path: &Path) -> Result<(), String> {
    std::fs::create_dir_all(path)
        .map_err(|_| "failed to create private Git directory".to_owned())?;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| "failed to inspect private Git directory".to_owned())?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err("private Git directory must not be a Windows reparse point".into());
    }
    secure_path(path, true).map_err(|_| "failed to secure private Git directory".to_owned())
}

pub(super) fn secure_private_file(path: &Path) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| "failed to inspect private Git file".to_owned())?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err("private Git state must be a real file".into());
    }
    secure_path(path, false).map_err(|_| "failed to secure private Git file".to_owned())
}

fn aligned_buffer(bytes: usize) -> Vec<usize> {
    vec![0; bytes.div_ceil(std::mem::size_of::<usize>())]
}

fn sid_ptr(storage: &[usize]) -> PSID {
    storage.as_ptr().cast_mut().cast()
}

fn wide(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

fn bool_result(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn current_user_sid() -> io::Result<Vec<usize>> {
    unsafe {
        let mut token = null_mut();
        bool_result(OpenProcessToken(
            GetCurrentProcess(),
            TOKEN_QUERY,
            &mut token,
        ))?;
        let _token = OwnedHandle(token);
        let mut needed = 0;
        let _ = GetTokenInformation(token, TokenUser, null_mut(), 0, &mut needed);
        if needed == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut token_info = aligned_buffer(needed as usize);
        bool_result(GetTokenInformation(
            token,
            TokenUser,
            token_info.as_mut_ptr().cast(),
            needed,
            &mut needed,
        ))?;
        let sid = (*(token_info.as_ptr().cast::<TOKEN_USER>())).User.Sid;
        let sid_len = GetLengthSid(sid);
        if sid_len == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut copy = aligned_buffer(sid_len as usize);
        std::ptr::copy_nonoverlapping(
            sid.cast::<u8>(),
            copy.as_mut_ptr().cast::<u8>(),
            sid_len as usize,
        );
        Ok(copy)
    }
}

fn local_system_sid() -> io::Result<Vec<usize>> {
    unsafe {
        let mut sid = aligned_buffer(SECURITY_MAX_SID_SIZE as usize);
        let mut len = SECURITY_MAX_SID_SIZE;
        bool_result(CreateWellKnownSid(
            WinLocalSystemSid,
            null_mut(),
            sid.as_mut_ptr().cast(),
            &mut len,
        ))?;
        Ok(sid)
    }
}

fn access_acl(user: PSID, system: PSID, directory: bool) -> io::Result<Vec<usize>> {
    unsafe {
        let user_len = GetLengthSid(user) as usize;
        let system_len = GetLengthSid(system) as usize;
        if user_len == 0 || system_len == 0 {
            return Err(io::Error::last_os_error());
        }
        let ace_base = std::mem::size_of::<ACCESS_ALLOWED_ACE>() - std::mem::size_of::<u32>();
        let bytes = std::mem::size_of::<ACL>() + ace_base * 2 + user_len + system_len;
        let mut storage = aligned_buffer(bytes);
        let acl = storage.as_mut_ptr().cast::<ACL>();
        bool_result(InitializeAcl(acl, bytes as u32, ACL_REVISION))?;
        let flags = if directory {
            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
        } else {
            0
        };
        bool_result(AddAccessAllowedAceEx(
            acl,
            ACL_REVISION,
            flags,
            FILE_ALL_ACCESS,
            user,
        ))?;
        bool_result(AddAccessAllowedAceEx(
            acl,
            ACL_REVISION,
            flags,
            FILE_ALL_ACCESS,
            system,
        ))?;
        Ok(storage)
    }
}

fn reject_reparse_ancestors(path: &Path) -> io::Result<()> {
    let mut cursor = PathBuf::new();
    for component in path.components() {
        cursor.push(component.as_os_str());
        match std::fs::symlink_metadata(&cursor) {
            Ok(metadata) if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private Git path contains a Windows reparse point",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn secure_path(path: &Path, directory: bool) -> io::Result<()> {
    reject_reparse_ancestors(path)?;
    let mut owner_acl = OwnerAcl::new(directory)?;
    let path_wide = wide(path);
    let result = unsafe {
        SetNamedSecurityInfoW(
            path_wide.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION
                | DACL_SECURITY_INFORMATION
                | PROTECTED_DACL_SECURITY_INFORMATION,
            owner_acl.user(),
            null_mut(),
            owner_acl.acl(),
            null(),
        )
    };
    if result != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    verify_owner_acl(path, owner_acl.user(), owner_acl.system())
}

fn verify_owner_acl(path: &Path, user: PSID, system: PSID) -> io::Result<()> {
    unsafe {
        let path_wide = wide(path);
        let requested = OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
        let mut needed = 0;
        let _ = GetFileSecurityW(path_wide.as_ptr(), requested, null_mut(), 0, &mut needed);
        if needed == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut descriptor = aligned_buffer(needed as usize);
        bool_result(GetFileSecurityW(
            path_wide.as_ptr(),
            requested,
            descriptor.as_mut_ptr().cast(),
            needed,
            &mut needed,
        ))?;
        let descriptor_ptr = descriptor.as_mut_ptr().cast::<c_void>();
        let mut owner = null_mut();
        let mut defaulted = 0;
        bool_result(GetSecurityDescriptorOwner(
            descriptor_ptr,
            &mut owner,
            &mut defaulted,
        ))?;
        if owner.is_null() || EqualSid(owner, user) == 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private Git owner does not match the current operator",
            ));
        }
        let mut dacl_present = 0;
        let mut dacl = null_mut();
        bool_result(GetSecurityDescriptorDacl(
            descriptor_ptr,
            &mut dacl_present,
            &mut dacl,
            &mut defaulted,
        ))?;
        let mut control = 0;
        let mut revision = 0;
        bool_result(GetSecurityDescriptorControl(
            descriptor_ptr,
            &mut control,
            &mut revision,
        ))?;
        if dacl_present == 0 || dacl.is_null() || control & SE_DACL_PROTECTED == 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private Git DACL is absent or inherits external permissions",
            ));
        }
        let mut info = ACL_SIZE_INFORMATION::default();
        bool_result(GetAclInformation(
            dacl,
            (&mut info as *mut ACL_SIZE_INFORMATION).cast(),
            std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        ))?;
        if info.AceCount != 2 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private Git DACL contains an unexpected principal",
            ));
        }
        let mut saw_user = false;
        let mut saw_system = false;
        for index in 0..info.AceCount {
            let mut raw = null_mut();
            bool_result(GetAce(dacl, index, &mut raw))?;
            let ace = &*(raw.cast::<ACCESS_ALLOWED_ACE>());
            if ace.Header.AceType != 0 || ace.Mask & FILE_ALL_ACCESS != FILE_ALL_ACCESS {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "private Git DACL contains a non-owner access rule",
                ));
            }
            let sid = (&ace.SidStart as *const u32).cast_mut().cast::<c_void>();
            if EqualSid(sid, user) != 0 {
                saw_user = true;
            } else if EqualSid(sid, system) != 0 {
                saw_system = true;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "private Git DACL grants an unexpected principal",
                ));
            }
        }
        if !saw_user || !saw_system {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private Git DACL is missing its operator or SYSTEM rule",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_directory_file_and_pipe_descriptor_are_owner_only() {
        let directory = private_tempdir("buzz-windows-private-test-").expect("private tempdir");
        let file = directory.path().join("config");
        std::fs::write(&file, b"fixture").expect("write fixture");
        secure_private_file(&file).expect("secure fixture");
        let mut attributes =
            OwnerOnlySecurityAttributes::new().expect("owner-only pipe security attributes");
        assert!(!attributes.as_mut_ptr().is_null());
    }
}
