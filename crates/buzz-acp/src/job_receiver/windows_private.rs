//! Owner-only Windows filesystem primitives for the durable A2A ledger.

use std::ffi::c_void;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::os::windows::io::AsRawHandle as _;
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};

use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, HANDLE};
use windows_sys::Win32::Security::Authorization::{SetNamedSecurityInfoW, SE_FILE_OBJECT};
use windows_sys::Win32::Security::{
    AclSizeInformation, AddAccessAllowedAceEx, CreateWellKnownSid, EqualSid, GetAce,
    GetAclInformation, GetFileSecurityW, GetLengthSid, GetSecurityDescriptorControl,
    GetSecurityDescriptorDacl, GetSecurityDescriptorOwner, GetTokenInformation, InitializeAcl,
    TokenUser, WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, ACL_SIZE_INFORMATION,
    CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, OBJECT_INHERIT_ACE,
    OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSID, SECURITY_MAX_SID_SIZE,
    SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::Storage::FileSystem::{
    GetFileInformationByHandle, MoveFileExW, ReplaceFileW, BY_HANDLE_FILE_INFORMATION,
    FILE_ALL_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_OPEN_REPARSE_POINT,
    MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, REPLACEFILE_WRITE_THROUGH,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FileIdentity {
    pub(super) volume: u32,
    pub(super) index: u64,
}

struct OwnedHandle(HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}

fn aligned_buffer(bytes: usize) -> Vec<usize> {
    vec![0; bytes.div_ceil(std::mem::size_of::<usize>())]
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

fn owner_acl(user: PSID, system: PSID, directory: bool) -> io::Result<Vec<usize>> {
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

fn reject_reparse_components(path: &Path) -> io::Result<()> {
    let mut cursor = PathBuf::new();
    for component in path.components() {
        cursor.push(component.as_os_str());
        match std::fs::symlink_metadata(&cursor) {
            Ok(metadata) if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private state path must not contain Windows reparse points",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn verify_owner_acl(path: &Path, user: PSID, system: PSID) -> io::Result<()> {
    unsafe {
        let path = wide(path);
        let requested = OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
        let mut needed = 0;
        let _ = GetFileSecurityW(path.as_ptr(), requested, null_mut(), 0, &mut needed);
        if needed == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut descriptor = aligned_buffer(needed as usize);
        bool_result(GetFileSecurityW(
            path.as_ptr(),
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
                "private state owner does not match the current operator",
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
                "private state DACL is absent or inherits external permissions",
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
                "private state DACL contains an unexpected principal",
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
                    "private state DACL contains a non-owner access rule",
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
                    "private state DACL grants an unexpected principal",
                ));
            }
        }
        if !saw_user || !saw_system {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private state DACL is missing its operator or SYSTEM rule",
            ));
        }
        Ok(())
    }
}

fn secure_path(path: &Path, directory: bool) -> io::Result<()> {
    reject_reparse_components(path)?;
    let user = current_user_sid()?;
    let system = local_system_sid()?;
    let user_ptr = user.as_ptr().cast_mut().cast::<c_void>();
    let system_ptr = system.as_ptr().cast_mut().cast::<c_void>();
    let mut acl = owner_acl(user_ptr, system_ptr, directory)?;
    let wide_path = wide(path);
    let result = unsafe {
        SetNamedSecurityInfoW(
            wide_path.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION
                | DACL_SECURITY_INFORMATION
                | PROTECTED_DACL_SECURITY_INFORMATION,
            user_ptr,
            null_mut(),
            acl.as_mut_ptr().cast(),
            null(),
        )
    };
    if result != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    verify_owner_acl(path, user_ptr, system_ptr)
}

pub(super) fn secure_directory(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "private state root must be a real directory",
        ));
    }
    secure_path(path, true)
}

pub(super) fn create_private_new(path: &Path) -> io::Result<File> {
    reject_reparse_components(path.parent().unwrap_or(path))?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    if let Err(error) = secure_path(path, false) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(error);
    }
    Ok(file)
}

pub(super) fn open_private_read(path: &Path) -> io::Result<File> {
    verify_existing_private_file(path)?;
    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
}

pub(super) fn open_private_lock(path: &Path, create: bool) -> io::Result<(File, FileIdentity)> {
    reject_reparse_components(path.parent().unwrap_or(path))?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "private lock must be a real file",
        ));
    }
    secure_path(path, false)?;
    let identity = file_identity(&file)?;
    Ok((file, identity))
}

pub(super) fn replace_private_file(source: &Path, destination: &Path) -> io::Result<()> {
    reject_reparse_components(source)?;
    reject_reparse_components(destination)?;
    let source_wide = wide(source);
    let destination_wide = wide(destination);
    let replaced = unsafe {
        if destination.exists() {
            ReplaceFileW(
                destination_wide.as_ptr(),
                source_wide.as_ptr(),
                null(),
                REPLACEFILE_WRITE_THROUGH,
                null(),
                null(),
            )
        } else {
            MoveFileExW(
                source_wide.as_ptr(),
                destination_wide.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        }
    };
    bool_result(replaced)?;
    verify_existing_private_file(destination)
}

fn verify_existing_private_file(path: &Path) -> io::Result<()> {
    reject_reparse_components(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "private state entry must be a real file",
        ));
    }
    let user = current_user_sid()?;
    let system = local_system_sid()?;
    verify_owner_acl(
        path,
        user.as_ptr().cast_mut().cast(),
        system.as_ptr().cast_mut().cast(),
    )
}

fn file_identity(file: &File) -> io::Result<FileIdentity> {
    unsafe {
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        bool_result(GetFileInformationByHandle(
            file.as_raw_handle() as HANDLE,
            &mut info,
        ))?;
        Ok(FileIdentity {
            volume: info.dwVolumeSerialNumber,
            index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn private_files_are_reopenable_replaceable_and_identity_stable() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "buzz-windows-private-{}-{nonce}",
            std::process::id()
        ));
        secure_directory(&root).expect("secure private root");

        let destination = root.join("state.json");
        let mut initial = create_private_new(&destination).expect("create private state");
        initial.write_all(b"initial").expect("write initial");
        initial.sync_all().expect("sync initial");
        drop(initial);

        let (first_lock, first_identity) =
            open_private_lock(&root.join("state.lock"), true).expect("create private lock");
        drop(first_lock);
        let (second_lock, second_identity) =
            open_private_lock(&root.join("state.lock"), false).expect("reopen private lock");
        assert_eq!(first_identity, second_identity);
        drop(second_lock);

        let replacement = root.join("replacement.tmp");
        let mut next = create_private_new(&replacement).expect("create private replacement");
        next.write_all(b"replacement").expect("write replacement");
        next.sync_all().expect("sync replacement");
        drop(next);
        replace_private_file(&replacement, &destination).expect("replace private state");

        let mut reopened = open_private_read(&destination).expect("reopen private state");
        let mut value = String::new();
        reopened.read_to_string(&mut value).expect("read state");
        assert_eq!(value, "replacement");
        drop(reopened);

        std::fs::remove_dir_all(root).expect("remove private fixture");
    }
}
