use apitap::writer::WriteMode;

#[test]
fn test_write_mode_merge() {
    let mode = WriteMode::Merge;
    assert!(matches!(mode, WriteMode::Merge));
}

#[test]
fn test_write_mode_append() {
    let mode = WriteMode::Append;
    assert!(matches!(mode, WriteMode::Append));
}

#[test]
fn test_write_mode_clone() {
    let mode = WriteMode::Merge;
    let cloned = mode.clone();

    assert_eq!(mode, cloned);
}

#[test]
fn test_write_mode_equality() {
    let mode1 = WriteMode::Merge;
    let mode2 = WriteMode::Merge;
    let mode3 = WriteMode::Append;

    assert_eq!(mode1, mode2);
    assert_ne!(mode1, mode3);
    assert_ne!(mode2, mode3);
}

#[test]
fn test_write_mode_debug() {
    let merge = WriteMode::Merge;
    let append = WriteMode::Append;

    let merge_str = format!("{:?}", merge);
    let append_str = format!("{:?}", append);

    assert!(merge_str.contains("Merge"));
    assert!(append_str.contains("Append"));
}

#[test]
fn test_write_mode_partial_eq() {
    assert_eq!(WriteMode::Merge, WriteMode::Merge);
    assert_eq!(WriteMode::Append, WriteMode::Append);
    assert_ne!(WriteMode::Merge, WriteMode::Append);
}

#[test]
fn test_write_mode_match_patterns() {
    let mode = WriteMode::Merge;

    let result = match mode {
        WriteMode::Merge => "merge_operation",
        WriteMode::Append => "append_operation",
    };

    assert_eq!(result, "merge_operation");
}

#[test]
fn test_write_mode_in_vec() {
    let modes = [WriteMode::Merge, WriteMode::Append, WriteMode::Merge];

    assert_eq!(modes.len(), 3);
    assert_eq!(modes[0], WriteMode::Merge);
    assert_eq!(modes[1], WriteMode::Append);
    assert_eq!(modes[2], WriteMode::Merge);
}

#[test]
fn test_write_mode_clone_independence() {
    let original = WriteMode::Merge;
    let cloned = original.clone();

    // Both should be equal
    assert_eq!(original, cloned);

    // Verify they're logically the same
    match (original, cloned) {
        (WriteMode::Merge, WriteMode::Merge) => {}
        _ => panic!("Clone should preserve variant"),
    }
}

#[test]
fn test_write_mode_as_function_parameter() {
    fn process_write_mode(mode: WriteMode) -> &'static str {
        match mode {
            WriteMode::Merge => "merging",
            WriteMode::Append => "appending",
        }
    }

    assert_eq!(process_write_mode(WriteMode::Merge), "merging");
    assert_eq!(process_write_mode(WriteMode::Append), "appending");
}

#[test]
fn test_write_mode_in_option() {
    let some_mode: Option<WriteMode> = Some(WriteMode::Merge);
    let none_mode: Option<WriteMode> = None;

    assert!(some_mode.is_some());
    assert!(none_mode.is_none());

    if let Some(mode) = some_mode {
        assert_eq!(mode, WriteMode::Merge);
    }
}

#[test]
fn test_write_mode_in_result() {
    let ok_mode: Result<WriteMode, String> = Ok(WriteMode::Append);
    let err_mode: Result<WriteMode, String> = Err("error".to_string());

    assert!(ok_mode.is_ok());
    assert!(err_mode.is_err());

    if let Ok(mode) = ok_mode {
        assert_eq!(mode, WriteMode::Append);
    } else {
        panic!("Expected Ok value");
    }
}
