use serde_json::Value;

/// Recursively rename a field in a JSON document, including nested object arrays
pub fn rename_nested_field(doc: &mut Value, old_field_path: &[&str], new_field: &str) -> bool {
    if old_field_path.is_empty() {
        return false; // Invalid path
    }

    let (current_key, remaining_path) = old_field_path.split_first().unwrap();

    match doc {
        Value::Object(obj) => {
            if let Some(value) = obj.get_mut(*current_key) {
                if remaining_path.is_empty() {
                    // Base case: Rename the field
                    if let Some(value) = obj.remove(*current_key) {
                        // split the new_field into components
                        let new_field_path: Vec<&str> = new_field.split('.').collect();
                        // use last element as the new field name
                        let new_field = new_field_path.last().unwrap();

                        obj.insert(new_field.to_string(), value);
                        return true;
                    }
                } else {
                    // Recursive case: Traverse deeper
                    return rename_nested_field(value, remaining_path, new_field);
                }
            }
        }
        Value::Array(arr) => {
            // Process each element in the array recursively
            let mut renamed = false;
            for item in arr {
                renamed |= rename_nested_field(item, old_field_path, new_field);
            }
            return renamed;
        }
        _ => {}
    }

    false
}

/// Unit tests for the application
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_rename_nested_field_simple_object() {
        let mut doc = json!({
            "a": {
                "b": 1
            }
        });

        let old_field_path = vec!["a", "b"];
        let new_field = "new_b";

        let result = rename_nested_field(&mut doc, &old_field_path, new_field);

        assert!(result, "Field renaming should succeed");
        assert_eq!(
            doc,
            json!({
                "a": {
                    "new_b": 1
                }
            }),
            "Field 'b' should be renamed to 'new_b'"
        );
    }

    #[test]
    fn test_rename_nested_field_nested_object() {
        let mut doc = json!({
            "a": {
                "b": {
                    "c": 2
                }
            }
        });

        let old_field_path = vec!["a", "b", "c"];
        let new_field = "new_c";

        let result = rename_nested_field(&mut doc, &old_field_path, new_field);

        assert!(result, "Field renaming should succeed");
        assert_eq!(
            doc,
            json!({
                "a": {
                    "b": {
                        "new_c": 2
                    }
                }
            }),
            "Field 'c' should be renamed to 'new_c'"
        );
    }

    #[test]
    fn test_rename_nested_field_array_of_objects() {
        let mut doc = json!({
            "a": {
                "b": [
                    { "c": 1 },
                    { "c": 2 }
                ]
            }
        });

        let old_field_path = vec!["a", "b", "c"];
        let new_field = "new_c";

        let result = rename_nested_field(&mut doc, &old_field_path, new_field);

        assert!(result, "Field renaming should succeed");
        assert_eq!(
            doc,
            json!({
                "a": {
                    "b": [
                        { "new_c": 1 },
                        { "new_c": 2 }
                    ]
                }
            }),
            "Fields 'c' in array elements should be renamed to 'new_c'"
        );
    }

    #[test]
    fn test_rename_nested_field_nonexistent_field() {
        let mut doc = json!({
            "a": {
                "b": 1
            }
        });

        let old_field_path = vec!["a", "x"];
        let new_field = "new_x";

        let result = rename_nested_field(&mut doc, &old_field_path, new_field);

        assert!(!result, "Field renaming should fail for nonexistent fields");
        assert_eq!(
            doc,
            json!({
                "a": {
                    "b": 1
                }
            }),
            "Document should remain unchanged"
        );
    }

    #[test]
    fn test_rename_nested_field_invalid_path() {
        let mut doc = json!({
            "a": {
                "b": 1
            }
        });

        let old_field_path: Vec<&str> = vec![];
        let new_field = "new_b";

        let result = rename_nested_field(&mut doc, &old_field_path, new_field);

        assert!(!result, "Field renaming should fail for invalid paths");
        assert_eq!(
            doc,
            json!({
                "a": {
                    "b": 1
                }
            }),
            "Document should remain unchanged"
        );
    }
}