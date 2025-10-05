use std::env;
use uuid::Uuid;
use std::fs;
use std::fs::read_to_string;

/// Simplifies the creation, management and state assertions for temporary files specifically used in tests.
pub struct TempTestFile {
    path: String,
}

impl TempTestFile {
    pub fn create_with_content(content: &str) -> TempTestFile {
        let mut temp_file = TempTestFile::create();
        temp_file.set_content(content);
        temp_file
    }

    pub fn create() -> TempTestFile {
        let temp_dir = env::temp_dir();
        let temp_dir = temp_dir.to_str().expect("Temp directory not found");

        let unit_tests_dir = format!("{temp_dir}/rust_unit_tests");
        fs::create_dir_all(&unit_tests_dir).expect("Unable to create test directory");

        TempTestFile { path: format!("{unit_tests_dir}/{}", Uuid::new_v4().to_string()) }
    }

    pub fn set_content(&mut self, content: &str) {
        fs::write(&self.path, content).unwrap();
    }

    pub fn assert_exists(&self, exists: bool) {
        assert_eq!(fs::exists(&self.path).unwrap(), exists);
    }

    pub fn assert_has_content(&self, content: &str) {
        assert_eq!(read_to_string(&self.path).unwrap(), content)
    }

    pub fn path(&self) -> &str { self.path.as_str() }
}

impl Drop for TempTestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}