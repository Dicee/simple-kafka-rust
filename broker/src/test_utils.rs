use std::env;
use std::fs;
use std::fs::read_to_string;
use uuid::Uuid;

/// Simplifies the creation, management and state assertions for temporary files specifically used in tests.
pub struct TempTestFile {
    test_dir: String,
    path: String,
}

impl TempTestFile {
    pub fn create_with_content(content: &str) -> TempTestFile {
        let mut temp_file = TempTestFile::create();
        temp_file.set_content(content);
        temp_file
    }

    pub fn create() -> TempTestFile { Self::create_within("") }
    
    pub fn create_within(sub_dir: &str) -> TempTestFile {
        let unit_tests_dir = get_unit_tests_dir();
        // allows clean isolation between tests, since by default Cargo runs them in parallel
        let temp_dir = Uuid::new_v4().to_string();
        let test_dir = format!("{unit_tests_dir}/{temp_dir}");

        fs::create_dir_all(&test_dir).expect("Unable to create test directory");

        let file_name = Uuid::new_v4().to_string();
        let sub_dir = ensure_trailing_slash(sub_dir);
        let full_path = format!("{test_dir}/{sub_dir}{file_name}");

        TempTestFile {
            test_dir,
            path: full_path
        }
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
        if let Err(e) = fs::remove_dir_all(&self.test_dir) {
            panic!("Failed to delete test directory: {}: {e:?}", self.test_dir);
        }
    }
}

fn get_unit_tests_dir() -> String {
    let temp_dir = env::temp_dir();
    let temp_dir = temp_dir.to_str().expect("Temp directory not found");
    format!("{temp_dir}/rust_unit_tests")
}

fn ensure_trailing_slash(s: &str) -> String {
    let mut s = s.to_string();
    if s.is_empty() { return s; }
    s.push_str("/");
    s
}