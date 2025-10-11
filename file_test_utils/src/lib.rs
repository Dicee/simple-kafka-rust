use std::env;
use std::fs;
use std::fs::read_to_string;
use uuid::Uuid;
use assertor::{assert_that, VecAssertion};
use walkdir::WalkDir;

/// Simplifies the creation, management and state assertions for temporary directories specifically used in tests.
pub struct TempTestDir {
    path: String,
}

/// Simplifies the creation, management and state assertions for temporary files specifically used in tests.
pub struct TempTestFile {
    test_dir: TempTestDir,
    path: String,
}

impl TempTestDir {
    pub fn create() -> TempTestDir {
        let unit_tests_dir = get_unit_tests_dir();
        // allows clean isolation between tests, since by default Cargo runs them in parallel
        let temp_dir = Uuid::new_v4().to_string();
        let path = format!("{unit_tests_dir}/{temp_dir}");

        fs::create_dir_all(&path).unwrap_or_else(|_| panic!("Unable to create test directory"));
        TempTestDir { path }
    }

    pub fn assert_exactly_contains_files(&self, expected_files_in_order: &Vec<String>) {
        let files: Vec<String> = WalkDir::new(&self.path)
            .sort_by_file_name()
            .into_iter()
            .map(|e| e.unwrap())
            .filter(|e| e.metadata().unwrap().is_file())
            .map(|e| e.path().to_str().unwrap().to_string())
            .map(|absolute_path| absolute_path.replace(&ensure_trailing_slash(&self.path), ""))
            .collect();

        assert_that!(files).contains_exactly_in_order(expected_files_in_order);
    }

    pub fn path(&self) -> &str { self.path.as_str() }
}

impl TempTestFile {
    pub fn create_with_content(content: &str) -> TempTestFile {
        let mut temp_file = TempTestFile::create();
        temp_file.set_content(content);
        temp_file
    }

    pub fn create() -> TempTestFile { Self::create_within("") }

    pub fn create_within(sub_dir: &str) -> TempTestFile {
        let test_dir = TempTestDir::create();
        let file_name = Uuid::new_v4().to_string();
        let sub_dir = ensure_trailing_slash(sub_dir);
        let full_path = format!("{}/{sub_dir}{file_name}", test_dir.path);

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
        assert_file_has_content(&self.path, content);
    }

    pub fn test_dir(&self) -> &TempTestDir { &self.test_dir }

    pub fn path(&self) -> &str { self.path.as_str() }
}

impl Drop for TempTestDir {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_dir_all(&self.path) {
            panic!("Failed to delete test directory: {}: {e:?}", self.path);
        }
    }
}

pub fn assert_file_has_content(path: &str, content: &str) {
    assert_eq!(read_to_string(path).unwrap_or_else(|_| panic!("Failed to read content as string for path {path}")), content)
}

fn get_unit_tests_dir() -> String {
    let temp_dir = env::temp_dir();
    let temp_dir = temp_dir.to_str().expect("Temp directory not found");
    format!("{temp_dir}/rust_unit_tests")
}

fn ensure_trailing_slash(s: &str) -> String {
    let mut s = s.to_string();
    if s.is_empty() { return s; }
    s.push('/');
    s
}