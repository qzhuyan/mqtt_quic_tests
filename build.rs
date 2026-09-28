use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=scenarios");
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("scenarios");
    let mut entries = fs::read_dir(&root)
        .expect("scenario directory")
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "rhai")
        })
        .collect::<Vec<_>>();
    entries.sort();
    let mut source = String::from("static BUNDLED: &[(&str, &str)] = &[\n");
    for path in entries {
        let name = path
            .file_stem()
            .unwrap()
            .to_str()
            .expect("UTF-8 scenario name");
        source.push_str(&format!("({name:?}, include_str!({path:?})),\n"));
    }
    source.push_str("];\n");
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("scenarios.rs"),
        source,
    )
    .unwrap();
}
