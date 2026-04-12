use std::sync::Mutex;

use tauri::Manager;
use tauri_plugin_shell::process::CommandChild;
use tauri_plugin_shell::ShellExt;

pub struct BackendChild(pub Mutex<Option<CommandChild>>);

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .setup(|app| {
            let data_dir = app.path().app_data_dir().map_err(|error| error.to_string())?;
            std::fs::create_dir_all(&data_dir).map_err(|error| error.to_string())?;

            // The bundled backend inherits these runtime settings when launched as a sidecar.
            std::env::set_var("DB_AUTO_PILOT_DATA_DIR", &data_dir);
            std::env::set_var("DB_AUTO_PILOT_HOST", "127.0.0.1");
            std::env::set_var("DB_AUTO_PILOT_PORT", "8000");

            let (_rx, child) = app
                .shell()
                .sidecar("db-auto-pilot-backend")
                .map_err(|error| error.to_string())?
                .spawn()
                .map_err(|error| error.to_string())?;

            app.manage(BackendChild(Mutex::new(Some(child))));
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("failed to build tauri app")
        .run(|app, event| {
            if let tauri::RunEvent::Exit = event {
                if let Some(state) = app.try_state::<BackendChild>() {
                    if let Ok(mut guard) = state.0.lock() {
                        if let Some(child) = guard.take() {
                            let _ = child.kill();
                        }
                    }
                }
            }
        });
}
