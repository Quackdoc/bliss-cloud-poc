use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::web::Json;
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::time::{Duration, sleep, interval};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Demo {
    demo: String,
}

#[derive(Debug)]
struct Task {
    user: User,
    last_keepalive: Instant,
    process: Child,
}

#[derive(Debug, Clone, Serialize)]
struct TaskSummary {
    user: User,
}

impl From<&Task> for TaskSummary {
    fn from(task: &Task) -> Self {
        TaskSummary {
            user: task.user.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct AppState {
    queue: Arc<Mutex<VecDeque<Task>>>,
}

#[rustfmt::skip]
fn spawn_process_for_user(demo: web::Json<Demo>, port: &u16) -> std::io::Result<Child> {
    let vnc_option = format!(":0,websocket={},to=100", port);

    // change restrict=no to allow internet
    let adb_option= format!("user,hostfwd=tcp::{}-:5555,restrict=yes", port + 10);
    println!("arg is {}", demo.demo.as_str());

    // make sure to configure arguments as needed.
    let args = match demo.demo.as_str() {
        "Software" => vec![
            "-accel", "kvm",
            "-m", "8G",
            "-smp", "4",
            "-cpu", "host",
            "-bios", "/usr/share/OVMF/x64/OVMF.4m.fd",
            "-device", "qxl",
            "-display", "none",
            "-vnc", &vnc_option,
            "-drive", "if=virtio,file=/nvme/VM/disks/android-test.qcow2",
            "-snapshot",
        ],
        "Scrcpy" => vec![
            "-accel", "kvm",
            "-m", "8G",
            "-smp", "4",
            "-cpu", "host",
            "-bios", "/usr/share/OVMF/x64/OVMF.4m.fd",
            "-device", "virtio-vga-gl,blob=true,hostmem=8G,venus=true",
            "-object", "memory-backend-memfd,id=mem1,size=8G",
            "-machine", "memory-backend=mem1",
            "-display", "egl-headless,gl=on",
            "-vnc", &vnc_option,
            "-net", "nic,model=virtio-net-pci",
            "-net", &adb_option,
            "-drive", "if=virtio,file=/nvme/VM/disks/android-test.qcow2",
            "-snapshot",
        ],
        _ => vec![
            "-accel", "kvm",
            "-m", "4G",
            "-smp", "2",
            "-cpu", "host",
            "-bios", "/usr/share/OVMF/x64/OVMF.4m.fd",
            "-device", "virtio-vga-gl,blob=true,hostmem=4G,venus=true",
            "-object", "memory-backend-memfd,id=mem1,size=4G",
            "-machine", "memory-backend=mem1",
            "-display", "egl-headless,gl=on",
            "-vnc", &vnc_option,
            "-drive", "if=virtio,file=/nvme/VM/disks/android-test.qcow2",
            "-snapshot",
        ]

    };

    Command::new("qemu-system-x86_64")
        .args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()

}

async fn enqueue_user(state: web::Data<AppState>, demo: web::Json<Demo>) -> impl Responder {
    let mut rng = rand::rng();
    let mut nums: Vec<u64> = (1..100).collect();
    nums.shuffle(&mut rng);
    let id = nums.choose(&mut rng).unwrap();

    let name = "demo";
    let user = User {
        id: *id,
        name: name.to_string(),
    };

    let mut queue = state.queue.lock().unwrap();
    // how many qemu instances can run
    let max_tasks = 5;

    loop {
        println!("Checking queue length: {}", queue.len());
        if queue.len() < max_tasks {
            break;
        }
        println!("Queue exceeded threshold of {} items.", max_tasks);
        drop(queue);
        sleep(Duration::from_secs(5)).await;
        queue = state.queue.lock().unwrap();
    }
    let rand_port = openport::pick_random_unused_port().unwrap();
    let process = match spawn_process_for_user(demo, &rand_port) {
        Ok(child) => child,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to start process: {}", e));
        }
    };

    let task = Task {
        user: user,
        last_keepalive: Instant::now(),
        process,
    };
    let reply = Json(serde_json::json!({
        "id": id,
        "port": rand_port
    }));

    queue.push_back(task);
    HttpResponse::Ok().json(reply)
}

async fn list_tasks(state: web::Data<AppState>) -> impl Responder {
    let queue = state.queue.lock().unwrap();
    let tasks: Vec<TaskSummary> = queue.iter().map(TaskSummary::from).collect();
    HttpResponse::Ok().json(tasks)
}

#[derive(Debug, Deserialize)]
struct KeepAliveRequest {
    id: u64,
}

async fn keepalive(state: web::Data<AppState>, req: web::Json<KeepAliveRequest>) -> impl Responder {
    let mut queue = state.queue.lock().unwrap();
    let mut found = false;

    for task in queue.iter_mut() {
        if task.user.id == req.id {
            task.last_keepalive = Instant::now();
            found = true;
            break;
        }
    }

    if found {
        HttpResponse::Ok().body("Keepalive received")
    } else {
        HttpResponse::NotFound().body("User task not found")
    }
}

async fn dequeue_task(state: web::Data<AppState>) -> impl Responder {
    let mut queue = state.queue.lock().unwrap();
    if let Some(mut task) = queue.pop_front() {
        let _ = task.process.kill();
        HttpResponse::Ok().json(task.user)
    } else {
        HttpResponse::Ok().body("Queue is empty")
    }
}

async fn cleaner_task(state: web::Data<AppState>, timeout: Duration) {
    let mut ticker = interval(Duration::from_secs(5));

    loop {
        ticker.tick().await;
        let now = Instant::now();
        let mut queue = state.queue.lock().unwrap();
        let original_len = queue.len();
        queue.retain_mut(|task| {
            if now.duration_since(task.last_keepalive) >= timeout {
                println!(
                    "Task for user id {} timed out; terminating process.",
                    task.user.id
                );
                let id = task.process.id();
                println!("{}", id);
                let _ = task.process.kill().expect("command couldn't be killed");
                false
            } else {
                true
            }
        });
        let removed = original_len - queue.len();
        if removed > 0 {
            println!("Cleaner removed {} timed-out task(s).", removed);
        }
    }
}

async fn redirect_to_index() -> impl Responder {
    HttpResponse::Found()
        .append_header(("Location", "/index.html"))
        .finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let keepalive_timeout = Duration::from_secs(30);
    let state = web::Data::new(AppState::default());

    {
        let state_clone = state.clone();
        actix_web::rt::spawn(async move {
            cleaner_task(state_clone, keepalive_timeout).await;
        });
    }
    println!("Starting server on http://localhost:8080");
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/enqueue", web::post().to(enqueue_user))
            .route("/queue", web::get().to(list_tasks))
            .route("/dequeue", web::post().to(dequeue_task))
            .route("/keepalive", web::post().to(keepalive))
            .route("/", web::get().to(redirect_to_index))
            .service(
                actix_files::Files::new("/", "./static/")
                    .prefer_utf8(true)
                    .show_files_listing()
                    //.index_file("index.html")
                    .default_handler(|req: ServiceRequest| {
                        let (http_req, _payload) = req.into_parts();
                        async {
                            let response = actix_files::NamedFile::open("./static/index.html")?
                                .into_response(&http_req);
                            Ok(ServiceResponse::new(http_req, response))
                        }
                    }),
            )
    })
    .bind(("localhost", 8080))?
    .run()
    .await
}
