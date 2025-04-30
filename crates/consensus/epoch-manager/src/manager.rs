//! The epoch manager type.
//!
//! This oversees the tasks that run for each epoch. Some consensus-related
//! tasks run for one epoch. Other resources are shared across epochs.

//
//
// epoch manager shared tasks:
// - engine
// - networks
//
// restarts:
// - primary
// - worker
//
//
//
//
//

// /// The long-running type that oversees epoch transitions.
// #[derive(Debug)]
// pub struct EpochManager<DB> {
//     /// The builder for node configuration
//     builder: TnBuilder,
//     /// The data directory
//     tn_datadir: P,
//     /// The database type
//     db: DatabaseType,
//     /// The execution engine that should continue across epochs
//     engine: Option<ExecutionNode>,
//     /// Primary network handle that should continue across epochs
//     primary_network: Option<PrimaryNetworkHandle>,
//     /// Worker network handle that should continue across epochs
//     worker_network: Option<WorkerNetworkHandle>,
//     /// Main task manager that runs across epochs
//     task_manager: TaskManager,
// }

// impl<DB> EpochManager<DB> {
//     /// Create a new instance of [Self].
//     pub fn new(builder: TnBuilder, tn_datadir: P, db: DatabaseType) -> Self {
//         Self {
//             builder,
//             tn_datadir,
//             db,
//             engine: None,
//             primary_network: None,
//             worker_network: None,
//             task_manager: TaskManager::new("Epoch Manager"),
//         }
//     }
// }
