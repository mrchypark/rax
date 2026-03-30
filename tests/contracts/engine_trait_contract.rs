use std::path::PathBuf;

use wax_bench_model::{
    EnginePhase, EngineStats, MountRequest, OpenRequest, OpenResult, SearchRequest, SearchResult,
    WaxEngine,
};

#[derive(Debug, Default)]
struct DummyEngine {
    mounted_path: Option<PathBuf>,
    phase: EnginePhase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DummyError;

impl WaxEngine for DummyEngine {
    type Error = DummyError;

    fn mount(&mut self, request: MountRequest) -> Result<(), Self::Error> {
        if self.phase != EnginePhase::New {
            return Err(DummyError);
        }
        self.mounted_path = Some(request.store_path);
        self.phase = EnginePhase::Mounted;
        Ok(())
    }

    fn open(&mut self, _request: OpenRequest) -> Result<OpenResult, Self::Error> {
        if self.phase != EnginePhase::Mounted {
            return Err(DummyError);
        }
        self.phase = EnginePhase::Open;
        Ok(OpenResult)
    }

    fn search(&mut self, request: SearchRequest) -> Result<SearchResult, Self::Error> {
        if self.phase != EnginePhase::Open {
            return Err(DummyError);
        }
        Ok(SearchResult {
            hits: vec![request.query_text],
        })
    }

    fn get_stats(&self) -> EngineStats {
        EngineStats {
            phase: self.phase,
            last_mounted_path: self.mounted_path.clone(),
        }
    }
}

#[test]
fn engine_trait_supports_runner_lifecycle_surface() {
    let mut engine = DummyEngine::default();
    let initial_stats = engine.get_stats();

    assert_eq!(initial_stats.phase, EnginePhase::New);
    assert_eq!(initial_stats.last_mounted_path, None);

    assert!(
        engine
            .search(SearchRequest {
                query_text: "cold open".to_owned(),
            })
            .is_err()
    );

    engine
        .mount(MountRequest {
            store_path: PathBuf::from("/tmp/store.wax"),
        })
        .unwrap();
    let mounted_stats = engine.get_stats();

    assert_eq!(mounted_stats.phase, EnginePhase::Mounted);
    assert_eq!(
        mounted_stats.last_mounted_path.as_deref(),
        Some(std::path::Path::new("/tmp/store.wax"))
    );

    assert!(engine.mount(MountRequest {
        store_path: PathBuf::from("/tmp/store.wax"),
    }).is_err());
    let mounted_stats_after_failed_mount = engine.get_stats();

    assert_eq!(mounted_stats_after_failed_mount, mounted_stats);

    let unopened = DummyEngine::default();
    let mut unopened = unopened;
    assert!(unopened.open(OpenRequest).is_err());
    let unopened_stats = unopened.get_stats();

    assert_eq!(unopened_stats.phase, EnginePhase::New);
    assert_eq!(unopened_stats.last_mounted_path, None);

    let open = engine.open(OpenRequest).unwrap();
    assert!(engine.open(OpenRequest).is_err());
    let stats_after_failed_reopen = engine.get_stats();
    let search = engine
        .search(SearchRequest {
            query_text: "cold open".to_owned(),
        })
        .unwrap();
    let stats = engine.get_stats();

    assert_eq!(open, OpenResult);
    assert_eq!(stats_after_failed_reopen.phase, EnginePhase::Open);
    assert_eq!(
        stats_after_failed_reopen.last_mounted_path.as_deref(),
        Some(std::path::Path::new("/tmp/store.wax"))
    );
    assert_eq!(search.hits, vec!["cold open".to_owned()]);
    assert_eq!(stats.phase, EnginePhase::Open);
    assert_eq!(stats.last_mounted_path.as_deref(), Some(std::path::Path::new("/tmp/store.wax")));
}
