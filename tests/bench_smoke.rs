//! The bench example runs end to end at its smallest scale, so it does not
//! rot between the runs BENCH.md records.

use std::process::Command;

#[test]
fn the_bench_runs_quick() {
    let output = Command::new(env!("CARGO"))
        .args(["run", "--quiet", "--example", "bench", "--", "--quick"])
        .output()
        .expect("cargo runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    for row in [
        "| publish,",
        "| latency,",
        "| fan-out, 10 subscribers",
        "| whole database",
    ] {
        assert!(stdout.contains(row), "{row} missing:\n{stdout}");
    }
}
