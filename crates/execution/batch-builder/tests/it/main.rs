//! Batch maker EL -> CL integration test

// prevent clippy unused dep warning
#[cfg(test)]
mod deps_for_tests {
    use tn_batch_builder as _;
    use tn_batch_validator as _;
}

// it test
mod build_batches;

fn main() {}
