use std::{
    env,
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    build_anemo_services(&out_dir);

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=DUMP_GENERATED_GRPC");

    nightly();
    beta();
    stable();

    Ok(())
}

fn build_anemo_services(out_dir: &Path) {
    let mut automock_attribute = anemo_build::Attributes::default();
    automock_attribute.push_trait(".", r#"#[mockall::automock]"#);

    let codec_path = "crate::codec::anemo::BcsSnappyCodec";

    let primary_to_primary = anemo_build::manual::Service::builder()
        .name("PrimaryToPrimary")
        .package("tn")
        .attributes(automock_attribute.clone())
        .method(
            anemo_build::manual::Method::builder()
                .name("send_certificate")
                .route_name("SendCertificate")
                .request_type("crate::SendCertificateRequest")
                .response_type("crate::SendCertificateResponse")
                .codec_path(codec_path)
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("request_vote")
                .route_name("RequestVote")
                .request_type("crate::RequestVoteRequest")
                .response_type("crate::RequestVoteResponse")
                .codec_path(codec_path)
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("fetch_certificates")
                .route_name("FetchCertificates")
                .request_type("crate::FetchCertificatesRequest")
                .response_type("crate::FetchCertificatesResponse")
                .codec_path(codec_path)
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("request_consensus")
                .route_name("RequestConsensus")
                .request_type("crate::ConsensusOutputRequest")
                .response_type("crate::ConsensusOutputResponse")
                .codec_path(codec_path)
                .build(),
        )
        .build();

    let primary_to_worker = anemo_build::manual::Service::builder()
        .name("PrimaryToWorker")
        .package("tn")
        .attributes(automock_attribute.clone())
        .method(
            anemo_build::manual::Method::builder()
                .name("synchronize")
                .route_name("Synchronize")
                .request_type("crate::WorkerSynchronizeMessage")
                .response_type("()")
                .codec_path(codec_path)
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("fetch_blocks")
                .route_name("FetchBlocks")
                .request_type("crate::FetchBlocksRequest")
                .response_type("crate::FetchBlocksResponse")
                .codec_path(codec_path)
                .build(),
        )
        .build();

    let primary_to_engine = anemo_build::manual::Service::builder()
        .name("PrimaryToEngine")
        .package("tn")
        .attributes(automock_attribute.clone())
        .method(
            anemo_build::manual::Method::builder()
                .name("verify_execution")
                .route_name("VerifyExecution")
                .request_type("crate::VerifyExecutionRequest")
                .response_type("crate::VerifyExecutionResponse")
                .codec_path(codec_path)
                .build(),
        )
        .build();

    let engine_to_primary = anemo_build::manual::Service::builder()
        .name("EngineToPrimary")
        .package("tn")
        .attributes(automock_attribute.clone())
        .method(
            anemo_build::manual::Method::builder()
                .name("canonical_update")
                .route_name("CanonicalUpdate")
                .request_type("crate::CanonicalUpdateMessage")
                .response_type("()")
                .codec_path(codec_path)
                .build(),
        )
        .build();

    let worker_to_primary = anemo_build::manual::Service::builder()
        .name("WorkerToPrimary")
        .package("tn")
        .attributes(automock_attribute.clone())
        .method(
            anemo_build::manual::Method::builder()
                .name("report_own_block")
                .route_name("ReportOwnBlock")
                .request_type("crate::WorkerOwnBlockMessage")
                .response_type("()")
                .codec_path(codec_path)
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("report_others_block")
                .route_name("ReportOthersBlock")
                .request_type("crate::WorkerOthersBlockMessage")
                .response_type("()")
                .codec_path(codec_path)
                .build(),
        )
        .build();

    let worker_to_worker = anemo_build::manual::Service::builder()
        .name("WorkerToWorker")
        .package("tn")
        .attributes(automock_attribute)
        .method(
            anemo_build::manual::Method::builder()
                .name("report_block")
                .route_name("ReportBlock")
                .request_type("crate::WorkerBatchMessage")
                .response_type("()")
                .codec_path(codec_path)
                .build(),
        )
        .method(
            anemo_build::manual::Method::builder()
                .name("request_batches")
                .route_name("RequestBlocks")
                .request_type("crate::RequestBlocksRequest")
                .response_type("crate::RequestBlocksResponse")
                .codec_path(codec_path)
                .build(),
        )
        .build();

    anemo_build::manual::Builder::new().out_dir(out_dir).compile(&[
        primary_to_primary,
        primary_to_worker,
        worker_to_primary,
        worker_to_worker,
        engine_to_primary,
        primary_to_engine,
    ]);
}

#[rustversion::nightly]
fn nightly() {
    println!("cargo:rustc-cfg=nightly");
}

#[rustversion::not(nightly)]
fn nightly() {}

#[rustversion::beta]
fn beta() {
    println!("cargo:rustc-cfg=beta");
}

#[rustversion::not(beta)]
fn beta() {}

#[rustversion::stable]
fn stable() {
    println!("cargo:rustc-cfg=stable");
}

#[rustversion::not(stable)]
fn stable() {}
