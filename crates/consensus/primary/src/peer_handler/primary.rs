#[async_trait]
impl<DB: Database> PrimaryToPrimary for PrimaryReceiverHandler<DB> {
    async fn send_certificate(
        &self,
        request: anemo::Request<SendCertificateRequest>,
    ) -> Result<anemo::Response<SendCertificateResponse>, anemo::rpc::Status> {
        let _scope = monitored_scope("PrimaryReceiverHandler::send_certificate");
        let certificate = validate_received_certificate_version(request.into_body().certificate)
            .map_err(|err| {
                anemo::rpc::Status::new_with_message(
                    StatusCode::BadRequest,
                    format!("Invalid certifcate: {err}"),
                )
            })?;

        match self.synchronizer.try_accept_certificate(certificate).await {
            Ok(()) => Ok(anemo::Response::new(SendCertificateResponse { accepted: true })),
            Err(DagError::Suspended(_)) => {
                Ok(anemo::Response::new(SendCertificateResponse { accepted: false }))
            }
            Err(e) => Err(anemo::rpc::Status::internal(e.to_string())),
        }
    }

    async fn request_vote(
        &self,
        request: anemo::Request<RequestVoteRequest>,
    ) -> Result<anemo::Response<RequestVoteResponse>, anemo::rpc::Status> {
        self.process_request_vote(request).await.map(anemo::Response::new).map_err(|e| {
            anemo::rpc::Status::new_with_message(
                match e {
                    // Report unretriable errors as 400 Bad Request.
                    DagError::InvalidSignature
                    | DagError::InvalidEpoch { .. }
                    | DagError::InvalidHeaderDigest
                    | DagError::HeaderHasBadWorkerIds(_)
                    | DagError::HeaderHasInvalidParentRoundNumbers(_)
                    | DagError::HeaderHasDuplicateParentAuthorities(_)
                    | DagError::AlreadyVoted(_, _, _)
                    | DagError::AlreadyVotedNewerHeader(_, _, _)
                    | DagError::HeaderRequiresQuorum(_)
                    | DagError::TooOld(_, _, _) => anemo::types::response::StatusCode::BadRequest,
                    // All other errors are retriable.
                    _ => anemo::types::response::StatusCode::Unknown,
                },
                format!("{e:?}"),
            )
        })
    }

    #[instrument(level = "debug", skip_all, peer = ?request.peer_id())]
    async fn fetch_certificates(
        &self,
        request: anemo::Request<FetchCertificatesRequest>,
    ) -> Result<anemo::Response<FetchCertificatesResponse>, anemo::rpc::Status> {
        let time_start = Instant::now();
        let peer =
            request.peer_id().map_or_else(|| "None".to_string(), |peer_id| format!("{}", peer_id));
        let request = request.into_body();
        let mut response = FetchCertificatesResponse { certificates: Vec::new() };
        if request.max_items == 0 {
            return Ok(anemo::Response::new(response));
        }

        // Use a min-queue for (round, authority) to keep track of the next certificate to fetch.
        //
        // Compared to fetching certificates iteratatively round by round, using a heap is simpler,
        // and avoids the pathological case of iterating through many missing rounds of a downed
        // authority.
        let (lower_bound, skip_rounds) = request.get_bounds();
        debug!(
            "Fetching certificates after round {lower_bound} for peer {:?}, elapsed = {}ms",
            peer,
            time_start.elapsed().as_millis(),
        );

        let mut fetch_queue = BinaryHeap::new();
        const MAX_SKIP_ROUNDS: usize = 1000;
        for (origin, rounds) in &skip_rounds {
            if rounds.len() > MAX_SKIP_ROUNDS {
                warn!(
                    "Peer has sent {} rounds to skip on origin {}, indicating peer's problem with \
                    committing or keeping track of GC rounds. elapsed = {}ms",
                    rounds.len(),
                    origin,
                    time_start.elapsed().as_millis(),
                );
            }
            let next_round = self.find_next_round(*origin, lower_bound, rounds)?;
            if let Some(r) = next_round {
                fetch_queue.push(Reverse((r, origin)));
            }
        }
        debug!(
            "Initialized origins and rounds to fetch, elapsed = {}ms",
            time_start.elapsed().as_millis(),
        );

        // Iteratively pop the next smallest (Round, Authority) pair, and push to min-heap the next
        // higher round of the same authority that should not be skipped.
        // The process ends when there are no more pairs in the min-heap.
        while let Some(Reverse((round, origin))) = fetch_queue.pop() {
            // Allow the request handler to be stopped after timeout.
            tokio::task::yield_now().await;
            match self
                .certificate_store
                .read_by_index(*origin, round)
                .map_err(|e| anemo::rpc::Status::unknown(format!("unknown error: {e}")))?
            {
                Some(cert) => {
                    response.certificates.push(cert);
                    let next_round =
                        self.find_next_round(*origin, round, skip_rounds.get(origin).unwrap())?;
                    if let Some(r) = next_round {
                        fetch_queue.push(Reverse((r, origin)));
                    }
                }
                None => continue,
            };
            if response.certificates.len() == request.max_items {
                debug!(
                    "Collected enough certificates (num={}, elapsed={}ms), returning.",
                    response.certificates.len(),
                    time_start.elapsed().as_millis(),
                );
                break;
            }
            if time_start.elapsed() >= FETCH_CERTIFICATES_MAX_HANDLER_TIME {
                debug!(
                    "Spent enough time reading certificates (num={}, elapsed={}ms), returning.",
                    response.certificates.len(),
                    time_start.elapsed().as_millis(),
                );
                break;
            }
            assert!(response.certificates.len() < request.max_items);
        }

        // The requestor should be able to process certificates returned in this order without
        // any missing parents.
        Ok(anemo::Response::new(response))
    }
}
